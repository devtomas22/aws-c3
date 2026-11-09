#!/usr/bin/env python3
"""aws-c3 core: Common Crawl fresh-URL pipeline.

Builds a compact, hash-sorted "master" of every URL ever seen in Common Crawl:

    row = blake2b16(surt_urlkey) || first_capture_ymd(u32) || last_capture_ymd(u32)

and, for the latest crawl processed, the "fresh" set: hashes whose first
capture happened inside that crawl's window (i.e. previously unseen).

Design
------
* CDXJ part files (cc-index/collections/<ID>/indexes/cdx-NNNNN.gz) are sorted
  by surt urlkey, so a crawl's parts in listed order form one sorted urlkey
  stream. The master is keyed by CONTENT hash of the urlkey; we therefore
  range-partition rows into 256 buckets during a single streaming pass and
  sort each bucket in RAM (~200 MB). No global external sort, no hash set.
* Master merge: heapq.merge(prev_master, bucket_0..255) keyed by hash;
  duplicate hashes collapse to (first=min, last=max). Rows first seen inside
  the crawl window go to the fresh set. Memory stays O(buckets).
* Resumable-ish: a crawl re-runs from its start if interrupted; the marker is
  written only after all buckets are valid.
* Storage is abstract (LocalStore for tests, S3Store in production), so the
  core is unit-testable offline.
"""

from __future__ import annotations

import gzip
import hashlib
import heapq
import json
import os
import time
import urllib.request
import zlib
from collections.abc import Iterator
from dataclasses import dataclass

BASE = "https://data.commoncrawl.org"
HASH_LEN = 16
ROW = __import__("struct").Struct("<16sII")  # hash, first ymd, last ymd
BUCKETS = 256
BUCKET_FLUSH = 220 * 1024 * 1024  # spill threshold per bucket


def url_hash(urlkey: str) -> bytes:
    return hashlib.blake2b(urlkey.encode("utf8"), digest_size=HASH_LEN).digest()


def ymd(ts: str) -> int:
    """'20241017172547' (or any CC timestamp prefix) -> 20241017."""
    return int(ts[:8])


# ----------------------------------------------------------------- storage

class LocalStore:
    """Directory-backed store (tests)."""

    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def read(self, key: str) -> bytes:
        with open(os.path.join(self.root, key), "rb") as f:
            return f.read()

    def exists(self, key: str) -> bool:
        return os.path.exists(os.path.join(self.root, key))

    def put(self, key: str, data: bytes) -> None:
        path = os.path.join(self.root, key)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)

    def stream_get(self, url: str) -> Iterator[bytes]:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=60) as r:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    return
                yield chunk


class S3Store:
    """Bucket-backed store (production)."""

    def __init__(self, bucket: str):
        import boto3

        self.s3 = boto3.client("s3")
        self.bucket = bucket

    def read(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def put(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def stream_get(self, url: str) -> Iterator[bytes]:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=60) as r:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    return
                yield chunk


# -------------------------------------------------------------- CC inputs

def load_collinfo(store) -> dict[str, dict]:
    key = "state/collinfo.json"
    if store.exists(key):
        return json.loads(store.read(key))
    req = urllib.request.Request("https://index.commoncrawl.org/collinfo.json")
    data = urllib.request.urlopen(req, timeout=60).read()
    store.put(key, data)
    return json.loads(data)


def crawl_windows(store) -> dict[str, tuple[int, int]]:
    """crawl id -> (from_ymd, to_ymd) using collinfo capture windows."""
    out = {}
    for c in load_collinfo(store):
        frm = ymd(c["from"].replace("-", "").split("T")[0])
        to = ymd(c["to"].replace("-", "").split("T")[0])
        out[c["id"]] = (frm, to)
    return out


def fetch_cdx_paths(store, crawl_id: str) -> list[str]:
    key = f"index-paths/{crawl_id}.list"
    if store.exists(key):
        return store.read(key).decode().splitlines()
    gz = gzip.decompress(b"".join(store.stream_get(f"{BASE}/crawl-data/{crawl_id}/cc-index.paths.gz")))
    paths = [p.strip() for p in gz.decode().splitlines() if p.strip() and "/indexes/cdx-" in p]
    store.put(key, "\n".join(paths).encode())
    return paths


def _inflater():
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)

    class _D:
        def decompress(self, c):
            return d.decompress(c)

        def flush(self):
            return d.flush()

    return _D()


def part_lines(store, url: str) -> Iterator[bytes]:
    """Yield decompressed lines of one gzip CDX part, streaming from the wire."""
    dec = _inflater()
    buf = b""
    for chunk in store.stream_get(url):
        buf += dec.decompress(chunk)
        while True:
            nl = buf.find(b"\n")
            if nl < 0:
                break
            yield buf[:nl]
            buf = buf[nl + 1 :]
    tail = dec.flush()
    for line in tail.split(b"\n"):
        if line:
            yield line


def crawl_rows(store, crawl_id: str, log=print) -> Iterator[bytes]:
    """Yield master rows (16s II packed with date=date) for one crawl."""
    windows = crawl_windows(store)
    frm, to = windows.get(crawl_id, (20080101, 20991231))
    paths = fetch_cdx_paths(store, crawl_id)
    t0 = time.time()
    n = 0
    for i, part in enumerate(paths):
        for line in part_lines(store, f"{BASE}/{part}"):
            sp = line.find(b" ")
            if sp <= 0:
                continue
            key = line[:sp].decode("utf8", "replace")
            rest = line[sp + 1 :]
            ts = rest.split(b" ", 1)[0][:8]  # capture timestamp yyyymmdd
            try:
                d = int(ts)
            except ValueError:
                d = frm
            if not (frm <= d <= to + 10):  # tolerate straddling captures
                d = max(frm, min(d, to))
            yield ROW.pack(hashlib.blake2b(key.encode(), digest_size=HASH_LEN).digest(), d, d)
            n += 1
            if n % 25_000_000 == 0:
                log(f"[{crawl_id}] part {i + 1}/{len(paths)}: {n / 1e6:.0f}M rows "
                    f"({n * ROW.size / 2**30:.1f} GiB raw, {time.time() - t0:.0f}s)")
    log(f"[{crawl_id}] streamed {len(paths)} parts, {n} rows, {time.time() - t0:.0f}s")


# ------------------------------------------------- per-crawl sorted stream

@dataclass
class Buckets:
    paths: list[str]  # hash-sorted gz bucket files


def build_crawl_buckets(store, crawl_id: str, workdir: str, log=print) -> Buckets:
    os.makedirs(workdir, exist_ok=True)
    bucket_paths = [os.path.join(workdir, f"{crawl_id}.b{b:03d}.gz") for b in range(BUCKETS)]
    if all(os.path.exists(p) for p in bucket_paths):
        log(f"[{crawl_id}] buckets already built, resuming merge")
        return Buckets(bucket_paths)

    tmp = [open(p + ".part", "wb") for p in bucket_paths]
    buffered = [0] * BUCKETS
    t0 = time.time()
    n = 0
    for row in crawl_rows(store, crawl_id, log):
        b = row[0]
        tmp[b].write(row)
        buffered[b] += ROW.size
        if buffered[b] >= BUCKET_FLUSH:
            tmp[b].flush()
            buffered[b] = 0
        n += 1
        if n % 10_000_000 == 0:
            log(f"[{crawl_id}] {n / 1e6:.0f}M rows in buckets ({time.time() - t0:.0f}s)")
    for f in tmp:
        f.close()

    total = len(bucket_paths)
    for i, bp in enumerate(bucket_paths):
        raw = open(bp + ".part", "rb").read()
        if raw:
            rows = sorted(ROW.iter_unpack(raw))
            blob = b"".join(ROW.pack(*r) for r in rows)
            with gzip.GzipFile(bp, "wb", compresslevel=6, mtime=0) as f:
                f.write(blob)
        else:
            open(bp, "wb").close()
        os.remove(bp + ".part")
        if i % 32 == 0:
            log(f"[{crawl_id}] sorted bucket {i}/{total} ({time.time() - t0:.0f}s)")
    log(f"[{crawl_id}] buckets complete ({time.time() - t0:.0f}s)")
    return Buckets(bucket_paths)


def bucket_rows(path: str) -> Iterator[tuple[bytes, int, int]]:
    with gzip.open(path, "rb") as f:
        while True:
            rec = f.read(ROW.size)
            if len(rec) < ROW.size:
                return
            yield ROW.unpack(rec)


def bucket_rows_from(store, key_prefix: str) -> list[Iterator[tuple[bytes, int, int]]]:
    """Bucket streams already uploaded to storage (crash-resume path)."""
    out = []
    for b in range(BUCKETS):
        key = f"{key_prefix}/{b:03d}.gz"
        blob = store.read(key)
        out.append(_rows_from_blob(blob))
    return out


def _rows_from_blob(blob: bytes) -> Iterator[tuple[bytes, int, int]]:
    import io as _io

    with gzip.GzipFile(fileobj=_io.BytesIO(blob)) as f:
        while True:
            rec = f.read(ROW.size)
            if len(rec) < ROW.size:
                return
            yield ROW.unpack(rec)


# ------------------------------------------------------------ master merge

def merge_master_streams(
    prev_master_blob: bytes | None,
    bucket_streams: list[Iterator[tuple[bytes, int, int]]],
    out_paths: tuple[str, str],
    window_ymd: int,
    log=print,
) -> tuple[int, int]:
    """True k-way merge: prev master (sorted) + each bucket (sorted)."""
    streams = []
    if prev_master_blob:
        streams.append(_rows_from_blob(prev_master_blob))
    streams.extend(bucket_streams)
    merged = heapq.merge(*streams)  # each stream sorted by hash -> global order
    fresh_f = gzip.GzipFile(out_paths[1] + ".tmp", "wb", mtime=0)
    out_f = gzip.GzipFile(out_paths[0] + ".tmp", "wb", compresslevel=6, mtime=0)
    total = fresh = 0
    cur = first = last = None
    t0 = time.time()
    for h, f, l in merged:
        if first is None or h != cur:
            if cur is not None:
                out_f.write(ROW.pack(cur, first, last))
                total += 1
                if total % 10_000_000 == 0:
                    log(f"[merge] {total / 1e6:.0f}M distinct ({time.time() - t0:.0f}s)")
            cur, first, last = h, f, l
        else:
            first = min(first, f)
            last = max(last, l)
        if first >= window_ymd:
            fresh_f.write(h)
            fresh += 1
    if cur is not None:
        out_f.write(ROW.pack(cur, first, last))
        total += 1
    out_f.close()
    fresh_f.close()
    os.replace(out_paths[0] + ".tmp", out_paths[0])
    os.replace(out_paths[1] + ".tmp", out_paths[1])
    log(f"[merge] done: {total / 1e6:.1f}M distinct, {fresh / 1e6:.2f}M fresh, {time.time() - t0:.0f}s")
    return total, fresh


def master_rows(path: str) -> Iterator[tuple[bytes, int, int]]:
    with gzip.open(path, "rb") as f:
        while True:
            rec = f.read(ROW.size)
            if len(rec) < ROW.size:
                return
            yield ROW.unpack(rec)
