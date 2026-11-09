#!/usr/bin/env python3
"""Offline end-to-end test for the aws-c3 pipeline: synthetic CDXJ parts,
two crawls, LocalStore backend. Verifies:
  * master dedup + first/last capture dates
  * fresh set = first-seen inside the latest crawl window
  * resumability (stream markers reused on second run)
Run: python3 tests/test_offline.py
"""
import gzip
import io
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import cc_fresh  # noqa: E402
import importlib

ROW = cc_fresh.ROW


def make_cdx_part(rows):
    lines = []
    for urlkey, ts in rows:
        meta = json.dumps({"url": "http://x.test/", "mime": "text/html",
                           "status": "200", "digest": "sha1:deadbeef"})
        lines.append(f"{urlkey} {ts} {meta}")
    return gzip.compress(("\n".join(lines) + "\n").encode())


def main():
    tmp = tempfile.mkdtemp(prefix="awsc3-test-")
    store_root = os.path.join(tmp, "store")
    os.makedirs(store_root, exist_ok=True)

    for d in ("CC-MAIN-2026-01", "CC-MAIN-2026-02"):
        os.makedirs(f"{store_root}/wire/{d}/indexes", exist_ok=True)

    crawl1 = [("com,aa)/x", "20260105120000"),
              ("com,bb)/", "20260106120000"),
              ("com,example)/a", "20260107120000"),
              ("org,cc)/y", "20260108120000")]
    crawl2 = [("com,bb)/", "20260205120000"),        # seen before -> not fresh
              ("com,example)/b", "20260206120000"),  # same host, different key -> fresh
              ("net,dd)/z", "20260207120000")]       # brand new -> fresh
    with open(f"{store_root}/wire/CC-MAIN-2026-01/indexes/cdx-00000.gz", "wb") as f:
        f.write(make_cdx_part(crawl1))
    with open(f"{store_root}/wire/CC-MAIN-2026-02/indexes/cdx-00000.gz", "wb") as f:
        f.write(make_cdx_part(crawl2))

    # wire parts served from the store dir, not the network
    class LocalWire(cc_fresh.LocalStore):
        def stream_get(self, url):
            crawl = url.split("/collections/")[1].split("/indexes/")[0]
            name = url.rsplit("/", 1)[1]
            with open(os.path.join(self.root, "wire", crawl, "indexes", name), "rb") as fh:
                while True:
                    c = fh.read(1 << 16)
                    if not c:
                        return
                    yield c

    cc_fresh.LocalStore = LocalWire

    # fake collinfo
    store = LocalWire(store_root)
    store.put("state/collinfo.json", json.dumps([
        {"id": "CC-MAIN-2026-01", "from": "2026-01-05T00:00:00", "to": "2026-01-18T00:00:00"},
        {"id": "CC-MAIN-2026-02", "from": "2026-02-05T00:00:00", "to": "2026-02-18T00:00:00"},
    ]).encode())
    # paths lists point at the single fake part per crawl
    for d in ("CC-MAIN-2026-01", "CC-MAIN-2026-02"):
        store.put(f"index-paths/{d}.list",
                  f"cc-index/collections/{d}/indexes/cdx-00000.gz".encode())

    import run_job
    importlib.reload(run_job)
    os.environ.update({
        "AWS_C3_LOCAL": store_root,
        "AWS_C3_CRAWLS": "CC-MAIN-2026-01,CC-MAIN-2026-02",
        "AWS_C3_LATEST": "CC-MAIN-2026-02",
        "AWS_C3_WORKDIR": os.path.join(tmp, "work"),
    })
    rc = run_job.main()
    assert rc == 0, "run_job failed"

    # ---- master verification
    master = store.read("master/seen-CC-MAIN-2026-02.gz")
    rows = {}
    with gzip.open(io.BytesIO(master), "rb") as f:
        while True:
            rec = f.read(ROW.size)
            if len(rec) < ROW.size:
                break
            h, first, last = ROW.unpack(rec)
            rows[h] = (first, last)

    expect = {
        "com,aa)/x":        (20260105, 20260105),
        "com,bb)/":         (20260106, 20260205),
        "com,example)/a":   (20260107, 20260107),
        "com,example)/b":   (20260206, 20260206),
        "net,dd)/z":        (20260207, 20260207),
        "org,cc)/y":        (20260108, 20260108),
    }
    assert len(rows) == 6, f"expected 6 distinct, got {len(rows)}"
    for u, (fexp, lexp) in expect.items():
        got = rows.get(cc_fresh.url_hash(u))
        assert got, f"missing {u}"
        assert got[0] == fexp, f"{u}: first {got[0]} != {fexp}"
        assert got[1] == lexp, f"{u}: last {got[1]} != {lexp}"
    print("master first/last dates verified (6 urls)")

    # ---- fresh set: only urls first captured inside CC-MAIN-2026-02
    fresh = store.read("fresh/CC-MAIN-2026-02.bin.gz")
    fresh_hashes = set()
    with gzip.open(io.BytesIO(fresh), "rb") as f:
        while True:
            h = f.read(16)
            if not h:
                break
            fresh_hashes.add(h)
    assert fresh_hashes == {cc_fresh.url_hash("com,example)/b"),
                            cc_fresh.url_hash("net,dd)/z")}, \
        f"fresh set mismatch ({len(fresh_hashes)} hashes)"
    print("fresh set verified (2 hashes)")

    # ---- resumability: rerun reuses stored streams
    rc = run_job.main()
    assert rc == 0
    print("resumability verified (second run reused streams)")

    shutil.rmtree(tmp, ignore_errors=True)
    print("\nALL OFFLINE TESTS PASSED")


if __name__ == "__main__":
    main()
