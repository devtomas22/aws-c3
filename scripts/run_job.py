#!/usr/bin/env python3
"""aws-c3 job driver: process the crawls listed in AWS_C3_CRAWLS oldest-first
against the master in AWS_C3_BUCKET; the fresh set is emitted for every crawl
(most useful for AWS_C3_LATEST). Streams are stored per crawl for crash-resume.

Environment:
  AWS_C3_BUCKET     target bucket (stack default: aws-c3-<account-id>);
                    unset -> local dir store (tests)
  AWS_C3_LOCAL      dir for the local store (default ./local-store)
  AWS_C3_CRAWLS     comma-separated crawl ids, oldest first
  AWS_C3_LATEST     crawl id whose unseen URLs form the fresh set
  AWS_C3_WORKDIR    scratch dir (default /work/scratch)
"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cc_fresh import (  # noqa: E402
    S3Store, LocalStore, crawl_windows, build_crawl_buckets,
    bucket_rows_from, merge_master_streams,
)


def make_store():
    bucket = os.environ.get("AWS_C3_BUCKET")
    if bucket:
        return S3Store(bucket)
    return LocalStore(os.environ.get("AWS_C3_LOCAL", "./local-store"))


def main() -> int:
    store = make_store()
    crawls = [c.strip() for c in os.environ.get("AWS_C3_CRAWLS", "").split(",") if c.strip()]
    latest = os.environ.get("AWS_C3_LATEST", crawls[-1] if crawls else "")
    if not crawls:
        print("AWS_C3_CRAWLS is empty - nothing to do", flush=True)
        return 1
    workdir = os.environ.get("AWS_C3_WORKDIR", "/work/scratch")
    windows = crawl_windows(store)
    print(f"crawls: {crawls}  latest={latest}  ({len(windows)} crawl windows known)",
          flush=True)

    for crawl in crawls:
        frm, _to = windows.get(crawl, (20080101, 20991231))
        done = f"streams/{crawl}/_done"

        if store.exists(done):
            print(f"[{crawl}] streams already in storage; reusing", flush=True)
        else:
            bdir = os.path.join(workdir, crawl)
            shutil.rmtree(bdir, ignore_errors=True)
            buckets = build_crawl_buckets(store, crawl, bdir)
            for i, p in enumerate(buckets.paths):
                with open(p, "rb") as f:
                    store.put(f"streams/{crawl}/{i:03d}.gz", f.read())
            store.put(done, b"ok")
            shutil.rmtree(bdir, ignore_errors=True)

        # previous master = the last crawl processed before this one (if any)
        idx = crawls.index(crawl)
        prev = crawls[idx - 1] if idx > 0 else None
        prev_blob = store.read(f"master/seen-{prev}.gz") if prev and store.exists(
            f"master/seen-{prev}.gz") else None
        if prev_blob:
            print(f"[{crawl}] base master: master/seen-{prev}.gz "
                  f"({len(prev_blob) / 2**20:.1f} MiB gz)", flush=True)

        streams = bucket_rows_from(store, f"streams/{crawl}")
        total, fresh = merge_master_streams(
            prev_blob, streams,
            (f"/tmp/{crawl}_master.gz", f"/tmp/{crawl}_fresh.bin.gz"),
            window_ymd=frm,
        )
        store.put(f"master/seen-{crawl}.gz", open(f"/tmp/{crawl}_master.gz", "rb").read())
        store.put(f"fresh/{crawl}.bin.gz", open(f"/tmp/{crawl}_fresh.bin.gz", "rb").read())
        os.remove(f"/tmp/{crawl}_master.gz")
        os.remove(f"/tmp/{crawl}_fresh.bin.gz")
        print(f"[{crawl}] master={total / 1e6:.1f}M distinct  fresh={fresh / 1e6:.2f}M "
              f"-> master/seen-{crawl}.gz, fresh/{crawl}.bin.gz", flush=True)

    print("job complete", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
