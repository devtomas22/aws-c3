# aws-c3 — fresh-URL index for Common Crawl

Build a compact **seen-URL master** over the entire Common Crawl history and
extract the **fresh set** (previously unseen URLs) of any crawl.

A "fresh URL" = a URL never captured in any earlier crawl. The pipeline keeps
**no page content** — only blake2b hashes of CDX urlkeys plus first/last capture
dates — so the entire history collapses to a ~100-250 GB artifact instead of
the ~30 TiB of raw CDX it is computed from.

```
Crawl CDX parts (sorted by surt urlkey)
  └─ stream -> 256 hash buckets -> sort each bucket in RAM (~200 MB)
        └─ k-way merge against previous master (heap, O(1) RAM per stream)
              ├─ master/seen-<crawl>.gz      every URL ever seen, first+last date
              └─ fresh/<crawl>.bin.gz        hashes first seen in this crawl
```

## What you get

| Object | Format | Typical size |
|---|---|---|
| `master/seen-<ID>.gz` | rows of `16sII`: blake2b16(surt urlkey), first yyyymmdd, last yyyymmdd | ~100-250 GB total (10-20B distinct URLs ever) |
| `fresh/<ID>.bin.gz` | concatenated 16-byte hashes | ~15-25 GB per crawl (~30-50% of 2.5-3B) |
| `streams/<ID>/NNN.gz` | sorted hash buckets (crash-resume + cheap re-merge) | ~35-45 GB per crawl |
| `index-paths/<ID>.list`, `state/collinfo.json` | crawl manifests | MBs |

Verified offline (tests/test_offline.py): dedup correctness, first/last dates,
fresh-set membership, crash-resume via stored streams.

## Deploy (fresh AWS account, plug-and-play)

1. Console: CloudFormation → Create stack → upload `cfn/aws-c3.yaml`
   (or CLI: `aws cloudformation create-stack --stack-name aws-c3 \
      --template-body file://cfn/aws-c3.yaml --parameters ... --capabilities CAPABILITY_IAM`)
2. Parameters:
   - `CrawlIds` — crawls to fold, oldest first. Single fresh pass on today's
     index → latest only. Full history → all 129 ids (see `scripts/run_job.py`
     docstring; obtainable from index.commoncrawl.org/collinfo.json).
   - `LatestCrawl` — crawl whose unseen URLs become `fresh/`.
   - `InstanceType` — c6i.4xlarge (one crawl ≈ 2.5-3 h) or c6i.8xlarge
     (full history ≈ 6-10 h).
   - `Spot=true` — ~60-70% cheaper; job resumes from stored streams on retry.
   - `MonthlyBudgetUsd` + `AlarmEmail` — hard budget alarm on day one.
   - No SSH keypair required: shell via `aws ssm start-session` (output
     `ConnectCommand`).
3. The instance clones this repo, runs `scripts/run_job.py`, writes results to
   `s3://aws-c3-<account-id>/`. Progress: `tail /var/log/aws-c3.log`.
4. Tear down when done — the data bucket is Retained by policy.

## Cost budget (estimates)

One-off full-history build (129 crawls ≈ 30 TiB CDX, 39.5k parts):

| Item | Amount |
|---|---|
| S3 GETs of CC data (same-region, us-east-1) | $0 |
| EC2 c6i.8xlarge spot ≈ 6-10 h | $3-8 |
| EC2 c6i.8xlarge on-demand | $12-15 |
| Master artifact (hash, zstd/gz) on S3 Standard, 100-250 GB | $2.30-5.75/month |
| Per-crawl streams (if kept hot), ~5 TB for all 129 | $115/month → auto-transitions to Deep Archive after 30 days ≈ **$5/month** |
| Fresh views (Deep Archive after 90 d) | <$1/month |
| Budget guard | alarm at 80% actual + 100% forecast |

Monthly increment (one new crawl, ~230 GiB CDX):

| Item | Amount |
|---|---|
| c6i.4xlarge spot ~2.5 h | $1-2 |
| New master delta (~10 GB growth) | ~$0.25/month |
| **All-in steady state** | **≈ $6-12/month** (spot, streams in Deep Archive) |

Data transfer: $0 (CC data leaves S3 us-east-1 only via our in-region job;
the compact fresh set downloads out of the account are tiny).

Not included: whatever you decide to *do* with the fresh set afterwards
(e.g. content fetch — that's the real bill, see below).

## Why not this host?

Measured 9 MB/s from the home/host to data.commoncrawl.org → ~5 weeks of
continuous wire time for all history. The in-region EC2 route runs the same
bytes in single-digit hours for less than a coffee.

## Repo layout

```
cfn/aws-c3.yaml        CloudFormation (bucket, IAM, spot/on-demand EC2, budget)
scripts/cc_fresh.py    parser, bucket builder, streaming merge (stdlib only)
scripts/run_job.py     job driver (env-configured, resumable)
tests/test_offline.py  offline end-to-end test on synthetic CDX parts
```

Python 3.9+; production needs boto3 (SSM role provides access); tests need
nothing but stdlib + network-free fixtures.

## Semantics & limits

* First/last dates use the capture timestamp (CDXJ field 2), clamped into the
  crawl window from collinfo.json.
* Hash = blake2b-128, size 16, over the raw surt urlkey. 16 bytes is collision-
  safe at 10^10+ scale for membership purposes (birthday bound ~2^63 inserts).
* "Fresh" = first capture inside the crawl's window. This is URL-level
  freshness — a rewritten OLD page is NOT fresh (use the CDX `digest` field for
  content-level freshness; not wired here yet).
* Interrupted spot instances: bucket streams up to the last finished part are
  not reused (parts are streamed whole); re-runs re-stream from part 00000,
  but pre-completed crawls' streams and masters are always reused.
* Athena alternative: you can express the same "first seen" over the public
  `cc-index-table` parquet with SQL for ~$50-100 of scan per full history —
  slower to iterate on, zero infra. This pipeline is the ~$10 way.
