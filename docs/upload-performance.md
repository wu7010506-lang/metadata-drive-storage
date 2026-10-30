# Upload performance measurements

The UI also exposes [per-phase upload timings](upload-phase-timings.md) to
separate reception, queueing, preparation and transfer; this adds observability,
not a new measured speedup.
An [experimental shared request pacer](upload-request-pacing.md) is available
for deferred A/B testing; it is disabled in the live configuration and has no
new demonstrated cloud throughput or quota-utilization result.

Latest selection: [2026-10-22 response/capacity/granularity follow-up](performance-wire-20261022.md)
retains the [64-worker selection](performance-20261022.md), with the
[resume-work reductions](performance-extreme-20261022.md).
The historical decisions below describe their own measurement dates.

Measured on 2026-10-06 using `scripts/benchmark.py`. Each pair uploads the same
random bytes under different fresh dataset names so resume does not skip work.
Timing includes local preparation, ID generation, connection initialization,
remote creation and committed SQLite completion records. Full download and
SHA-256 verification is performed after the timed upload.

Description payload size: 25,000 bytes. Request delay: 0 seconds.

| Pair | Chunks | Workers | Upload seconds | Chunks/second | Retries |
|---|---:|---:|---:|---:|---:|
| A | 32 | 4 | 14.320 | 2.235 | 0 |
| A | 32 | 8 | 15.529 | 2.061 | 0 |
| B | 128 | 8 | 32.296 | 3.963 | 0 |
| B | 128 | 4 | 54.357 | 2.355 | 0 |
| C | 128 | 8 | 28.545 | 4.484 | 0 |
| C | 128 | 16 | 20.690 | 6.186 | 0 |

All runs passed reconstruction verification. Pair C reduced upload time by
27.52% with 16 workers; the local configuration now uses 16 workers. Small files
may be slower with additional workers because of connection initialization.
Individual network samples vary and these measurements do not establish
sustained throughput for thousands of chunks or behaviour under rate limiting.

Pair C Drive dataset IDs:

- 8 workers: `ab89c74a-69c6-4406-a62f-0876d17f8e31`
- 16 workers: `aaf92dbc-22dc-46ce-a179-56d2b297050a`

The benchmark data remains in the configured Drive folder and SQLite index.

## 2026-10-08: durable index batching

`python scripts/benchmark_sqlite.py --chunks 512 --repeats 3` compared
per-chunk pending/completion transactions with groups of 64 on the same local
disk. Profile order was reversed on alternate rounds. No Google API calls were
made and the live index was not touched.

| Repeat | One-chunk transactions | Seconds | Groups of 64 transactions | Seconds |
|---|---:|---:|---:|---:|
| 1 | 1,024 | 5.1190 | 16 | 0.0868 |
| 2 | 1,024 | 5.3611 | 16 | 0.0878 |
| 3 | 1,024 | 5.1120 | 16 | 0.0790 |

Median time fell from 5.1190 to 0.0868 seconds (about 59x for **this isolated
SQLite workload**, not overall upload throughput). Every run verified all 512
completion records. Existing rollback-journal and disk synchronization settings
were retained.

The uploader now reserves a bounded ID window in a transaction before submitting
remote creates. A single coordinator commits available verified completions;
with individual requests, completion groups can still be small. Therefore the
microbenchmark's fixed groups of 64 do not imply 59x faster uploads.

Experimental Drive batching is opt-in through `UPLOAD_BATCH_SIZE` (1..100).
Each worker owns its client; successful subresponses are checked and committed,
transient subresponses use bounded backoff, and a lost response retains its ID
for conflict confirmation on resume. Partial failures stop new submissions and
drain in-flight successes. Batch request quotas are still counted per item.

Live comparisons use an isolated benchmark SQLite database and equal random
bytes within each run. The `--cleanup` option permanently removes only objects
created and verified by that benchmark; user datasets are not touched.

### Initial live batch comparison

128 chunks (2,399,872 original bytes), description payload 25,000, fixed delay 0.
Each profile was measured twice, reversing order on the second round. Full
reconstruction passed in every case; all runs had zero retries / HTTP 429s.

| Workers x batch size | First upload seconds | Second upload seconds | Median seconds |
|---|---:|---:|---:|
| 16 x 1 | 18.662 | 19.566 | 19.114 |
| 2 x 16 | 39.125 | 39.568 | 39.347 |
| 4 x 16 | 22.175 | 22.314 | 22.245 |

The two batch profiles were slower than 16 individual requests despite fewer
SQLite connection scopes (15 vs 121/129). Batch support alone is therefore not
evidence of a speedup. This measurement does not identify how Google schedules
subrequests internally. Raw results: `data/benchmark-batch-20261008.jsonl`.

The local SQL time was approximately 0.75..0.78 seconds for individual requests
and 0.08..0.09 seconds for batches. That suggests remote/client work dominates
this small upload after index batching; it is not a CPU/disk profile of a large
file. Sequential reconstruction took 47.78..56.41 seconds, outside upload timing.

### Selected profile: 8 workers x 16 subrequests

Two additional equal-size paired comparisons tested 8 x 16 against 16 x 1,
reversing execution order. Each pair shared exactly the same input; the second
pair generated fresh random bytes of the same length. Both pairs used 128 chunks
and 2,399,872 original bytes. All four uploads and restorations passed with no
upload retries or HTTP 429s.

| Pair | Execution order | 16 x 1 seconds | 8 x 16 seconds |
|---|---|---:|---:|
| D | 8 x 16, then 16 x 1 | 19.757 | 14.025 |
| E | 16 x 1, then 8 x 16 | 19.252 | 13.483 |

Mean upload time fell from 19.5045 to 13.754 seconds: about 29.5% less time,
or 1.42x throughput for this workload. The local `.env` now selects
`UPLOAD_WORKERS=8`, `UPLOAD_BATCH_SIZE=16`, `DOWNLOAD_WORKERS=16`. Environment
changes apply only after the project server is started/restarted. Generic
configuration defaults keep batch size and download workers at 1.

On pair D's 8 x 16 dataset, sequential reconstruction took 51.542 seconds and
16-reader reconstruction of the **same dataset** took 11.252 seconds (4.58x).
Additional 16-reader checks took 9.681, 8.690 and 9.223 seconds. Each check included
independent worker-client initialization and full chunk/full-file verification.
Readers were not tested under sustained quota pressure or on GiB-sized files.

Raw verified results: `data/benchmark-batch-final-20261008.jsonl`. The two earlier
100-item cleanup batches timed out after measurement, without invalidating
upload/verification results. Cleanup was repaired to use 16-item batches,
bounded retries and idempotent 404 handling; both affected datasets were then
cleaned using their verified report/index. Only benchmark-created objects were
deleted, permanently. The benchmark databases remain isolated from the live
index and are audit records after remote cleanup.

The final small-batch cleanup also encountered one HTTP 409 conflict. The
cleanup-only retry policy now retries those same DELETE IDs with bounded
backoff, while ordinary permanent API errors still abort. Conflict is never
treated as deletion success. Recovery completed successfully; all ten datasets
created by these 2026-10-08 comparisons (1,280 test objects) were removed. The
58-test offline suite includes lost-response, 404 and DELETE-conflict recovery.

At the end of the 2026-10-08 phase, the downloader bounded queued reads and consumed verified payloads in order,
but still reconstructs the full file in memory. True streaming is not part of
this change. SQLite WAL, synchronization weakening, larger metadata payloads,
account rotation and quota evasion were not enabled.

## 2026-10-11: bounded-memory transfers and persistent clients

The next phase replaced whole-file upload buffers with incremental compression
into an 8 MiB memory/disk spool, then incremental Base64 partitioning. File
downloads now incrementally decode and decompress directly into an atomic private
destination. SQLite reads use bounded windows rather than a full chunk catalogue.
UI multipart staging no longer copies the received file into a second source file.
The actual-file limit is 2048 MiB inclusive, with bounded multipart headroom.

Version-1 byte encoding was compared against the old one-shot encoder for empty,
tiny, boundary-size and random >2 MiB inputs, for both zstd and gzip fallback.
Odd 31-character partitions, interrupted uploads, ambiguous writes, batch errors,
corruption, and preserving an existing output on failed verification remain
covered. All reserved upload hashes are checked before resumed remote creates.

The UI now reuses thread-confined clients and workers between jobs and shares a
network-thread budget and request pacing across uploads/downloads. It avoids
redundant per-job folder checks, but still validates actual transfer responses.
Normal shutdown drains jobs and closes clients. No metadata capacity increase,
backoff removal, SQLite synchronization weakening or quota evasion was introduced.

### Local codec checks (separate processes)

| Virtual input | Preparation seconds | Restoration seconds | Python allocation peak MiB | Process peak working set MiB |
|---|---:|---:|---:|---:|
| 1 MiB repeated `x` | 0.011 | 0.003 | 2.29 | 24.02 |
| 2048 MiB repeated `x` | 2.496 | 3.429 | 2.38 | 28.58 |
| 256 MiB random | 0.896 | 3.295 | 11.06 | 41.94 |

All three passed size/SHA-256 verification and bounded-read/write assertions.
The 2 GiB input compressed to 65,558 bytes, so this is a memory/size-boundary check,
not a realistic incompressible 2 GiB transfer. The random input compressed to
268,441,613 bytes and exercised the disk-spill path. Inputs were generated one
block at a time; outputs were hashed rather than written to final disk. Native
codec memory is included in process working set, not Python tracemalloc alone.
Neither table measures UI multipart I/O, HTTP transport, final fsync/rename,
Drive throughput, or a full 2 GiB remote round trip.

After adding 1/2/3-character legacy partition coverage, the final offline suite
passed 94 tests and `compileall` succeeded. A repeat of the 2 GiB virtual check
took 2.562/3.316 seconds (prepare/restore), with 28.66 MiB process peak; the
256 MiB random repeat took 0.831/3.246 seconds with 41.44 MiB process peak.
Real UI route tests additionally cover multipart staging without a copy, an
inclusive scaled file-size boundary, download progress, legacy-route cleanup,
persisting final job files, and HTTP Range re-requests. These use a mock Drive,
not a remote 2 GiB dataset. Tests also cover disk-spool cleanup on failures and
an ENOSPC error without publishing output.

### Read-only real connection check

Three paired checks compared building/authenticating a fresh client and reading
folder metadata against reading the same folder through an already-established
worker-owned connection. A first warm-up query was excluded. No remote data was
created, modified or deleted (seven folder reads total).

| Pair | Cold seconds | Reused seconds |
|---|---:|---:|
| 1 | 1.113 | 0.329 |
| 2 | 1.186 | 0.485 |
| 3 | 1.107 | 0.332 |

Medians: 1.113 versus 0.332 seconds, about 70.2% less time for this particular
metadata read including cold setup/authentication. This supports connection reuse
for repeated small jobs; it is not an end-to-end upload/download speed multiplier.
Large transfers still make many Drive requests: 2 GiB incompressible data at
25,000 ASCII bytes per chunk is approximately 114,500 chunks. Pooling and bounded
memory do not remove that remote request cost or Google's quotas.

The existing server was not restarted while jobs might be active. Source/config
changes apply on the next restart. Download job files persist for local use and
Range re-requests; a failed remote reconstruction still starts over on retry.

## 2026-10-11: request density, scheduling and initial authentication

Tests use isolated indexes and the configured test folder, retaining retries,
stable IDs, verification, and durable commits. A 64-chunk baseline uploaded in
12.163 seconds and restored in 6.389 with 16 readers. SQLite scopes totalled just
0.068 seconds: network/client work, rather than the local index, dominated this
sample. Ranked hypotheses were fewer objects through packing/encoding, smaller
concurrent batches, then cached initial authentication.

### Packing: fewer objects did not make upload faster

Each profile used exactly the same 2,399,872 random input bytes; order reversed
in round two. Combined packing uses the existing 8,500-byte name and 25,000-byte
description capacities; neither individual field capacity was increased.

| Metadata + encoding | Chunks | Upload 1 seconds | Upload 2 seconds | Mean seconds |
|---|---:|---:|---:|---:|
| description + Base64URL | 128 | 13.435 | 12.666 | 13.0505 |
| name + description + Base64URL | 96 | 13.967 | 16.155 | 15.061 |
| name + description + Base85 | 90 | 15.272 | 15.466 | 15.369 |

The density hypothesis was falsified as an upload speedup for this workload.
Combined/Base85 remain explicitly experimental, **not the default**. Some
download times improved with fewer objects; that does not prove faster upload.
All six restored exactly, with zero upload retries/429s. Raw report:
`data/optimization-20261011-packing.jsonl`. All 628 newly created test objects
were permanently removed, without deleting user datasets. Cleanup retries
occurred after measurement, separately from reported upload retries.

### Scheduling: a smaller batch with more workers helped

Equal 2,399,872-byte input, 128 description/Base64URL chunks, reversed order on
round two, legacy independent authentication:

| Workers x batch size | Upload 1 seconds | Upload 2 seconds | Mean seconds |
|---|---:|---:|---:|
| 8 x 16 | 13.192 | 13.941 | 13.5665 |
| 16 x 8 | 12.639 | 11.755 | 12.197 |
| 16 x 4 | 13.165 | 15.596 | 14.3805 |

16 x 8 used about 10.1% less time (1.11x throughput) than 8 x 16. 16 x 4 did not
improve it. These are short samples, not confidence intervals or GiB guarantees.
All six restored exactly, with zero upload retries/429s. Report:
`data/optimization-20261011-scheduling.jsonl`. All 768 new test objects were
permanently removed.

### Authentication A/B: confirmed cold-worker improvement

On the same 2,399,872-byte random source, both profiles used 16 workers x 8
requests, description/Base64URL, 128 chunks, and fresh HTTP clients. Order was
legacy/shared in round one and shared/legacy in round two. Startup folder checks
are outside upload timing (like UI startup); worker initialization is included.

| Initial authentication | Upload 1 seconds | Upload 2 seconds | Mean seconds |
|---|---:|---:|---:|
| Independent legacy token retrieval | 11.180 | 11.327 | 11.2535 |
| Cached initial token, independent clients | 9.099 | 8.947 | 9.023 |

Mean upload time fell by 19.8% (1.25x throughput). Observed original-byte
throughput was 0.2515/0.2558 MiB/s. All four restored exactly, with zero upload
retries/429s. Report: `data/optimization-20261011-auth.jsonl`. This measures cold
workers; it is not an additional 20% gain on an already-warm persistent pool,
nor a native Drive comparison. The earlier scheduling and this authentication
experiment are separate comparisons; their percentages should not be added.

All 512 authentication-test objects were permanently removed after verification.
Including the 64-object baseline, packing and scheduling tests, this phase
created, verified and removed 1,972 test objects, scoped only to newly recorded
benchmark IDs. The live SQLite index and user datasets were not altered.

The local `.env` now selects 16 x 8, description + Base64URL. The UI startup/CLI
uses the cached-token factory. Generic configuration defaults remain conservative.
The user's running server was not restarted; changes apply on its next restart.

### Serial ID requests and cached initial credentials

ID allocation now adapts from 100 up to 1,000 IDs per call, Google's documented
maximum in the [create-file guide](https://developers.google.com/workspace/drive/api/guides/create-file).
IDs are still committed as pending before creates; the UI reuses unused IDs.
An offline 168-chunk upload needed one generateIds call instead of two. This is
an API-count reduction, not a measured throughput multiplier.

The startup/CLI factory loads the key once and refreshes its initial token under
a lock. Each client receives independent mutable credentials and HTTP connections.
Long-lived clients still refresh independently at expiry. Concurrent tests
verified one key load/initial refresh with eight separate credential objects.
A live read-only check verified two independent clients/folder reads, taking
1.124 seconds initially and 0.749 with the cached token. This is cold-client
setup, not an upload result or reused HTTP-connection measurement.

New manifests record encoding; old indexes migrate with Base64URL as default.
Changing encoding/backend cannot resume a different format. Combined hashes cover
both fields, and version-2 datasets retain corruption/ambiguous-write checks.
The expanded offline suite passed 124 tests. A virtual Base85 2 GiB repeated-input
check verified exact size/hash with 2.67 MiB Python allocation peak and 31.21 MiB
process working set. It is not a physical/remote incompressible 2 GiB benchmark.

Batching does not merge quota accounting: Google's
[performance guide](https://developers.google.com/workspace/drive/api/guides/performance)
counts each subrequest separately. The remaining cost is creating many objects,
not just transferring encoded bytes. No native Drive baseline was supplied or
measured; matching it is unproven. Native bulk upload would require an explicitly
approved normal file-content mode that consumes storage space. Service accounts
require Shared Drive or user OAuth for that path according to the
[Drive error guide](https://developers.google.com/workspace/drive/api/guides/handle-errors#storageQuotaExceeded).

## 2026-10-18: bounded worker scaling

The upload-worker validation ceiling was raised from 16 to 32 so 24/32 could be
measured. Download workers remain capped at 16. A red config test and a CLI
validation failure reproduced the former ceiling before the change. Tests cover
thread-owned clients, draining successes and resuming failures at 24/32, as well
as lazy persistent-client initialization for tiny jobs at 16/24/32. A tiny job
creates one network client, not the configured maximum, and reuses it next time.

Before measuring, the outstanding verified 64 MiB quota-capture benchmark's
3,580 remote IDs were fully cleaned up. That earlier upload took 199.938 seconds
(0.3201 MiB/s) with one retry and no 429; it is not a paired baseline for today's
different input sizes. The interrupted second quota-capture test remains separate,
unverified and untouched. Neither the live SQLite index nor user datasets are
part of these benchmarks.

### Phase A: more workers alone did not yield a reliable improvement

Each profile uploaded exactly the same 9,599,872 random bytes. The target was
512 chunks; compressor overhead made **513 actual chunks** in every run. All
profiles used description/Base64URL, 25,000-byte payloads, batches of 8, cached
initial authentication and cold independent workers. Order was 16/24/32, then
32/24/16. Each upload restored exactly before the next started. Cleanup was
deferred until all measurements, not run in the background during uploads.

| Workers x batch | Upload 1 seconds | Upload 2 seconds | Mean seconds | Total upload retry events | Quota retry events |
|---|---:|---:|---:|---:|---:|
| 16 x 8 | 24.950 | 69.656 | 47.303 | 1 | 0 |
| 24 x 8 | 20.947 | 81.447 | 51.197 | 1 | 0 |
| 32 x 8 | 77.707 | 79.326 | 78.5165 | 3 | 0 |

24 workers won the first run but not the repeat; selecting it on that first
sample would have been misleading. 32 workers were worse in both rounds. No
larger-worker default was selected from this phase. Upload SQLite scopes totalled
0.572..1.169 seconds; shortening local index work alone cannot account for the
large observed delays. No upload quota backoff (recognized 403 rate limit/429)
was logged. The absence of a quota error is not proof of unlimited capacity.

Report: `data/scaling-20261018-512.jsonl`. Each of the six datasets is verified;
all 3,078 newly created objects were subsequently removed, with idempotent
recovery for cleanup retries. Cleanup traffic was after all upload measurements.

### Measurement/cleanup hygiene

Benchmark reports now distinguish `quota_retries` (logged 403 rate-limit/429
backoff events) from the existing 429-only field, and subsequent runs include a
`retry_http_statuses` histogram (0 denotes a transport failure without HTTP
status). One batch retry event can represent multiple failed subrequests; these
are not Cloud Console quota-unit counters. Retry logs name the exception class
and HTTP status without logging tokens or request payloads.

`--cleanup-at-end` separates deletes from profile timing. Optional cleanup
workers (1..8) each own a client/callback/retry state; batches stay at 16 items
and retries stay bounded/idempotent. Cleanup still rejects incomplete manifests.
Tests exercise lost responses, 404, transient delete conflicts, worker bounds,
and preserving unrelated remote objects. These harness changes do not weaken
production durability, verification or quota handling.

### Phase B: equal in-flight budget, smaller batches

The next comparison kept the nominal in-flight budget at 128 items: 16 x 8
versus 32 x 4. Each used the same new 9,599,872-byte random input (513 chunks),
with order 16 x 8 / 32 x 4, then reversed. Phase A cleanup finished before this
phase began; Phase B deletes were again deferred until all measurements.

| Workers x batch | Upload 1 seconds | Upload 2 seconds | Mean seconds |
|---|---:|---:|---:|
| 16 x 8 | 25.429 | 25.296 | 25.3625 |
| 32 x 4 | 38.657 | 19.775 | 29.216 |

All four restored exactly, with zero upload retries and zero quota backoff.
32 x 4's mean was about 15.2% slower despite one faster sample; it was not
selected. Results do not establish a statistically precise long-run difference,
but provide no basis for a reliable speedup. This phase also demonstrates
variation without logged retries; not all delays can be attributed to backoff.
Report: `data/scaling-20261018-small-batch.jsonl`.

All 2,052 Phase B objects were then removed. Across both worker-scaling phases,
all ten uploads restored exactly and all 5,130 newly created test objects were
permanently cleaned up. User datasets and the live index were not changed.

Decision: retain this machine's `UPLOAD_WORKERS=16`, `UPLOAD_BATCH_SIZE=8`,
description/Base64URL, and 16 download workers. The supported upload ceiling
remains 32 for explicit experiments; the larger values are not recommended
defaults from these measurements. The server was not restarted or its jobs
interrupted. No faster configuration, native Drive parity, sustained GiB
throughput, or 99% quota utilization was achieved by these experiments.

The final offline suite passed 140 tests and `compileall` succeeded. Neither
individual metadata capacity nor the 2 GiB actual-input limit changed. The
bounded-memory codec, pre-write durable ID reservation, verified progress,
corruption detection, atomic download publication and retry protections remain.

## 2026-10-18: latency and parent-layout follow-up

### HTTP timeout comparison (not selected as a speedup)

Same 9,599,872-byte random input, 513 chunks, 16 x 8,
description/Base64URL. Order 60/20 seconds, then reversed. Folder/auth startup,
download and deferred cleanup are outside upload timing. Each run restored
exactly with zero upload retry/403-rate-limit/429 events.

| HTTP socket inactivity timeout | Upload 1 seconds | Upload 2 seconds | Mean seconds |
|---|---:|---:|---:|
| 60 seconds | 26.462 | 27.029 | 26.7455 |
| 20 seconds | 26.123 | 26.864 | 26.4935 |

The approximately 0.9% difference is too small to establish a meaningful gain.
The hypothesized 60-second tail did **not reproduce in this comparison**. No
lower default was selected. Report: `data/timeout-20261018-512.jsonl`; all four
datasets verified and all 2,052 newly created objects were removed.

The new bounded `upload_http` instrumentation measured 65 outer batch requests
per run: median latency 5.092..5.671 seconds, p95 8.510..8.786, maximum
9.040..12.576. Every outer batch response was HTTP 200, and every chunk was
independently verified. ID generation was one GET of 0.232..0.243 seconds.
Concurrent request-duration totals are not wall-clock time or quota units.
Metrics never retain URLs, payloads, headers or error messages; they store only
category, duration, HTTP status or exception class. Per-category latency samples
are capped at 10,000, while counts/totals/max cover all calls.

`GOOGLE_API_HTTP_TIMEOUT_SECONDS` is now explicit (5..120, default still 60) for
the UI startup and transfer CLIs. It is a socket inactivity timeout, not a whole
request/job deadline. Each thread has a separate authorized HTTP transport and
credential copy; token refresh has its own unchanged timeout. SDK redirect
semantics (including not treating 308 as an ordinary redirect) are retained.

### Parent sharding experiment (not selected)

Hypothesis: indexing many concurrent creations under one parent might dominate
latency. The benchmark-only `scripts/benchmark_layout.py` compared the configured
root with eight newly created, marked child folders. Same random input bytes,
same 513 chunks, 16 x 8, 60-second timeout and full verified restoration. Order
was root/eight, then reversed; deletes began only after all measurements.

| Parent layout | Upload 1 seconds | Upload 2 seconds | Mean seconds |
|---|---:|---:|---:|
| Configured root | 29.736 | 40.034 | 34.885 |
| Eight new child folders | 39.674 | 42.312 | 40.993 |

Eight folders were slower in both rounds and also added 8.311/11.034 seconds of
setup, excluded from upload timing. The simple parent-contention hypothesis did
not yield a speedup here. All four restored exactly with zero upload retries or
quota-backoff events. Successful batch responses could take up to 21.981 seconds;
latency variation is not necessarily a logged timeout or a quota error.
Report: `data/layout-20261018-512.jsonl`.

All 2,052 new layout-test chunk objects and all 16 newly created test folders
were subsequently removed. The journal confirms every test folder is deleted.
Cleanup retries/HTTP 500s occurred after measurement and are not upload retries.

No production layout changed and no existing object moved. The experimental
proxy only rewrites newly uploaded chunk parents within these marked test folders.
Its isolated journal records folder IDs before remote creation and verifies
conflicts before accepting them. Folder cleanup checks ID/MIME/parent/marker and
lists children, including trashed children; it refuses nonempty or unowned folders.
Tests cover real upload/restore/resume, rejecting unrelated parents, journaled
ambiguous folder creation, and refusing deletion when unrelated data is present.

C: had approximately 500 MiB free during this phase. Subsequent large test
fixtures/source spools were placed in the configured E: temporary area rather
than deleting any user files to make space.

### Selected singleton fast path

The uploader formerly wrapped even a one-item group in multipart batch HTTP.
It now calls the same verified direct-create/conflict-confirmation path for a
group of one, including the final singleton of a large upload. Groups of two or
more still batch. Preallocated IDs are durable before submission, successful
progress still follows a commit, and a lost create response is confirmed with
the same ID on retry/resume rather than duplicating an object.

Controlled A/B: exactly the same random 16,384-byte input, one chunk, serial
worker, configured batch size 8, 60-second timeout. The benchmark forces the old
singleton batch envelope only in the control case. Four paired rounds alternate
execution order. Folder/authentication setup and verified download are outside
upload timing; ID generation and SQLite commits are included. HTTP connections
are warm from the folder check; this is not browser-to-Drive end-to-end timing.

| Singleton protocol | Round 1 seconds | Round 2 | Round 3 | Round 4 | Mean seconds |
|---|---:|---:|---:|---:|---:|
| Direct (new auto path) | 1.665 | 1.622 | 1.497 | 1.358 | 1.5355 |
| Forced batch envelope (old path) | 2.585 | 2.585 | 2.356 | 2.314 | 2.4600 |

Direct won every pair and reduced mean time by about 37.6% (1.60x throughput)
**for this one-chunk workload**. This is not a 38% large-file speedup. All eight
restored exactly, with zero retries or quota errors, and all eight test objects
were removed. Report: `data/tiny-protocol-20261018.jsonl`.

Regression tests first observed the unnecessary one-item batch, then verified
direct calls, restoration and same-ID recovery after an ambiguous write. Tests
also retain multi-item partial failures and resumable success draining. The
benchmark's forced-envelope flag only permits one worker so it actually exercises
the intended call site. Benchmark clients are explicitly closed between runs.

### Wider-batch comparison stopped on explicit rate limiting

The next planned comparison held 16 workers and changed batch width 8/16. It
encountered HTTP 403 `userRateLimitExceeded`. The first 16 x 8 profile eventually
restored exactly in 78.702 seconds, with 10 logged quota-backoff events and no
429. The 16 x 16 profile had started before the operator observed the buffered
warning output; the benchmark process was then stopped. Its isolated index had
321 complete and 192 pending entries. It was **not** declared verified and no
wider-batch result/default was selected.

Report: `data/batch-width-20261018-512.jsonl` (only the completed, verified
profile). Its upload HTTP summary includes 73 outer batch calls; every outer
batch status was 200 despite recognized rate-limit subresponses. This is direct
evidence for inspecting subresponses/retry reasons rather than only outer 200s
or a project-wide utilization percentage. The exact active quota dimension and
headroom still require Cloud Console; the response does not prove the previously
shown project-wide quota was at 100%. See Google's
[usage limits](https://developers.google.com/workspace/drive/api/guides/limits)
and [error guidance](https://developers.google.com/workspace/drive/api/guides/handle-errors).

The benchmark harness now stops subsequent profiles after a recognized quota
backoff during startup or transfer, retaining its isolated report/index and
skipping immediate automated cleanup. Production's bounded backoff, integrity
checks and durable IDs are unchanged; this is not quota evasion, account rotation,
or a claim of automatic global quota control.

Recovery uses `scripts/cleanup_aborted_benchmark.py`, not a fake completion flag.
It rejects the live index, requires exact dataset ID/benchmark filename, and
checks every present remote ID, parent, zero content size, MIME, metadata,
readable name and payload hash before deletion. Unknown/changed objects are
refused. Missing preallocated IDs are recorded as absent; original pending/
complete states remain unchanged. At most four single-object workers run, and
errors stop new submissions while in-flight work drains. A separate cleanup
journal supports recovery without claiming successful upload/restoration.

Recovery completed: both datasets' 513 recorded IDs were individually verified
and deleted (1,026 objects total, no absent IDs). The second manifest's 321
complete/192 pending statuses were not changed. Its incomplete upload has no
speed or restoration-success claim. Across this follow-up, all 5,138 newly
created test chunk objects and all 16 new test folders were removed. The earlier
user-cancelled quota-capture dataset from before this follow-up remains outside
this cleanup scope. Isolated SQLite/report files remain as audit records.

Final validation: **169 offline tests passed**, and `compileall` succeeded.
Large test fixtures used three explicitly named E: scratch directories; those
generated fixtures were removed after tests. The interrupted benchmark's random
source was identified by the manifest SHA-256 before deletion. No private source
files, live index or running user server was removed/restarted. Main settings
remain description/Base64URL, 16 x 8, 60-second timeout and 2 GiB input limit.
The singleton fast path applies after the server is restarted with the new code.
