# 2026-10-22: confirmed writes before replaying a lost response

## Selected implementation

`NameChunkUploader` now handles a retryable transport exception after a create
by reading its already-reserved ID before sending another mutation. Full ID,
payload and app-properties matching is unchanged. A matching object is confirmed;
only a definite 404 allows another create attempt. A failed lookup or a conflicting
object is not permission to overwrite/replay: the job fails recoverably with its
stable ID/index intact. Singletons and multi-item batches follow the same policy.
Already acknowledged items are not read again. Healthy uploads add no GETs.

The coordinator still commits verified completions before reporting progress.
Original hashing/compression, reservations, chunk partitioning, legacy manifests,
bounded retries and TLS verification are retained. No transport-level blind replay,
identity rotation, quota changes or artificial traffic is enabled in production.

The new regression first failed against the original actual batch/retry caller:
four remotely stored objects caused eight create executions after a lost response.
After the fix they cause four, and partially stored groups retry only definite
absences. Tests also reject corrupted read-back and unavailable/forbidden lookup
without another create, preserve thread confinement, and keep the old policy as
an explicit benchmark control (`confirm_ambiguous_writes=False`).

## Direct observation motivating the fix

The user's completed 797,987,631-byte upload had 38,689 chunks. Server timing was
2,582.726s total: reception 206.701s, preparation 3.748s, transfer 2,372.259s.
This is the application's completion state, not an additional full download.
The user dataset was neither modified nor used as benchmark input.

During subsequent tests, eight pending records from the stopped pacing experiment
were read via a single metadata GET batch. All eight were already remotely stored
with the expected dataset ID and payload SHA-256; verification took 2.195s. This
shows ambiguous success occurred in this environment, not that every timeout had
the same cause. The previous algorithm confirmed only after replay returned 409.

## Rejected/unselected experiments

All comparisons use isolated indexes, fresh benchmark names and identical source
bytes within a run. Download verification is outside upload timing.

### Adaptive request pacing

32 MiB, 1,790 chunks, 64x8, fixed/adaptive/adaptive/fixed plan:

| Profile | Upload seconds | Outcome |
|---|---:|---|
| Fixed, first | 56.535 | Restored; one HTTP 500 retry |
| Adaptive ceiling 11,400/min, first | 71.263 | Restored; zero upload retries |
| Adaptive, second | Interrupted | Large timeout wave; only 144 committed completions, 1,024 reserved rows |
| Fixed, second | Not run | Test stopped to avoid continuing a regressing candidate |

Only the specifically verified benchmark process (PID 18040) was stopped, not
the UI. Raw report: `data/pacing-live-20261022.jsonl`; isolated index retains the
partial experiment. This incomplete comparison is **not evidence of a speedup**.
Pacing remains disabled. The controller can reduce its target excessively under
a prolonged, staggered timeout wave; synthetic ramp tests did not establish live
stability and it was not promoted.

### Gzip request bodies (distinct from existing gzip responses)

A two-object capability probe passed full reconstruction. Its actual HTTP request
body decreased from 44,087 to 32,479 bytes. Those counts exclude headers/TLS and
are not the original file's size. This endpoint accepted the encoding in this
probe; no universal endpoint-support claim is made.

32 MiB equal-input trial, 64x8, identity/gzip/gzip/identity plan:

| Profile | Upload seconds | Outcome |
|---|---:|---|
| Identity, first | 315.476 | Restored; five outer transport timeouts |
| Gzip, first | 52.953 | Restored; zero upload retries |
| Gzip, second | Interrupted | Large timeout wave, no committed completion at stop |
| Identity, second | Not run | No complete reversed comparison |

The one good gzip run sent 35,285,055 encoded request-body bytes versus its
48,341,888 uncompressed body bytes. Less traffic is established; a stable upload
speedup is **not**. The first identity run was dominated by a failed tail batch,
so the apparent 315s/53s ratio must not be attributed to compression. The stopped
test process was PID 30424. Reports/indexes are `data/request-body-gzip-20261022.*`
and `data/request-gzip-probe-20261022.*`. Production request compression stays off.

## Controlled lost-response comparison

Exactly 2,399,872 identical random source bytes, 128 chunks, 16x4, socket timeout
20s. A benchmark-only wrapper discarded exactly one successful create-batch
response per run. It did not modify the request/data and was disabled before
download/cleanup. The explicit `InjectedBatchResponseLoss` outcome distinguishes
this from a real socket timeout. Order: legacy/readback/readback/legacy.

| Policy | First seconds | Second seconds | Mean seconds | Outer batches/run |
|---|---:|---:|---:|---:|
| Legacy replay | 31.142 | 22.420 | 26.781 | 33 |
| Verified read-back | 13.727 | 22.766 | 18.2465 | 32 |

All four fully restored, with zero observed upload quota backoffs. The mean is
31.87% lower, but one reversed pair was essentially tied (read-back 1.54% slower).
Network variability is material. A single scoped GET+DELETE diagnostic of a
different test object also overlapped the final legacy run; do not interpret this
as a perfectly isolated causal estimate for general file uploads. What is directly
demonstrated is elimination of one four-item create replay and its backoff.

The first report's `ambiguous_chunks_confirmed` column is incorrectly zero because
the counter was attached only to `src.retry`, while confirmations log via
`src.uploader`. Four confirmation warnings per read-back run and the HTTP counts
establish recovery. The counter hookup was corrected afterward; raw results were
not rewritten. Report: `data/ambiguous-recovery-live-20261022.jsonl`.

## Normal, non-injected comparison

16 MiB identical random input, 895 chunks, 64x8, socket timeout 20s. Same reversed
order, no injected faults. All four fully restored.

| Policy | First seconds | Second seconds | Mean seconds | Observed upload retries |
|---|---:|---:|---:|---:|
| Legacy | 79.665 | 46.291 | 62.978 | 16 / 1 |
| Read-back | 29.437 | 26.987 | 28.212 | 0 / 0 |

The observed mean is 55.20% lower. **Neither read-back run actually entered the
new recovery branch** (`ambiguous_chunks_confirmed=0`, no upload timeouts). Thus
this difference cannot establish a 55% causal speedup from that branch; the old
runs encountered transient failures and the new ones did not. This validates
healthy-path compatibility/no extra requests and documents variability. Some
download-verification requests also retried, outside upload timing. The tested
20s socket timeout is not the production's unchanged 60s setting.
Report: `data/readback-natural-20261022.jsonl`.

## Isolated recovery-latency probe

A separate single-batch (60,000 bytes, four chunks) comparison measures time from
the one deliberately discarded successful response until the upload returns.
This removes most initial create/connection latency from the recovery comparison.
The counter is fixed in this probe. Four reversed pairs completed:

| Policy | Recovery seconds, in run order | Mean | Median |
|---|---|---:|---:|
| Legacy replay | 3.980 / 4.457 / 4.846 / 5.370 | 4.663 | 4.652 |
| Verified read-back | 1.637 / 11.269 / 1.449 / 1.495 | 3.963 | 1.566 |

Mean recovery was 15.03% shorter; three of four pairs improved. The slow read-back
outlier is retained, not discarded. All eight reconstructed correctly, with no
upload quota backoff. Each read-back run confirmed four objects and sent one
create batch instead of two. This measures recovery after an injected lost
response, **not a guaranteed 15% improvement for ordinary whole-file uploads**.
Report/index: `data/recovery-latency-20261022.*`. Tests began October 22 and cleanup
continued October 23 (Asia/Taipei).

## Cleanup and deployment status

Test cleanup uses only exact benchmark IDs/names from isolated indexes, never
the live dataset index or a whole shared folder. Ambiguous/incomplete experiments
require remote parent, payload hash, app-properties, name, MIME/zero-content
verification before individual deletion. A read-only existence prefilter can
skip previously deleted/uncreated IDs; present objects still get full checks.
User manifests and files are preserved. Cleanup progress is journaled in each
isolated index instead of inventing upload completion.

All four 128-chunk controlled datasets (512 objects), all eight single-batch
datasets (32 objects), and the two-object gzip capability probe were removed.
Two uniquely identified random benchmark source files were removed from the
temporary directory after size/hash checks (64 MiB total; regenerable test data,
not user files). Larger-experiment cleanup/deployment results will be recorded
when finished. Report artifacts remain for audit.

Deployment: the idle UI was restarted on port 8770 with verified read-back enabled
by default. Home and status endpoints returned 200; the user's XAPK index still
reported 38,689/38,689 completed chunks and its original SHA-256. Workers/batch
remain 64x8, socket timeout remains 60s, request pacing and request gzip remain
disabled. Final regression checks: 346 Python tests, two front-end tests, and
`compileall` passed. No whole-file speedup or 95% quota-utilization guarantee is
claimed.

Large-experiment cleanup is resumable via the explicit ten-dataset allowlist in
`scripts/cleanup_followup_20261022.py`. It validates parent, identity and full
payload hash before deleting each present object, with at most four independent
clients and four-item batches. Only confirmed deleted/absent IDs are journaled;
incomplete cleanup must not be described as complete. This script never targets
the live dataset index. Its small-batch cleanup shares the central identity
validator with individual cleanup to keep deletion checks consistent.
