# 2026-10-22: remove redundant encoding, test higher concurrency

## Selected local work reduction

The real upload caller was redundantly encoding already completed chunks.
Before changes, `pytest tests/test_upload_work_reduction.py -k resume` failed:
an already complete four-chunk dataset produced eight encoded payloads on resume;
a two-record prefix in a fourteen-chunk dataset encoded all fourteen chunks in
the prevalidation pass before scheduling another pass.

`PreparedUpload.chunks` now supports exact start/stop chunk ranges on its seekable
compressed spool. Base64URL's three-byte/four-character and Base85's
four-byte/five-character group boundaries are preserved, including odd widths
and final partial groups. Empty ranges do not read. Truncated spools raise an
integrity error. No metadata format, compression algorithm or chunk partition
changes, so existing datasets stay compatible.

The uploader still checks every reserved hash before any new remote create.
Prevalidation stops after its last existing record, instead of scanning a tail
that has no hashes to check. Complete resumes return after that validation pass.
Scheduling skips completely completed windows and seeks past completed window
prefixes; already validated complete records are not hashed again. Mixed windows
can still encode completed chunks between pending islands, bounded by the window.
Durable ID reservation, completion commit ordering, full create-response payload
checks, ambiguous-write confirmation and bounded TLS/backoff protections remain.

This does not remove the initial compression/original SHA-256 pass. It does not
pipeline browser reception/compression/Drive writes or add an uncompressed format.

## Offline A/B

```powershell
python scripts/benchmark_chunk_ranges.py --report data/chunk-ranges-20261022.jsonl
```

64 MiB deterministic random bytes held in a seekable memory stream, width 25,000,
Base64URL; four reversed-order pairs. The script includes the exact former
encoding routine as its control. Every pair has matching payload count and digest.
Compression, SQLite, browser staging, network and reserved-hash prevalidation
are outside this microbenchmark. Reported values are **encoding-only**, not
end-to-end resume or upload speed.

| Requested encoded range | Former mean seconds | Seeked mean seconds |
|---|---:|---:|
| Whole input | 0.329796 | 0.332160 |
| Last 10% | 0.268663 | 0.035611 |

Whole-input difference is about 0.7%, not evidence of a new-file speedup. Skipping
the completed prefix reduces tail-encoding duration about 86.7% (7.54x ratio).
The real caller tests separately prove that prevalidation is still performed.
An attempted 192 KiB encoding block was slower for full input and was not kept;
the selected implementation retains the previous width-adapted read size.

## Concurrency protocol

Upload validation supports 1..128 (96/128 tested offline); downloads stay 1..16.
The machine's existing 64 x 8 profile remains until live comparisons justify
changing it. Template/library defaults are not raised.

```powershell
python scripts/benchmark.py --profiles 64x8 128x8 --chunks 1024 --shared-auth --repeats 2 --download-workers 16 --database data/scaling128-20261022.db --report data/scaling128-20261022.jsonl --cleanup --cleanup-at-end --cleanup-workers 8
```

Same 19,199,872 random source bytes per profile, 1,025 actual chunks due to codec
overhead; order 64/128/128/64. Independent cold worker HTTP clients with cached
initial token. Authentication/folder startup, full download verification and
deferred cleanup excluded from upload duration. Isolated indexes/fresh filenames
prevent resume skips or modifications to user data. Quota backoff stops next
profiles and immediate cleanup. This is a doubled-size short comparison, **not**
the earlier proposed 64 MiB sustained-load test or a full GiB upload.

## Live outcome: reject 128 workers

| Profile | Upload seconds | Upload retry events | Verification |
|---|---:|---:|---|
| 64 x 8, round 1 | 26.223 | 0 | Restored exactly |
| 128 x 8, round 1 | 94.979 | 1 transport timeout | Restored exactly |
| 128 x 8, round 2 | 106.878 | 112 transport timeouts + 4 HTTP 403 rate-limit backoffs | Restored exactly |
| 64 x 8, round 2 | Not started | — | — |

The SDK identified `userRateLimitExceeded`, not a generic permission error.
The second 128-worker run had 240 outer batch calls: 128 successful HTTP 200
responses and 112 timeout outcomes. Its batch p95 was 66.166s versus 12.685s
in the one completed 64-worker control. These are application retry/HTTP events,
not Console unit counts, and do not identify the precise quota dimension.

The harness exited via `stopped_after_quota` after the third run and did not
start the fourth upload or immediate cleanup. By the time an additional targeted
process-stop check ran, the benchmark had already exited on its own; no unrelated
process was stopped. All three datasets were fully restored, so their isolated
verified reports can safely drive subsequent low-concurrency targeted cleanup.

There is no valid completed four-run comparison or sustained-load conclusion.
Both 128-worker observations were substantially slower than the 64-worker
control; **keep production at 64 x 8**, not 128. Do not infer spare usable
throughput solely from a low project-per-minute quota percentage.

The benchmark was then hardened to check quota after upload **before** adding a
verification download. A caller-level offline test proves the download is not
started on that path. This check does not cancel already in-flight upload calls;
production retry limits/backoff and success-draining remain unchanged.

## Sparse-index work reduction

Resume prevalidation formerly called `iter_chunks` across every possible index
window up to the manifest count, even when only a few rows were reserved. The
iterator now uses indexed keyset pagination (`ORDER BY chunk_index LIMIT window`)
over actual rows, with the connection closed before yielding to consumers.
Memory stays bounded; sparse rows remain visible, and the downloader retains
its independent missing/out-of-order chunk checks. The red test caught a fourth
unnecessary query; the optimized two-row, million-position test uses three
queries when page size is one (two records plus end-of-iteration).

`python scripts/benchmark_sparse_index.py --report data/sparse-index-20261022.jsonl`
measured four reversed-order pairs on a temporary SQLite index in the configured
E: staging area: 100,000 declared chunk positions, two actual records, page size
128. Old range paging used 782 connections, mean 0.262163s; new keyset paging
used two connections, mean 0.001252s. All row sequences matched. The temporary
database was removed automatically. This is an index-only synthetic sparse
case, not a 200x end-to-end upload claim; dense/full indexes have much less to gain.

Final offline verification: **292 passed**, including legacy formats, odd-width
range encoding, empty/truncated ranges, conflict-before-new-write checks,
ambiguous creates, missing/corrupt download chunks, thread confinement and lazy
tiny jobs at 96/128 workers. The production setting remains 64 x 8, 16 download
workers, 10 GiB file ceiling, unchanged payload capacity and verification strength.

## Completion and scope

After the load test exited, all three verified test datasets were removed using
`benchmark.py --cleanup-report-only` with four independent cleanup clients and
16-item delete batches. All **3,075 newly created objects** were deleted; no live
index records, user files, folders or previous experiment artifacts were removed.
Cleanup reports for all three dataset IDs completed successfully. Reports and
isolated indexes remain for audit. The nine generated pytest directories were
validated under the dedicated E: test root, checked for reparse points and removed
(100,434,527 generated bytes); rerunning tests regenerates these fixtures.

The idle project UI on 8770 was restarted, verified as Metadata Drive Lab with
Drive connected, 64 x 8 and maxUploadMb=10240. No user upload was started. The
selected gains concern resume preparation/local work; this turn establishes no
additional fresh-file network-throughput improvement beyond the prior 64-worker
selection. Full browser/compression/upload pipelining and raw/uncompressed
storage remain unimplemented, not claimed as complete.
