# 2026-10-22: bounded 64-worker experiment

## Method

Keep metadata-only storage, verified chunk payloads, stable IDs, TLS certificate
validation, bounded EOF/backoff recovery, and the 10 GiB acceptance limit.
Only the supported upload-worker ceiling changes from 32 to 64; do not assume
this is a recommended default. Download workers remain capped at 16.

Use isolated SQLite indexes and fresh filenames; never resume an existing user
dataset for timing. Each comparison uses exactly the same random input bytes,
9,599,872 bytes, description/Base64URL and 25,000-character chunks. Compression
overhead produces 513 chunks. Startup authentication/folder checks, download
verification and test-object cleanup are outside the upload timing window.

Standalone baseline command:

```powershell
python scripts/benchmark.py --profiles 16x8 --chunks 512 --shared-auth --download-workers 16 --database data/throughput-baseline-20261022.db --report data/throughput-baseline-20261022.jsonl --cleanup --cleanup-workers 2
```

Measured upload: 33.070 seconds, 0.2768 MiB/s; SQLite scopes: 0.826 seconds.
64 outer batch calls had median 6.452s, p95 9.594s, maximum 9.998s.
One singleton create handles the final chunk. Zero upload retries or observed
quota backoff. All 513 objects restored correctly, then were deleted.

Comparison command (16/64, then reverse order):

```powershell
python scripts/benchmark.py --profiles 16x8 64x8 --chunks 512 --shared-auth --repeats 2 --download-workers 16 --database data/scaling64-20261022.db --report data/scaling64-20261022.jsonl --cleanup --cleanup-at-end --cleanup-workers 4
```

Cleanup is deferred so deletes do not overlap measured uploads. On recognized
quota backoff, the harness does not start the next profile or immediate cleanup;
retain its isolated index/report for recovery. Percent utilization from the
Cloud Console is not measured here; neither a low project chart nor absent 429s
proves the absence of backend/per-user limits.

## Offline safeguards

Config tests reject 0/65 and accept 48/64 upload workers. Parallel tests create
enough distinct batches to occupy every tested worker, assert thread-confined
clients and commit other in-flight successes after a failed batch, then resume
without recreating completed chunks. Persistent-pool tests verify tiny jobs use
only the required client even with 48/64 configured workers.

No full 10 GiB remote transfer or native Drive throughput is claimed.

## Results and selection

| Concurrent batches x batch size | Round 1 seconds | Round 2 seconds | Mean seconds |
|---|---:|---:|---:|
| 16 x 8 | 65.237 | 50.139 | 57.6880 |
| 64 x 8 | 19.317 | 19.606 | 19.4615 |

Order: 16/64/64/16. All four uploads restored exactly. Zero upload retries,
recognized 403-rate-limit/429 events or TLS EOFs were logged; this is not a
measurement of Cloud Console utilization. Mean upload duration fell 66.3%,
about 2.96x throughput by the ratio of mean durations. The earlier standalone
16-worker result was 33.070s, demonstrating substantial temporal variability;
relative to that separate, unpaired observation the 64-worker mean is only
41.2% shorter. Do not extrapolate the 66.3% gain as a sustained GB-scale guarantee.

64-worker batch median latency was 11.156/11.461s, not individually faster
than 16 workers' 7.807/8.606s. Overlapping more of these delayed operations
reduced total wall time. 16-worker p95s were 38.451/24.864s versus 11.990/13.245s
at 64 workers. SQLite scopes were 0.898/1.131s at 64 versus 1.268/1.855s at 16.
The observation supports greater overlap, not a proven root cause of Google's
variable server/network latency, nor a claim of quota saturation.

Selected for this machine: `.env` upload workers 64, batch size 8. Keep the
template/library conservative defaults, download workers 16 and all durability,
integrity and retry safeguards. Offline suite: **194 passed**. The additional
48/64 tests include both error-draining/resume and lazy tiny-file clients.

## Completion

All 513 standalone baseline objects and all 2,052 comparison objects were
subsequently deleted using their isolated verified manifests (2,565 total).
Cleanup had transient transport timeouts and bounded retries, outside every
upload measurement; all five cleanup operations completed successfully. No
user datasets or live-index records were deleted. Reports and isolated indexes
remain as audit artifacts. Only this turn's generated local pytest directories
were removed (56,156,541 bytes); these fixtures are reproducible by rerunning tests.

The UI was started on port 8770 and checked: Metadata Drive Lab homepage,
connected=true, uploadWorkers=64, uploadBatchSize=8, downloadWorkers=16,
maxUploadMb=10240. No user upload was automatically started.
