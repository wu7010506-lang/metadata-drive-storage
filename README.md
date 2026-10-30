# metadata-drive-storage

`metadata-drive-storage` is a research prototype for measuring whether small,
encoded payloads can be stored reliably in legal Google Drive API v3 metadata
fields. It is **not** a production storage system, an “unlimited Drive” product,
or a quota/rate-limit bypass.

## Quickstart

### Option A: 30-Second Offline Verification (No Google Cloud Credentials Required)

You can verify the full compression/chunking pipeline, SQLite manifest integrity, fault-injection suites (**346 pytest checks**), and bounded-memory 2 GiB streaming locally without any Google Drive account:

**macOS / Linux (Bash / Zsh):**
```bash
git clone https://github.com/wu7010506-lang/metadata-drive-storage.git
cd metadata-drive-storage
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Run all 346 offline unit & fault-injection tests
pytest -q

# Run offline 2 GiB streaming compression + SHA-256 restoration benchmark
python scripts/benchmark_streaming.py --size-mib 2048
```

**Windows (PowerShell):**
```powershell
git clone https://github.com/wu7010506-lang/metadata-drive-storage.git
cd metadata-drive-storage
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# Run all 346 offline unit & fault-injection tests
pytest -q

# Run offline 2 GiB streaming compression + SHA-256 restoration benchmark
python scripts/benchmark_streaming.py --size-mib 2048
```

### Option B: End-to-End Google Drive Usage (Web UI & CLI in 4 Steps)

1. **Create a Service Account & Shared Drive Folder**:
   - In [Google Cloud Console](https://console.cloud.google.com/), enable the **Google Drive API** and create a Service Account JSON key (save it outside the repo or at `credentials/service-account.json`).
   - Share a target Google Drive folder (preferably in a Shared Drive) with the Service Account email (`Can add files` / Editor) and copy the folder ID from its URL.
2. **Configure `.env`** (`cp .env.example .env` on macOS/Linux or `Copy-Item .env.example .env` on Windows):
   ```dotenv
   GOOGLE_APPLICATION_CREDENTIALS=credentials/service-account.json
   GOOGLE_DRIVE_FOLDER_ID=your_shared_drive_folder_id

   # Empirically measured 64x8 profile (-66.3% upload time on 513 chunks; see docs/performance-20261022.md)
   UPLOAD_BACKEND=description
   DESCRIPTION_SAFE_PAYLOAD_BYTES=25000
   STORAGE_ENCODING=base64url
   UPLOAD_WORKERS=64
   UPLOAD_BATCH_SIZE=8
   DOWNLOAD_WORKERS=16
   ```
3. **Launch the Local Web UI** (drag-and-drop upload up to 10 GiB + 1-click SHA-256 verified restore):
   ```bash
   python scripts/run_ui.py
   # Open http://127.0.0.1:8765 in your browser
   ```
4. **Or Use the CLI Directly**:
   ```bash
   # Probe exact metadata byte limits for your environment
   python scripts/probe_metadata.py --backend description --max-bytes 32768

   # Compress, chunk into 0-byte Drive objects, and record SQLite manifest
   python scripts/upload_file.py path/to/input.bin

   # Reassemble chunks and atomically write output after full SHA-256 verification
   python scripts/download_file.py <DATASET_ID> path/to/restored.bin
   ```

## Current milestone

Milestone 1 implements:

- Service Account authentication from environment configuration
- target-folder access and write-capability verification
- independent `appProperties`, `properties`, `description`, and `name` backends
- one-object exponential growth and binary-search capacity probing
- exact write/read comparison, latency and API-request reporting
- bounded truncated exponential backoff with jitter
- TLS EOF recovery: discard the failed worker's cached connections and retry
  within the configured attempt limit, without disabling certificate checks.
  Stable precommitted chunk IDs and metadata verification prevent duplicate
  objects when Drive accepted a write but its response was lost. Certificate
  validation errors and disk-full errors are not made retryable.
- cleanup by Drive file ID in a `finally` path

The requested upload extension also implements zstandard compression (with gzip
fallback), unpadded Base64URL encoding, automatic chunking, SHA-256 hashes, a
SQLite manifest/index, and resumable creation of zero-content Drive files whose
names or descriptions carry the payload. Verified download/restoration is now
implemented; short end-to-end comparisons are recorded in `docs/upload-performance.md`.

## Architecture

```text
scripts/probe_metadata.py
        | configuration + authentication
        v
MetadataBackend (one selected field)
        | create one empty Drive file
        | update -> get -> exact compare
        | exponential growth -> binary search
        v
ProbeResult (capacity, safety margin, latency, requests, errors)
        |
        `-> delete temporary file
```

The implemented data path is `bytes -> compression -> Base64URL -> chunks -> Drive
metadata`; restoration reverses it and accepts output only after chunk and full
SHA-256 verification. SQLite remains the canonical manifest and Drive-ID
index so normal reads never scan an entire Drive.

## Important API limitations

Google officially limits each custom `properties` or `appProperties` key plus
value to **124 UTF-8 bytes**, with at most 30 public properties per file, 30
private properties per application/file, and 100 custom properties total. The
backend legally distributes the test payload over 30 keys named `p00` through
`p29`; each three-byte key leaves 121 bytes for its value, producing a documented
raw-payload ceiling of 3,630 bytes on a clean temporary object. The probe respects
that ceiling and does not deliberately send larger values. Real chunks need room
for their identifiers and hashes, so this raw number is not a usable chunk size.
See Google's [custom-property limits](https://developers.google.com/workspace/drive/api/guides/properties#limits_of_custom_file_properties).

The Drive v3 File reference gives no numeric maximum for `name` or `description`.
That absence is **not** a promise of unlimited capacity. Results for these fields
are observational, specific to the account/API behavior at test time, and capped
by `PROBE_MAX_BYTES`. A “probe cap reached” result is not a discovered API maximum.

The probe creates a real Drive file without uploaded media by using
[`files.create`](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/create).
It pre-generates the file ID to reduce duplicate-object risk when a create result
is indeterminate. Every recorded temporary object is permanently deleted after
the run.

## Google Drive and Service Account setup

1. Create or select a Google Cloud project.
2. Enable **Google Drive API**.
3. Create a Service Account and a JSON key.
4. Prefer a **Shared Drive** on a supported Google Workspace edition. Current
   Google documentation says Service Accounts have no Drive storage quota and
   cannot own files, although another current guide is inconsistent on ownership.
   This prototype therefore does not rely on a Service Account's My Drive.
5. Create/select a test folder in the Shared Drive and add the Service Account
   email with permission to add files. If sharing a folder directly, disable email
   notification because a Service Account has no inbox.
6. Copy the folder ID from its Drive URL.

Shared-drive operations set `supportsAllDrives=true`, as required by Google's
[shared-drive guidance](https://developers.google.com/workspace/drive/api/guides/enable-shareddrives).
Workspace domain-wide delegation is an alternative but is outside this milestone.

## Environment setup

Python 3.11 or newer is required.

```powershell
cd metadata-drive-storage
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env`:

```dotenv
GOOGLE_APPLICATION_CREDENTIALS=C:/absolute/path/to/service-account.json
GOOGLE_DRIVE_FOLDER_ID=your_shared_drive_folder_id
```

Never commit `.env`, Service Account JSON, private keys, access tokens, or any
credential. `.gitignore` excludes `.env`, all JSON files, `credentials/`, virtual
environments, caches, and SQLite databases. Keeping keys outside this repository
is safer than relying on ignore rules alone.

## Run the probe

```powershell
python scripts/probe_metadata.py --backend appProperties
python scripts/probe_metadata.py --backend description --max-bytes 32768
```

Available backends are `appProperties`, `properties`, `description`, and `name`.
The default safety margin is 15%; configure it through `.env` or
`--safe-margin-percent`. Requests are bounded by `GOOGLE_API_MAX_ATTEMPTS`.
Only rate-limit-related 403 responses, HTTP 429, and transient 5xx responses are
retried, following Google's [error guidance](https://developers.google.com/workspace/drive/api/guides/handle-errors)
and [exponential-backoff guidance](https://developers.google.com/workspace/drive/api/guides/limits#exponential).

The CLI reports every attempted byte size, maximum confirmed payload, recommended
safe payload, write/read latency, API request count, and errors. It exits nonzero
for authentication, permission, unexpected API, or cleanup failures.

## Upload a file using empty Drive objects

First probe the `name` backend and use its **recommended safe payload** as the
chunk size. Do not copy the configured probe ceiling when the result says “probe
cap reached”; increase the conservative probe ceiling and measure again.

```powershell
python scripts/probe_metadata.py --backend name --max-bytes 4096
python scripts/upload_file.py path/to/input.bin
```

Copy the probe's recommended value into `NAME_SAFE_PAYLOAD_BYTES` in `.env`.
`--chunk-size` remains available as an explicit per-run override. In the tested
`Metadata-Test` folder on 2026-09-10, the largest successful ASCII name payload
was 10,000 bytes and the configured 15% safety margin produced 8,500 bytes. This
is an empirical result, not a Google API guarantee; re-probe for other accounts
or environments.

The uploader reads the file, compresses before encoding, converts it to unpadded
Base64URL, and splits it automatically. Each Drive object has zero file-content
bytes; its `name` is one payload chunk. `dataset_id`, `chunk_index`, `chunk_hash`,
and `version` are stored in `appProperties`, while the complete manifest and
Drive IDs are stored in the local SQLite database configured by `SQLITE_PATH`.

Generated Drive IDs are written to SQLite as `pending` before remote creation.
After a successful create/read-back comparison, the chunk becomes `complete`.
Running the same command again finds the matching manifest and skips completed
chunks rather than recreating them.

## Download and verify

The downloader reads the manifest and ordered Drive IDs from SQLite, validates
every chunk's index, app properties, and SHA-256, joins and decodes Base64URL,
decompresses the payload, then checks restored size and full-file SHA-256. It
does not expose or write output unless all checks pass.

Use the **驗證並下載** button beside any completed dataset in the local UI, or:

```powershell
python scripts/download_file.py DATASET_ID path/to/restored-file.bin
```

CLI output is written atomically through a temporary file in the destination
directory, so a failed verification cannot leave a plausible partial result.

## Local upload interface

The browser interface runs only on this computer and keeps the Service Account
key in the Python backend. Start it with:

```powershell
python scripts/run_ui.py
```

Then open `http://127.0.0.1:8765`. Drag a file onto the upload area or choose it
from the file picker. The page reports Drive connection state, chunk progress,
compression statistics, SHA-256, dataset ID, and recent SQLite-indexed datasets.
The server refuses non-loopback bind addresses. The default maximum input file
is 10240 MiB (10 GiB / 10,737,418,240 bytes), inclusive. `UI_MAX_UPLOAD_MB` can lower
this limit but cannot exceed 10240. The CLI applies the same actual-file limit;
multipart headers do not count toward it. Changes require a server restart.
This raises the acceptance limit, not available disk space: an incompressible
10 GiB UI upload can need about 20 GiB for the staged input plus compressed spool,
with additional room for SQLite, downloads and filesystem overhead. Keep a margin
above that on the configured temporary drive. No whole 10 GiB buffer is allocated.

## Tests and basic checks

### Upload throughput

With `UPLOAD_BATCH_SIZE=1`, `UPLOAD_WORKERS=16` enables sixteen concurrent chunk requests in both the UI and
CLI. Each worker owns an independent Drive client. IDs are generated in batches
of 100..1000, adapted to remaining chunks, and saved locally in reservation-window transactions before creation; successful create responses are checked
against the payload and metadata. Conflicting IDs are independently read back.
Completed chunks remain resumable, and changing the configured chunk size does
not repartition an existing matching dataset. `GOOGLE_API_MIN_INTERVAL_SECONDS`
applies across workers; transient errors retain exponential backoff. The local
configuration uses 25,000-byte description chunks and no fixed request delay.
Actual throughput depends on API latency and rate limiting.

`UPLOAD_WORKERS` supports 1..128; higher counts are experimental until measured on this
connection. The download limit stays 16. Raising upload concurrency does not
increase the Google account/project quota, and more workers can be slower.

After the 2026-10-08 comparison, this computer selected 8 workers x 16
metadata requests per batch, plus 16 download workers. The paired 128-chunk
uploads took 14.025/13.483 seconds vs 19.757/19.252 seconds for 16 individual
workers (about 29% less time). On the same dataset, sequential reconstruction
took 51.542 seconds and 16 readers took 11.252 seconds. These short measurements
are not sustained large-file guarantees. See `docs/upload-performance.md`.

The 2026-10-11 scheduling comparison selected **16 workers x 8 requests per
batch** instead: two 128-chunk uploads averaged 12.197 seconds versus 13.5665
for 8 x 16 (about 10.1% less time). At that point this computer selected that profile,
description + Base64URL, and 16 download workers. The startup factory also caches
the initial authentication token for independent worker clients. These changes
apply after a server restart, not to an already-running process.

A second paired comparison at 16 x 8 measured cold-worker uploads averaging
9.023 seconds with cached initial authentication versus 11.2535 with independent
token retrieval (19.8% less time). All runs fully restored with zero upload
retries. This gain does not apply again to already-warm persistent clients, and
it is not evidence of matching native Drive uploads.

The 2026-10-18 follow-up did **not** justify another default change. With the
same approximately 9 MiB source, 24/32 workers at batch size 8 were not reliably
faster. In a separate reversed-order comparison, 16 x 8 averaged 25.3625 seconds
versus 29.216 for 32 x 4. That comparison retained **16 x 8** rather than
selecting a configuration from its single fastest sample. See the full report
for retry counts, variability and cleanup scope.

The 2026-10-22 reversed-order comparison selected **64 x 8** on this connection:
two equal 9,599,872-byte uploads averaged 19.4615s versus 57.688s for 16 x 8
(about 66.3% less time). All four restored exactly, with zero upload retries or
observed quota backoff. This machine's `.env` now uses 64 upload workers, batches
of 8, and the same 16 download workers. The library/template conservative default
is not changed. Tiny jobs still initialize only needed clients. These are short,
cold-client measurements, not proven sustained GB-file throughput or native Drive
parity. See [the experiment report](docs/performance-20261022.md).

The subsequent [128-worker and redundant-work follow-up](docs/performance-extreme-20261022.md)
keeps **64 x 8**: 128 workers were slower and observed rate-limit/timeout backoff.
Resumes now stop hash prevalidation after the last reserved chunk, avoid a second
encoding pass for complete datasets, and seek over completed scheduling windows.
Existing reserved hashes are still checked before any new writes, and existing
chunk partitions/formats stay unchanged. The encoding-only microbenchmark is not
an end-to-end upload speed claim; initial whole-file compression is still required.

The [response/capacity/granularity follow-up](docs/performance-wire-20261022.md)
also retains **64 x 8**. Both legacy and explicitly negotiated batch responses
were already gzip; explicit negotiation did not improve upload time and stays off.
A 32,768-character random description create was rejected, so payload capacity
stays 25,000. The first 64 x 4 sample did not materially improve on 64 x 8;
64 direct calls were slower and observed user-rate-limit backoff. No new-file
speedup is claimed from those experiments.

The follow-up added a singleton fast path: one remaining chunk uses verified
direct creation instead of a one-item batch envelope. A four-pair 16 KiB/one-chunk
comparison averaged 1.5355 seconds versus 2.4600 (37.6% less time), including ID
generation and SQLite writes but excluding folder/auth startup and browser I/O.
This is not a large-file speed multiplier. Multi-item groups still batch, and
stable-ID/conflict confirmation, full integrity checks and resume are retained.

The [lost-response recovery follow-up](docs/performance-recovery-20261022.md)
confirms already-stored chunks before replaying an ambiguous create. Healthy
uploads add no reads. Four controlled pairs reduced mean recovery latency from
4.66s to 3.96s, with one slow-read outlier; this is not a whole-file speed promise.
Adaptive 95%-ceiling pacing regressed and remains disabled. Experimental request
gzip reduced body bytes but did not establish a stable speedup, so it stays off.

To revert to individual uploads, set both `UPLOAD_WORKERS=16` and
`UPLOAD_BATCH_SIZE=1`; use `DOWNLOAD_WORKERS=1` to revert downloads. Restart the
project server for environment changes to take effect. Generic defaults in
`.env.example` remain conservative rather than assuming this machine's results.

`UPLOAD_BATCH_SIZE=1` preserves individual requests. Experimental values 2..100
group metadata-only creates into Drive batch requests. In this mode
`UPLOAD_WORKERS` means concurrent **batches**, not concurrent individual chunks;
start with 2 or 4 workers, not 16 batches of 100. A larger batch does not reduce
quota consumption and is not necessarily faster. Each subresponse is checked
individually. Only transient/unconfirmed items are retried with bounded backoff;
successful items are not resent. Lost responses retain their original IDs, so a
resume confirms any existing object before marking it complete.

The coordinator alone commits completion records, batching available results
before reporting progress. An error stops new submissions but drains other
in-flight results, preserving verified successes for resume. SQLite's existing
journal and synchronization settings are retained; this change does not enable
WAL or weaken crash durability.

Run `python scripts/benchmark.py --workers 8 16 --chunks 128` to compare fresh
uploads of equal random payloads. It reports upload time, chunks per second,
retry count, and HTTP 429 retries, then verifies reconstruction separately.
The small benchmark datasets remain in Drive and the SQLite index.

For a worker/batch comparison with repeated runs and an isolated index:

```powershell
python scripts/benchmark.py --profiles 16x1 2x16 4x16 --chunks 128 --repeats 2 --database data/batch-benchmark.db --report data/batch-benchmark.jsonl --cleanup
python scripts/benchmark_sqlite.py --chunks 512 --repeats 3
```

Repeated runs reverse profile order. Reports include local transaction count/time
and a separately timed full reconstruction check. `--cleanup` permanently deletes
only the freshly created, verified benchmark objects; the benchmark index remains
as an audit record, not a usable dataset catalogue after cleanup. Failures retain
their index/objects for diagnosis. Neither script deletes existing user datasets.

For worker scaling, use enough chunks to occupy every worker (e.g. 512 at batch
size 8), and keep source bytes and batch size fixed. `quota_retries` counts logged
403 rate-limit/429 backoff events; the existing `http_429_retries` field remains
429-only. These counters are not Cloud Console quota-unit measurements, and one
batch backoff event can represent multiple failing subrequests.

`--cleanup --cleanup-at-end` defers deletion until all profiles finish.
`--cleanup-workers 1..8` bounds independent cleanup clients, always outside
upload timing. Cleanup uses four-item DELETE batches, stable IDs, bounded retry
and 404 idempotency; it refuses an incomplete manifest. An interrupted benchmark
must not be passed off as verified just to clean it.

`GOOGLE_API_HTTP_TIMEOUT_SECONDS` (5..120, default 60) explicitly configures
socket inactivity timeouts for the UI startup and transfer CLIs. It is not a
whole-job deadline; shorter values can cause more ambiguous-write confirmations
and retries. The 2026-10-18 60/20-second comparison did not establish a meaningful
speedup, so the local default remains 60. Existing worker clients must be
recreated by restarting the server before a changed value applies.

The benchmark accepts `--http-timeouts 60 20`. `upload_http` summarizes counts,
outer HTTP outcomes and p50/p95/max latency separately for batch and ordinary
requests. It excludes startup folder checks and download/cleanup. Totals sum
concurrent request durations and can exceed wall-clock time. Latency samples
are bounded to the first 10,000 per category; counts/totals/max cover all events.
Outer batch HTTP 200 does not mean every subresponse succeeded; the retry and
full-restoration checks remain necessary. URLs, headers and payloads are not
retained in these metrics.

Benchmark startup/transfer quota backoff now stops subsequent profiles, preserving
the report/index and avoiding immediate cleanup load. Rate-limit errors can be
inside outer HTTP 200 batches. The 2026-10-18 wider-batch experiment actually
observed HTTP 403 `userRateLimitExceeded`; no wider batch default was adopted.

For an interrupted benchmark, `scripts/cleanup_aborted_benchmark.py` is a separate
recovery tool. It requires the existing isolated index, exact dataset ID and exact
benchmark filename. It refuses the live index and verifies remote identity,
location, zero content and payload hash before deleting each known ID. It does
not mark pending chunks complete or turn a partial upload into a verified one.

`scripts/benchmark_layout.py` is a benchmark-only parent-layout experiment, not
a production layout setting. It compares the configured root against newly
created temporary child folders. Folder setup is reported separately from upload
timing. An isolated SQLite journal records folder IDs before creation; cleanup
checks their marker/parent and refuses any nonempty folder (including trashed
children). Interrupted runs retain their journal; `--cleanup-only` can resume
cleanup of verified test datasets and empty, owned test folders. It cannot turn
an incomplete upload into a verified one. Existing user objects are never moved.

```powershell
python scripts/benchmark.py --profiles 16x8 24x8 32x8 --chunks 512 --shared-auth --repeats 2 --download-workers 16 --database data/scaling.db --report data/scaling.jsonl --cleanup --cleanup-at-end --cleanup-workers 8
```

On 2026-10-06, an equal-payload 128-chunk comparison measured 28.545 seconds
with 8 workers and 20.690 seconds with 16 workers (1.38x throughput). Both runs
had zero retries and passed full reconstruction verification. This single
comparison measures this connection and payload, not a guaranteed speedup.

```powershell
python -m pytest
python -m compileall -q src scripts tests
python scripts/probe_metadata.py --help
```

Unit tests do not access Google Drive. A live probe requires user-supplied
credentials and the explicitly shared test folder.

## Security and platform policy

This project does not rotate accounts, create accounts, evade quota, bypass rate
limits, call private APIs, or attempt to exceed documented limits. Stop and record
any explicit Drive API rejection. A measured value is evidence about one legal API
configuration—not permission to abuse it and not a storage SLA.

## Parallel reconstruction

`DOWNLOAD_WORKERS=1` retains sequential reads; values 2..16 enable independent
reader clients in both the UI and download CLI. At most twice the worker count
is queued; payloads are verified in the workers and consumed in manifest order.
Request pacing remains shared across readers. The output is
still released only after full-file size and SHA-256 verification.

File reconstruction now incrementally decodes Base64 and decompresses directly
into a private output file in at most 1 MiB writes. The file is atomically renamed
only after size and SHA-256 validation. It does not collect all metadata payloads
or the restored file in memory. `download_bytes()` remains an explicitly
in-memory convenience API; the UI and file-download CLI use `download_file()`.

Use `--download-workers 1 16` with the benchmark to measure both reader counts
on the same freshly uploaded dataset. Download timing is separate from upload.

## Streaming and connection reuse (2026-10-11)

Uploads hash and compress in 1 MiB source reads. Up to 8 MiB of compressed data
stays in an in-memory spool; larger results spill to `UPLOAD_TEMP_DIR`. Base64
encoding and SQLite index reads use bounded windows. Compression is prepared
before remote writes, not overlapped with them: the manifest needs its hash,
compressed size and chunk count first. Legacy zstd/gzip and version-1 partitions
remain compatible; all reserved hashes are checked before a resumed upload can
write new chunks. A codec change cannot silently mix new bytes with reserved
partitions: hash conflicts fail safely; a different partition count uses a new
dataset rather than modifying the old one.

The UI multipart parser writes directly into the upload job's staged source,
avoiding a second full-file copy. Set `UPLOAD_TEMP_DIR=E:/metadata-drive-temp`
on this computer. Small compressed results avoid an extra disk spool. Worst-case
incompressible 2 GiB input still needs approximately 4 GiB of upload staging/spool
disk space, plus headroom, on the configured volume. Streaming bounds RAM; it
does not eliminate local disk requirements. Failed/completed upload jobs close
their spool and remove their own staged source. Abrupt process termination may
leave staged source files; automatic stale-file deletion is not implemented.

The UI keeps a persistent network worker pool and thread-owned Drive clients.
Upload and download share at most `max(UPLOAD_WORKERS, DOWNLOAD_WORKERS)` network
threads and the same request pacing state. Each upload still bounds active groups
to `UPLOAD_WORKERS`; downloads queue at most twice `DOWNLOAD_WORKERS`. Unused
preallocated IDs are reused between uploads. Credentials/connections are created
lazily and closed on normal server shutdown. The startup factory loads the key
once and supplies independent credential copies with an initially cached token
to each client. HTTP connections and mutable credential objects are never shared
across threads. Existing long-lived clients refresh their own tokens after expiry.
Folder access is checked once at startup; actual transfer responses still enforce
permissions and validate metadata. A cold first transfer can still take longer.

Downloaded job files remain in `DOWNLOAD_OUTPUT_DIR` for local use and HTTP Range
re-requests; they are not deleted when an HTTP response closes. The UI displays
the saved path. Browser-side Range support does not resume a failed Drive
reconstruction; remote-download checkpointing is still future work.

Offline verification and a read-only connection check:

```powershell
python scripts/benchmark_streaming.py --size-mib 2048
python scripts/benchmark_streaming.py --size-mib 256 --random --spool-dir data/stream-benchmark-temp
python scripts/benchmark_connections.py --repeats 3
```

The 2 GiB benchmark uses virtual repeated data and a digest-only destination;
it does not allocate a 2 GiB source, write a 2 GiB output, or contact Drive.
The random benchmark uses a temporary compressed disk spool, then removes it.
Connection checks only read the configured folder; neither benchmark uploads or
deletes remote objects. Results and limitations are in `docs/upload-performance.md`.

### Experimental metadata packing

`UPLOAD_BACKEND=combined` partitions a logical chunk between the separately
measured name and description capacities. Both capacities must be positive.
`STORAGE_ENCODING=base85` uses ASCII Base85 instead of unpadded Base64URL. These
options reduce object count, but were **slower for upload** in the 2026-10-11
comparison; this computer retains description + Base64URL. They are explicitly
marked experimental in the UI. Neither increases an individual field's capacity.

New manifests record their encoding (version 2 for combined/Base85). Old indexes
migrate with Base64URL as the default, and existing datasets download according
to their own manifest rather than the current upload encoding. Changing encoding
or backend creates a different dataset; it does not resume a legacy upload using
different bytes. Keep its original selections to resume an unfinished upload.

To compare equal input sizes and authentication without touching the live index:

```powershell
python scripts/benchmark.py --profiles 8x16 16x8 --input-bytes 2399872 --chunks 128 --repeats 2 --download-workers 16 --database data/scheduling-test.db --report data/scheduling-test.jsonl --cleanup
python scripts/benchmark.py --profiles 16x8 --auth-profiles legacy shared --input-bytes 2399872 --chunks 128 --repeats 2 --download-workers 16 --database data/auth-test.db --report data/auth-test.jsonl --cleanup
```

The input size is fixed across profiles, authentication/profile order reverses
each round, and every measured upload must restore exactly before being reported.
The initial folder/authentication check is outside upload timing, like UI startup;
worker initialization remains inside it. This is not a native Drive baseline.

## Roadmap

The next milestone is lifecycle management and benchmarking: repair incomplete
chunks, remove a dataset and its Drive objects safely, rebuild a lost local index
from `appProperties`, and measure throughput/error rate at larger file sizes.
