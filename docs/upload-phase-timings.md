# Upload phase accounting (2026-10-22)

The local UI now reports wall-clock phases under `timings` on
`GET /api/uploads/<job_id>`, displays them during polling and after success or
failure, and emits one numeric summary in the server log per terminal job.
This is instrumentation, not a demonstrated upload throughput improvement.
No extra Drive requests, per-chunk timing callbacks, payload logs, or changes
to compression, chunk boundaries, integrity checks or durable reservation are
introduced.

| Phase | Included work |
|---|---|
| receiving | From Flask request dispatch, through multipart parsing, file staging, form validation and stage flush/close. |
| queued | From job acceptance until its upload coordinator begins. Other uploads can cause this wait. |
| setup | Obtaining the coordinator's client and constructing the uploader. This can include credentials/connection setup. |
| preparing | Full-file read, SHA-256 and compression into a bounded-memory/disk spool. |
| indexing | Dataset lookup/save and any existing reserved-chunk hash validation for resume. |
| transferring | Windowed encoding, ID allocation, pending/completion commits, worker client setup, HTTP batches, response validation, bounded retries and in-flight draining. |

These phases do not overlap. Poll snapshots include the current phase's elapsed
time without resetting it. Success/error freezes the duration before deleting
the staged source file. A fully completed resume has no transferring phase.
Values are seconds, not percent of quota or summed durations across threads.

The timer does **not** measure browser file selection, browser work before server
dispatch, post-completion HTTP polling latency, or final staged-file cleanup.
Reception metrics become visible only once the multipart request finishes and
a job ID exists. Requests rejected before job creation (e.g. oversized files)
do not have a job timing report. In-memory metrics, like UI job state, do not
survive a server restart. Logs can be retained separately.

`transferring` is not pure API processing time: CPU/disk work and network wait
overlap within this phase. Do not add concurrent request latency totals to these
phase durations or subtract SQLite scope totals to claim Google backend time.
The client renders missing fields safely for compatibility with an older server.
`0.00` can mean either not reached or shorter than the display precision.

## Deterministic checks

`python -m pytest tests/test_upload_timings.py -q` uses the real Flask multipart
caller, real compression/index/uploader and a fake Drive. An injected monotonic
clock attributes receiving=2, preparing=5, indexing=3, transferring=18 seconds
for a 28-second synthetic timeline. Preparation/index/transfer failures must
freeze at the correct boundary without adding any read-back requests. Separate
checks cover a blocked first upload keeping the second job in the queue, live
snapshots, and a completed resume skipping transfer.

`node --test tests/upload_timing_view.test.cjs` exercises the display controller
with a minimal DOM double. It does not replace a real-browser visual check.

## Next decision

After restarting the server and refreshing the UI, compare normal uploads of a
small file and a representative larger, poorly compressible file (not a full
10 GiB stress test). Keep input size, backend, encoding, 64 workers and batch 8
fixed and note phase times plus retries. A large preparing/receiving share
supports exploring bounded pipeline overlap; a dominant transferring share
requires investigating request throughput/latency rather than deleting hash
checks. No new cloud benchmark was run for this instrumentation change.
