# Experimental request pacing (2026-10-22)

## Outcome and scope

The upload monitor was paused at the user's request. No production server was
restarted, no user transfer was modified or cancelled, and no additional cloud
benchmark was run while the user's XAPK upload was active.

An opt-in shared request pacer and matched A/B profiles are implemented and
tested offline. The live configuration remains 64 workers x batch 8 with
`UPLOAD_TARGET_REQUESTS_PER_MINUTE` unset (disabled). **There is no demonstrated
new cloud speedup or achievement of 95% utilization.** Enabling a ceiling is
not evidence that the network/backend can sustain that request rate.

## Why the Console graph is not a throughput target by itself

The screenshot shows **Previous quota: Requests per minute**, limit 12,000,
current 5.15%, peak 29.54%. A hypothetical 95% of that legacy metric is 11,400
requests/minute, or 190/second. This is not an assertion that all those requests
can be sustained writes, nor that this legacy quota is the only applicable one.

[Drive usage limits](https://developers.google.com/workspace/drive/api/guides/limits)
distinguishes project/user quotas, newer quota units and grandfathered older
quotas, and warns about additional backend rate checks. A service account
counts as one account. The public method-cost table does not explicitly list
`files.create`, so this change does not invent its new-unit cost or convert a
request ceiling into quota units.

[Google's large-migration guidance](https://knowledge.workspace.google.com/admin/getting-started/google-drive-large-migration-best-practices)
also advises staying below 3 sustained writes/inserts per second per account
and avoiding very large single-owner/folder structures. That is important
migration guidance, **not a measurement of this account's instantaneous limit**;
the observed short bursts here can be much faster. It reinforces that project
request quota alone cannot establish sustained file-creation capacity.

[Batch request documentation](https://developers.google.com/workspace/drive/api/guides/performance)
counts each inner request against quota. Counting a batch of 8 as one would
overestimate headroom eightfold. The controller does not rotate identities,
change account permissions or quotas, disable TLS, or fill the graph with dummy
requests. Existing idempotent IDs, response/hash checks and bounded backoff stay.

The existing httplib2 timeout path was inspected before changing transport
recovery: its `Http.request` already removes/closes a timed-out connection.
Therefore no redundant timeout-reset fix or claim that stale connections caused
the observed timeout wave was introduced.

## Pacer behavior

`UploadRequestPacer` in `src/upload_pacing.py` owns a single thread-safe budget.
The UI transfer pool shares it across upload workers/jobs; standalone parallel
uploads inject the coordinator's same pacer into all child uploaders.

- 0/unset: no pacer. Existing measured behavior and defaults are unchanged.
- Positive `UPLOAD_TARGET_REQUESTS_PER_MINUTE`: adaptive **ceiling**, not a minimum.
- Start at at most 32 inner requests/s or the lower configured target.
- After each 16 healthy request rounds, grow the current rate by 10% (at least
  one request/s), bounded by the target. A rate is not measured throughput.
- Recognized transient HTTP/transport errors halve the rate, coalesced to at
  most one reduction per second. Resume growth after 5 seconds without congestion.
- No saved tokens after idle. Admission spacing uses actual inner batch size;
  a retry of one unconfirmed item costs 1, not the old batch size or zero.
- ID allocation and ambiguous-write confirmation requests each cost 1 as well.
  Failed final attempts also provide feedback; attempts are still bounded.
- Existing positive `GOOGLE_API_MIN_INTERVAL_SECONDS` remains another limit.
  Download/other application traffic is outside this upload pacer. SDK-internal
  authentication replays are not separately observed. `admittedRequests` is
  controller admission accounting, **not authoritative Console billing/usage**.

Pacing is a long-run spacing limit with one HTTP batch's natural burst at its
start, not an exact server-side sliding-minute quota guarantee. It cannot make
available workers, network capacity or successful writes appear. Many timeouts
can drive the rate well below the requested ceiling; this is deliberate.

`GET /api/status` exposes the configured ceiling and shared pool pacer snapshot
when enabled. Snapshot fields are numeric only. No credentials or payloads are
logged. The terminal summary explicitly refers to the shared pool, not a
per-job counter. Metrics are not persisted across server restarts.

## Verification

The regression first ran against the actual uploader/batch/retry seam and failed:
the observation probe received no admissions instead of `[4, 1]` for an initial
four-item batch plus a retry of its one transient item. It passes after wiring
the controller into each application-level request attempt.

```powershell
python -m pytest tests/test_upload_pacing.py -q
```

Deterministic injected-clock tests verify cost-based spacing, healthy ramp to
the ceiling, wave-coalesced reductions, lower configured targets, final-attempt
feedback, security failures not disguised as congestion, and unchanged IDs and
full restoration. Both UI-style pooled clients and standalone parallel clients
share the same admission budget. No network is used by these tests.

The benchmark's offline caller test verifies input hashes are identical across
profiles, filenames/dataset IDs are fresh, and profile order reverses. Synthetic
clock rates do not demonstrate that Google will serve requests at those rates.

## Deferred live comparison

Update: the subsequent October 22 live test regressed and was stopped during a
timeout wave. Pacing remains disabled; see the measured results and selected
recovery fix in [the follow-up](performance-recovery-20261022.md). The command
below documents the original plan, not an instruction to rerun it automatically.

Only run after current user transfers finish; do not run as competing traffic.
This command is a **plan, not a completed measurement**:

```powershell
python scripts/benchmark.py --profiles 64x8 --request-targets 0 11400 --input-bytes 33554432 --chunks 1790 --shared-auth --repeats 2 --download-workers 16 --database data/pacing-comparison.db --report data/pacing-comparison.jsonl --cleanup --cleanup-at-end --cleanup-workers 4
```

The comparison is fixed/adaptive/adaptive/fixed on the same 32 MiB random input
with separate dataset names. It reports the actual target and pacer admissions
alongside committed chunk throughput, HTTP latencies, timeouts and retry counts.
Full reconstruction remains outside upload timing. Existing quota guards stop
subsequent tests and immediate cleanup after recognized 403/429 backoff; retain
the isolated index/report for verified targeted cleanup after cooldown.

Do not enable this in the live `.env` merely because the controller can reach
190/s under a fake clock. Promote it only if repeated real comparisons improve
committed bytes/second without worsening failure behavior. Nothing in this
experiment guarantees a 95% Console reading or native Drive upload speed.
