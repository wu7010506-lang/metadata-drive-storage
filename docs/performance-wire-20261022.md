# 2026-10-22: response negotiation, capacity and request granularity

## Gzip hypothesis: not selected

Google's [performance guide](https://developers.google.com/workspace/drive/api/guides/performance)
describes requesting gzip via Accept-Encoding and a User-Agent containing gzip.
An offline real-SDK batch capture showed that its outer request did not explicitly
set those two headers before reaching the authorized transport. That was a
testable negotiation difference, **not proof the live response was uncompressed**.

Inspection of the installed `httplib2.Http.request` explains the live result:
after the authorized-transport boundary it supplies the default User-Agent
`Python-httplib2/<version> (gzip)` and `Accept-Encoding: gzip, deflate` when absent
(and no Range header). The original transport was therefore already negotiating
gzip at the lower layer. The production-disabled experiment does not parse URLs
or add headers on the default path.

A read-only four-call comparison of eight existing description chunks returned
gzip in both modes, with exactly matching hashes and 205,310 decoded response
bytes per batch. Times were 1.782/1.851s legacy versus 0.841/0.929s explicit gzip,
but this small read-only observation cannot establish an upload gain or attribute
its difference to compression (both responses were already compressed).

The upload A/B used identical 9,599,872 random bytes, 513 chunks, 64 x 8,
description/Base64URL, cached initial auth and independent cold worker clients.
Order: legacy/gzip/gzip/legacy. All four restored exactly with zero upload retries
or observed quota backoff. Deletions are outside all upload timing windows.

| Batch outer negotiation | Round 1 seconds | Round 2 seconds | Mean seconds |
|---|---:|---:|---:|
| Legacy | 19.970 | 19.706 | 19.8380 |
| Explicit gzip | 21.643 | 19.862 | 20.7525 |

All 64 successful outer batch responses per upload were gzip in **both** modes,
each totalling 13,164,306 decoded bytes. `decoded_bytes` is not compressed wire
length; httplib2 already decompressed the content. No wire-byte reduction is
claimed. Explicit negotiation was about 4.6% slower by these two means, with no
demonstrated benefit; keep it **off** in production. It is available only as an
explicit benchmark profile through `--batch-compression-profiles legacy gzip`.

Reports/index: `data/batch-gzip-20261022.jsonl`, `data/batch-gzip-20261022.db`.
The benchmark now counts gzip responses and decoded bytes without storing URLs,
credentials, headers or payloads. The transport option changes only batch response
negotiation on the exact HTTPS Drive batch endpoint, never request contents,
TLS validation, direct-call headers or payload verification.

## Bounded description create probe

`probe_description_create.py` reserves and journals every ID before creation,
uses random Base64URL ASCII rather than repeated characters, checks both the
create response and an independent GET, and removes only objects whose ID,
name, parent, marker, MIME and zero-content size match the probe. It stops on
the first HTTP 400/413 rejection; other errors propagate. Offline safety tests
cover stopping at rejection, refusing wrong-parent deletion and preserving
unrelated objects.

On this account/folder, 25,000 characters created and read back correctly;
32,768 was rejected with HTTP 400. The 25,000-character object was removed and
the rejected ID was absent. No larger sizes were attempted. This brackets an
observed acceptance/rejection, not an exact maximum or a Google contract.
Keep the existing description capacity at 25,000. Report:
`data/description-create-20261022.jsonl`.

All four gzip-comparison datasets (2,052 objects) were subsequently removed,
and the only created capacity-probe object was removed; the rejected ID was
absent. No user files or live-index records changed. Explicit gzip remains off.
Offline suite including real-SDK header tests, metric privacy, capacity cleanup
scope and 64-worker direct-create failure/resume tests: **300 passed**.

## Request-granularity protocol

```powershell
python scripts/benchmark.py --profiles 64x8 64x4 64x1 --chunks 512 --shared-auth --repeats 2 --download-workers 16 --database data/granularity64-20261022.db --report data/granularity64-20261022.jsonl --cleanup --cleanup-at-end --cleanup-workers 8
```

Run only after the previous cleanup completes; same random 9,599,872 source
bytes, 513 chunks, order 8/4/1/1/4/8 at 64 workers. The one-item path uses direct
verified creates, not a batch envelope. This changes nominal in-flight chunk
budget from 512 to 256 to 64, respectively, while retaining the same maximum
HTTP-worker count. Fewer in-flight chunks are not assumed to be slower: each
request may return sooner. All payload/metadata matching, stable IDs, SQLite
reservation and completion ordering, and retry bounds stay enabled. Downloads
and deferred cleanup remain outside measured upload duration. No native-Drive
or GiB sustained throughput claim is implied by this short comparison.

## Granularity result: no selection

| Profile | First upload seconds | Outcome |
|---|---:|---|
| 64 x 8 | 20.789 | Full restoration verified; no upload retry |
| 64 x 4 | 20.726 | Full restoration verified; no upload retry |
| 64 x 1 | 35.576 | Seven recognized HTTP 403 rate-limit backoffs; no full restore performed |

The SDK reported `userRateLimitExceeded` during direct creates. All 513 direct
create records were committed complete after their normal response checks, but
the hardened benchmark stopped at phase `upload`, before a verification download
or the remaining reverse-order profiles. There are only **two verified report
rows**, not a completed six-run comparison. The direct-upload elapsed time and
quota count come from its captured terminal output, not a fabricated verified row.

The 0.063-second first-round difference between batches of 4 and 8 is not a
demonstrated speedup. Direct calls were slower and rate limited in this run.
Keep 64 x 8, description/Base64URL, 25,000-character payloads, existing verification
and retry limits; do not adopt single requests, explicit gzip or larger payloads.
The rate-limit response identifies the observed reason, not the exact limiting
dimension or proof of a particular Console quota percentage being exhausted.

The upload returned before the quota guard; therefore no test upload remains
running. The isolated index, exact generated filenames and stable IDs are kept
for subsequent cooled-down, low-concurrency identity/content-verified cleanup.

## Completion

Following more than a minute without new upload load, targeted recovery ran at
four workers. Every one of the three exact dataset/filename pairs was checked
against its isolated index and remote ID, parent, name, MIME, zero-content size,
app properties and payload SHA-256 before deletion. Journals confirm **513 deleted,
0 absent for each dataset**, 1,539 total. This does not retroactively label the
third dataset as fully downloaded/restored; its verification status stays distinct.

Across this turn, 2,052 gzip-test objects, 1,539 granularity-test objects and one
capacity object were removed (**3,592 newly created test objects**). The rejected
capacity ID was absent. Cleanup experienced bounded transport-timeout retries;
all cleanup operations ultimately completed. No user files or live-index records
were removed, and reports/isolated indexes remain for audit. Generated pytest
fixtures were checked for path bounds/reparse points and removed (42,849,641 bytes
in the main fixture cleanup, plus the tiny final guard-test directory); rerunning
tests regenerates them.

Full offline suite: 300 passed. After avoiding default-path URL parsing, the
20 targeted auth/benchmark tests passed again. Production stays 64 x 8, 16
download workers, 25,000 description characters, Base64URL, full response checks
and the 10 GiB input cap; the existing healthy 8770 process was not interrupted.
Explicit batch gzip remains disabled. **No additional fresh-file upload speedup
was found or deployed in this turn.**
