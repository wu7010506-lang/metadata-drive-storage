# Google Drive metadata storage: Milestone 1 research

Research date: 2026-09-10. Sources are limited to first-party Google documentation.

## Decision summary

- A metadata-only request uses `POST https://www.googleapis.com/drive/v3/files`, sends a Drive `File` resource as JSON, and does not require `uploadType`. It creates a real Drive item, not a separate hidden metadata record. With no metadata or content, Google documents an `Untitled` item with MIME type `application/octet-stream`; folders and shortcuts are also documented metadata-only file types. ([`files.create` reference](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/create), [create-file guide](https://developers.google.com/workspace/drive/api/guides/create-file#create_metadata-only_files))
- `properties` and `appProperties` are unsuitable for large single-field payloads: every key plus value is limited to 124 UTF-8 bytes, with at most 30 public properties per file and at most 30 private properties per application per file. ([custom-property limits](https://developers.google.com/workspace/drive/api/guides/properties#limits_of_custom_file_properties))
- Google documents no numeric maximum for `name` or `description` in the Drive API v3 file-resource reference. This is absence of a documented limit, not evidence of unlimited or supported storage capacity. Probe these fields conservatively and stop at the first API rejection. ([File resource](https://developers.google.com/workspace/drive/api/reference/rest/v3/files))
- For a service-account prototype, prefer a shared drive or Workspace user impersonation. Multiple current Google pages say service accounts have no Drive storage quota and cannot own files. ([shared-drive overview](https://developers.google.com/workspace/drive/api/guides/about-shareddrives), [`storageQuotaExceeded`](https://developers.google.com/workspace/drive/api/guides/handle-errors#storageQuotaExceeded))
- Retry only transient failures: 403 with reason `rateLimitExceeded` or `userRateLimitExceeded`, HTTP 429, and HTTP 500/502/503/504. Do not retry every 403. ([Drive error guide](https://developers.google.com/workspace/drive/api/guides/handle-errors))

## 1. `files.create` metadata-only behavior

The metadata endpoint is:

```http
POST https://www.googleapis.com/drive/v3/files
Content-Type: application/json

{
  "name": "probe-item",
  "description": "probe",
  "appProperties": {"probe": "value"},
  "parents": ["TARGET_FOLDER_ID"]
}
```

The body is a Drive `File`; a successful response is also a `File`. The `/upload/drive/v3/files` endpoint and `uploadType` are for media uploads, while `uploadType` is not required for metadata-only creation. ([`files.create`](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/create))

Google defines metadata-only files as files with no content and names folders and shortcuts as examples. Separately, creating with no parameters produces an empty `Untitled`, `application/octet-stream` item. Therefore, for the probe, a JSON-only create request with ordinary file metadata is best treated as creation of a normal Drive item without uploaded media—not as a special metadata database object. This last sentence is an inference from the two documented behaviors. ([create-file guide](https://developers.google.com/workspace/drive/api/guides/create-file))

Location matters:

- A file can have only one parent. If `parents` is omitted on create, Drive places it in the caller's My Drive root. ([File resource: `parents`](https://developers.google.com/workspace/drive/api/reference/rest/v3/files#File.FIELDS.parents))
- For shared-drive operations, including `files.create`, `files.get`, `files.update`, and `files.delete`, send `supportsAllDrives=true`. ([shared-drive support](https://developers.google.com/workspace/drive/api/guides/enable-shareddrives))
- Cleanup should permanently delete every probe object by recorded file ID. A failed/indeterminate create may have succeeded remotely, so record the returned ID immediately whenever available.

### Retry/idempotency caveat

Blindly retrying `files.create` after an indeterminate network/server result can create a duplicate. Google supports pre-generated file IDs: reuse of an ID after a successful create returns `409 Conflict` rather than creating a duplicate. Pre-generated IDs are not supported for Google Workspace-file creation except Drive SDK files and folders. This mechanism is available for ordinary non-Workspace probe objects and is worth using if the client design permits it. ([generate IDs](https://developers.google.com/workspace/drive/api/guides/create-file#generate_ids_to_use_with_your_files))

## 2. Candidate metadata fields

| Field | Official meaning / visibility | Documented numeric limit relevant here | Milestone 1 implication |
|---|---|---:|---|
| `name` | File name; need not be unique within a folder. | None stated in the v3 resource reference. | User-visible identity metadata, not a private store. Probe conservatively; do not claim unlimited capacity. |
| `description` | A short description of the file. | None stated in the v3 resource reference. | Standard metadata and likely user-visible; “short” gives no numeric contract. Probe conservatively. |
| `properties` | Arbitrary key/value pairs visible to all apps that can access the file. “Visible to all apps” does **not** make the file public on the internet; file ACLs still govern access. | 30 public properties per file across all sources; key + value <= 124 UTF-8 bytes for each property. | Hard-cap every attempted property before sending it. |
| `appProperties` | Arbitrary key/value pairs private to the requesting app. Retrieval requires an authenticated OAuth 2.0 access token; an API key cannot retrieve them. | 30 private properties per file from any one app; key + value <= 124 UTF-8 bytes for each property. | Best privacy fit of the four, but capacity is only a few KB at most and less after key overhead. |

Sources: [File resource fields](https://developers.google.com/workspace/drive/api/reference/rest/v3/files), [custom file properties](https://developers.google.com/workspace/drive/api/guides/properties).

Custom-property limits in full:

- Maximum 100 custom properties per file, totaled from all sources.
- Maximum 30 public `properties` per file, totaled from all sources.
- Maximum 30 private `appProperties` per file from any one application.
- Maximum 124 bytes in UTF-8 for each property's key and value combined.

The 124-byte rule means payload capacity is not `30 * 124`: every key consumes part of its property's allowance. The actual safe aggregate must subtract all key bytes and leave a safety margin. It must also respect the 100-property total if other applications already use properties on the file. ([limits](https://developers.google.com/workspace/drive/api/guides/properties#limits_of_custom_file_properties))

On update/copy, entries with `null` values are cleared. The Drive UI does not provide a built-in editor for custom properties. ([File resource](https://developers.google.com/workspace/drive/api/reference/rest/v3/files), [properties guide](https://developers.google.com/workspace/drive/api/guides/properties))

## 3. Service account and shared-drive setup

Recommended setup for this prototype:

1. Enable Drive API in the Google Cloud project and create a service account/key; load the key through `GOOGLE_APPLICATION_CREDENTIALS`, never from a committed path.
2. Use a shared drive from a supported Google Workspace edition. Personal `@gmail.com` accounts cannot create shared drives, although they can sometimes be added as external members if organization policy permits. ([shared-drive overview](https://developers.google.com/workspace/drive/api/guides/about-shareddrives))
3. Explicitly add/share the target shared drive or folder with the service account email and grant a role capable of adding files (for example, `writer`). Domain-wide sharing does not automatically include service accounts because service accounts are not members of the Workspace domain. ([manage shared drives](https://developers.google.com/workspace/drive/api/guides/manage-shareddrives), [roles](https://developers.google.com/workspace/drive/api/guides/ref-roles))
4. Configure `GOOGLE_DRIVE_FOLDER_ID` (or equivalent) to the exact target folder, set it as the sole parent on create, and pass `supportsAllDrives=true` on all relevant calls.

Google also documents that a specific Drive file/folder can be directly shared with the service-account email without domain-wide delegation or an administrator role; deselect notification because a service account has no inbox. However, sharing a folder only grants access—it does not remove the separate current warning that service accounts have no storage quota and cannot own files. For creating new probe objects, shared drive or user impersonation is the conservative choice. ([service-account credentials](https://developers.google.com/workspace/guides/create-credentials#service-account))

Domain-wide delegation is needed only when acting on behalf of Workspace users across a domain. Each call then acts as the impersonated user and remains limited by that user's permissions and the delegated OAuth scopes; it is not blanket Drive access. ([delegating domain-wide authority](https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority))

### Current Google documentation inconsistency

The current create-file guide's “File ownership” section says a service account can own files in dedicated Drive storage, while the shared-drive overview and Drive error guide say service accounts have no storage quota and cannot own files. Do not base Milestone 1 on service-account My Drive behavior. Use a shared drive or impersonate a human Workspace user, and report a clear setup error when neither is configured. ([create-file ownership section](https://developers.google.com/workspace/drive/api/guides/create-file#file_ownership), [shared-drive overview](https://developers.google.com/workspace/drive/api/guides/about-shareddrives), [`storageQuotaExceeded`](https://developers.google.com/workspace/drive/api/guides/handle-errors#storageQuotaExceeded))

## 4. Retry and backoff policy

Drive's 403 response is overloaded: it can mean a quota problem, lack of permission, storage exhaustion, folder limits, or domain policy. Parse the JSON error `reason`; status code alone is insufficient. ([403 errors](https://developers.google.com/workspace/drive/api/guides/handle-errors#403-errors))

Retry with exponential backoff:

- HTTP 403 only when `reason` is `rateLimitExceeded` or `userRateLimitExceeded`.
- HTTP 429 (`rateLimitExceeded`).
- HTTP 500, 502, 503, or 504.
- Transient transport failures where the outcome is unknown, while applying the create-idempotency protection noted above.

Do not retry ordinary permission/authentication errors, `storageQuotaExceeded`, structural/file limits, invalid requests, or other non-transient 403 reasons without their documented corrective action. ([Drive error guide](https://developers.google.com/workspace/drive/api/guides/handle-errors))

Google's documented truncated exponential-backoff example is:

```text
delay = min((2 ** n) + random_milliseconds, maximum_backoff)
```

`random_milliseconds` is freshly chosen on each retry and is at most 1,000 ms. Google says a typical `maximum_backoff` is 32 or 64 seconds. The application must also cap the number of retries and must not retry indefinitely. The precise delay and retry count depend on the use case and network conditions. ([usage limits: exponential backoff](https://developers.google.com/workspace/drive/api/guides/limits#exponential))

For this small probe, a reasonable implementation that follows the documented shape is `max_attempts`, `maximum_backoff`, fresh jitter on every retry, and a strict request counter. If retries are exhausted, surface the last structured Drive error and still attempt cleanup of all known probe IDs.

## 5. Milestone 1 consequences

- Treat the official custom-property limits as validation rules, not values to rediscover by sending deliberately invalid requests. Any `properties`/`appProperties` probe should stay at or below 124 UTF-8 bytes per key/value pair and the documented property counts.
- Measure encoded UTF-8 bytes, not Python character count. Base64URL payload is ASCII, but keys and future metadata may not be.
- Keep each backend independent. Results for `name` or `description` do not generalize to custom properties.
- Persist the exact backend, attempted byte length, read-back byte length/hash, latency, request count, and structured failure reason. An undocumented empirical maximum is environment/time dependent and must be labeled observational rather than contractual.
- Use a small, fixed number of temporary objects. Prefer updating one object during growth/binary-search cycles where the field semantics permit it; delete every known object in `finally`-style cleanup.
- `appProperties` is the strongest semantic match for app-private metadata, but its official size limits make it unsuitable for the large chunks implied by a bulk-storage design. Milestone 1 should report that result plainly rather than attempt to evade the limits.

