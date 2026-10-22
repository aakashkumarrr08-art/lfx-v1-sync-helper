# LFX v1 Sync Helper

The LFX v1 Sync Helper is a Go microservice that synchronizes data between LFX v1 and LFX One (v2). It syncs v1 data from NATS KV buckets into v2 services via authenticated API calls, and also makes authenticated API calls back to LFX v1 endpoints to keep v1 in sync with changes originating in v2. It uses a JetStream consumer-based architecture that enables horizontal scaling across multiple service instances for processing v1 data updates.

## Architecture Overview

### Key implementation decisions

1. **KV Bucket Watcher**: Instead of consuming NATS messages directly from streaming data sources, this service watches a NATS KV bucket (`v1-objects`) where v1 data is written by replication jobs (e.g. Meltano)
2. **Direct API Calls**: Core resources (projects, committees) are synced via LFX One API services. Records owned by external wrapper services (e.g. meetings, voting, surveys) and records consumed directly from the `v1-objects` KV bucket by other services (e.g. `salesforce_b2b-*`) are passed through without API calls.
3. **JWT Authentication**: Reuses Heimdall's signing key to create JWT tokens for secure API authentication, supporting user impersonation while also bypassing LFX One permissions—as Heimdall tokens are not just proof of authentication, but are of *authorization*.
4. **Mapping Storage**: Maintains v1-to-v2 ID mappings in a dedicated NATS KV bucket to track state and to avoid introducing "legacy ID" fields in LFX One data models.

### Data Flow

For a more detail view, see the root [README.md](../../README.md) diagrams.

```mermaid
flowchart LR
    meltano[Meltano]
    v1-KV[v1-objects<br />KV Bucket]
    v1-sync-helper["JetStream Consumer<br />(Load Balanced)"]
    v2_api[Project/Committee Services]
    indexer[Indexer Service]
    v1_api[LFX v1 API Gateway]
    meltano -->|puts into| v1-KV
    v1-KV -->|watched by| v1-sync-helper
    v1-sync-helper -->|makes API<br />calls to| v2_api
    indexer -->|publishes domain<br />events to| v1-sync-helper
    v1-sync-helper -->|makes API<br />calls to| v1_api
```

## Horizontal Scaling

The service is designed to scale horizontally across multiple instances using a JetStream consumer with delivery groups:

### Scaling Architecture

Retrieving updates from the KV Bucket handled via a non-ephemeral **JetStream pull consumer**. This has the advantage over an ordinary `kv.Watch()` of distributing messages across multiple instances, similar to how other LFX One services use named queues to scale out processing.

The service attempts to create a JetStream pull consumer with the following properties, if it does not already exist (created by the Helm chart, for instance):

```yaml
name: v1-sync-helper-kv-consumer
stream: KV_v1-objects
filterSubject: "$KV.v1-objects.>"
ackPolicy: explicit
maxDeliver: 3
ackWait: 30s
maxAckPending: 1000
```

### Supported Objects

#### v1 → v2 (KV bucket watch)

- **Projects**: LFX project nested hierarchy (PCC / Salesforce)
- **Committees & members**: LFX committees (PCC)

#### v2 → v1 (indexer domain events)

|NATS Subject|Action|
|---|---|
|`lfx.committee.created`|Create committee in v1 via Project Service API|
|`lfx.committee.updated`|Update committee in v1 via Project Service API|
|`lfx.committee.deleted`|Delete committee in v1 via Project Service API|
|`lfx.committee_member.created`|Add member to committee in v1 via Project Service API|
|`lfx.committee_member.updated`|Update committee member in v1 via Project Service API|
|`lfx.committee_member.deleted`|Remove member from committee in v1 via Project Service API|

Loop detection prevents infinite sync cycles: if a non-tombstoned reverse mapping already exists for the object UID, the event originated from v1 and is skipped.

## Configuration

### Environment Variables

| Variable                    | Required | Description                                                                       |
|-----------------------------|----------|-----------------------------------------------------------------------------------|
| `NATS_URL`                  | No       | NATS server URL (default: `nats://nats:4222`)                                     |
| `PROJECT_SERVICE_URL`       | Yes      | Project Service API URL                                                           |
| `COMMITTEE_SERVICE_URL`     | Yes      | Committee Service API URL                                                         |
| `HEIMDALL_CLIENT_ID`        | No       | Client ID for JWT claims (default: `v1_sync_helper`)                              |
| `HEIMDALL_PRIVATE_KEY`      | Yes      | JWT private key (PEM format) for v2 services                                      |
| `HEIMDALL_KEY_ID`           | No       | JWT key ID (if not provided, fetches from JWKS)                                   |
| `HEIMDALL_JWKS_URL`         | No       | JWKS endpoint URL (default: cluster service)                                      |
| `AUTH0_TENANT`              | Yes      | Auth0 tenant name (without .auth0.com suffix)                                     |
| `AUTH0_CLIENT_ID`           | Yes      | Auth0 client ID for v1 API authentication                                         |
| `AUTH0_PRIVATE_KEY`         | Yes      | Auth0 private key (PEM format) for v1 API                                         |
| `LFX_API_GW`                | No       | LFX API Gateway URL (default: `https://api-gw.dev.platform.linuxfoundation.org/`) |
| `DATABASE_URL`              | Yes*     | PostgreSQL DSN for the v1 platform database (live user lookups). *Alternatively set the discrete `V1_DB_*` variables below. |
| `V1_DB_HOST`                | Yes*     | v1 platform database host (used when `DATABASE_URL` is unset)                     |
| `V1_DB_PORT`                | No       | v1 platform database port (default: `5432`)                                       |
| `V1_DB_NAME`                | No       | v1 platform database name (default: `sfdc`)                                       |
| `V1_DB_USER`                | Yes*     | v1 platform database username (used when `DATABASE_URL` is unset)                 |
| `V1_DB_PASSWORD`            | Yes*     | v1 platform database password (used when `DATABASE_URL` is unset)                 |
| `V1_DB_SSLMODE`             | No       | PostgreSQL `sslmode` parameter (default: `prefer`)                                |
| `USE_MSGPACK`               | No       | Encode KV values as MessagePack instead of JSON (default: `false`)                |
| `DYNAMODB_INGEST_ENABLED`   | No       | Subscribe to DynamoDB stream events from `dynamodb-stream-consumer` (default: `false`). Requires the `dynamodb_streams` NATS stream to exist. |
| `DYNAMODB_STREAM_NAME`      | No       | NATS stream name to consume DynamoDB events from (default: `dynamodb_streams`)    |
| `MEMBER_SERVICE_URL`        | No       | Member Service API URL; required only for the `--backfill-acs-org` pass           |
| `PORT`                      | No       | HTTP server port (default: `8080`)                                                |
| `BIND`                      | No       | Interface to bind on (default: `*`)                                               |
| `DEBUG`                     | No       | Enable debug logging (default: `false`)                                           |

### Setting authentication parameters

The following script demonstrates how to set environment variables for both LFX v2 Heimdall impersonation and LFX v1 Auth0 authentication:

```bash
# LFX v2 Heimdall Authentication
# Optional - used for principal and subject claims (default: v1_sync_helper)
export HEIMDALL_CLIENT_ID="v1_sync_helper"
# Required - PEM formatted private key for signing JWTs
export HEIMDALL_PRIVATE_KEY="$(kubectl get secret/heimdall-signer-cert -n lfx -o json | jq -r '.data["signer.pem"]' | base64 --decode)"
# Optional - if not provided, fetches from JWKS endpoint
export HEIMDALL_KEY_ID="your-key-id"
# Optional - defaults to cluster service
export HEIMDALL_JWKS_URL="http://lfx-platform-heimdall.lfx.svc.cluster.local:4457/.well-known/jwks"

# LFX v1 Auth0 Authentication
export AUTH0_TENANT="linuxfoundation-dev"
export AUTH0_CLIENT_ID="your-auth0-client-id"
export AUTH0_PRIVATE_KEY="$(cat auth0-private-key.pem)"
# Optional - defaults to dev environment
export LFX_API_GW="https://api-gw.dev.platform.linuxfoundation.org/"
```

#### User impersonation logic

The service implements per-record user impersonation. JWT tokens for the same
identity will be reused within the same sync operation to minimize overhead for
token generation (and potentially for downstream validation, e.g. in the
indexer service).

**Machine User Impersonation** (when v1 `lastmodifiedbyid` has `@clients` suffix):
- **Principal**: `{client_id}@clients` (passed through verbatim)
- **Subject**: `{client_id}` (without @clients suffix)
- **Email**: Not included

**User Impersonation** (when v1 `lastmodifiedbyid` is User Service platform ID):
- Looks up user via LFX v1 User Service API: `GET /v1/users/{platformID}`
- **Principal**: `{username}` (from API response)
- **Subject**: `{username}` (same as principal)
- **Email**: `{email}` (from API response if available)
- **Caching**: User data cached for 6 hours (background-refreshed if older than 10 minutes)
- **Locking**: Prevents concurrent API calls for same user

**Fallback Client Mode** (when no principal or lookup fails):
- **Principal**: `v1_sync_helper@clients`
- **Subject**: `v1_sync_helper`
- **Email**: Not included

This approach ensures that v1 sync operations are properly attributed to the actual user who made the changes, with efficient caching to minimize API calls.

## Development

### Prerequisites

- Go 1.26+
- NATS Server with JetStream enabled
- Access to Heimdall JWT configuration

### Building

```bash
# Build the binary
make build

# Build with debug symbols
make debug

# Run all checks (format, lint, test)
make check

# Run the service
make run
```

## Deployment

### Kubernetes with Helm

See the Helm chart in `./charts/lfx-v1-sync-helper` for deployment configuration.

### One-shot administrative commands

The binary supports one-shot commands that exit after completing their task.

**`--backfill-acs-project`** — merges ACS user grants into v2 project settings (`Writers`, `Auditors`, `MeetingCoordinators`). Reads SFID→UID mappings from the `v1-mappings` KV bucket. Additive-only (never removes existing entries) and idempotent. Supports `--dry-run` to preview without writing.

**`--backfill-acs-org`** — merges ACS org grants (`company-admin` → `writer`, `viewer` → `auditor`) into v2 b2b_org settings. Reads live LF member org SFIDs from the `v1-objects` KV bucket (`salesforce_b2b-Account.*` keys, filtered to `IsDeleted=false` and `IsMember__c=true`). The b2b_org UID is the 18-char normalized SFID (`sfutil.Normalize18`) — no network round-trip. Requires `MEMBER_SERVICE_URL`. Additive-only and idempotent. Supports `--dry-run`. End-of-run summary includes: `orgs_total`, `orgs_changed`, `writers_added`, `auditors_added`, `orgs_skipped`, `errors`.

**`--backfill-workspaces`** — backfills legacy workspace data into v2 member service. Reads workspace and workspace_project records from the `v1-objects` KV bucket (`platform-organization_workspace.*` and `platform-organization_workspace_project.*` keys) and syncs them into v2 via the member service API. Requires `MEMBER_SERVICE_URL`. Additive-only and idempotent. Supports `--dry-run`. End-of-run summary includes: `workspaces_total`, `workspaces_created`, `workspaces_updated`, `workspaces_deleted`, `projects_added`, `projects_removed`, `workspaces_skipped`, `workspaces_errors`.

**`--backfill-projects`** — re-emits v1 `salesforce-project__c` rows with no live `project.sfid.*` v1-mappings entry (LFXV2-3220 removed the project allowlist; projects created before 17 Aug 2026 and never modified since were never synced) by re-PUTting the existing `v1-objects` value and letting the running deployment's KV consumer create them. Because that consumer unconditionally ACKs project messages and hard-errors on a missing parent mapping, candidates are re-emitted parent-before-child, level by level (`orderCandidatesByDepth`), rate-limited (`--emit-rate`, default 2.0/s) with a settle-poll between levels. Refuses to start a live run that mixes Formation-staged and non-Formation candidates unless `--exclude-stage-prefix`, `--include-stage-prefix`, or `--allow-formation` is passed. Aborts unless `--force` if live+tombstoned project mappings fall below the safety floor (~1000). Requires only NATS (`NATS_URL`, `AUTH0_CLIENT_ID`) — no v2 API calls. Supports `--dry-run` (candidate/ordering report only — cannot observe the downstream create) and `--limit` (0 = unlimited). End-of-run summary includes: `scanned`, `mappings_live`, `mappings_tombstoned`, `candidates`, `emitted`, `levels`, `formation_candidates`, `remaining_unmapped`, `errors`.

The flags are independent and mutually exclusive — run each as a separate Job invocation.

### Detecting committee parent drift (read-only)

`scripts/detect_committee_parent_drift.py` compares three **complete**, closely
timed CSV exports without connecting to or writing either system:

- `--v1`: `committee_sfid,parent_sfid` from live `platform.collaboration__c`
  rows (`sfid`, `committee_id`), including rows whose parent is null/empty.
- `--v2`: `committee_uid,parent_uid` from current v2 committee bases, including
  committees without a parent. Export the actual current bases, not index history.
- `--mappings`: `committee_sfid,committee_uid` from **live**
  `committee.sfid.*` v1-mappings entries; exclude tombstones (`!del`).

```sh
python3 scripts/detect_committee_parent_drift.py \
  --v1 v1-committees.csv --v2 v2-committees.csv \
  --mappings committee-mappings.csv > committee-parent-report.csv
```

The report includes matches, both-empty pairs, one-sided parents, different
parents, unmapped parents/committees, and missing records. It does not choose
which system wins; even a blank parent might be an intentional clear. Review
conflicts and establish a source-of-truth policy **before deploying** the
bidirectional parent writer: a subsequent full-snapshot v2 update can clear a
v1 parent when v2 has none, even if v2 was only missing that parent due to the
old sync omission. A v1 update carrying `committee_id` can likewise overwrite
a conflicting v2 parent. The script contains no repair/write path. Treat
exports and the report as operational data.

The existing `--backfill-committees` command repairs *unmapped committees*,
not parent links on already mapped committees. It does not depth-order nested
unmapped committees, so it must not be used as a hierarchy repair command.

In production, apply the Job manifest manually (not ArgoCD-managed). A dry-run pass is recommended first:

```sh
kubectl --context lfx-v2-prod -n v1-sync-helper apply -f manifests/backfill-acs-job.yaml
```

Locally:

```sh
lfx-v1-sync-helper --backfill-acs-project [--dry-run]
lfx-v1-sync-helper --backfill-acs-org [--dry-run]
```

### Required NATS KV Buckets

The service requires two NATS KV buckets:

1. **`v1-objects`**: Stores v1 objects from Meltano (10GB capacity)
2. **`v1-mappings`**: Stores v1-to-v2 ID mappings (2GB capacity)

These are automatically created by the Helm chart.

## API Integration

### JWT Token Generation

The service generates JWT tokens for each API call using PS256 algorithm:

- **Algorithm**: PS256 (RSA-PSS with SHA-256)
- **Key ID**: From config or JWKS endpoint
- **Issuer**: `heimdall` (fixed)
- **Subject**: varies per record (see User Impersonation Logic)
- **Principal**: varies per record (see User Impersonation Logic)
- **Email**: optional; varies per record (see User Impersonation Logic)
- **Audiences**: Service-specific (hardcoded, but varies per record)
  - Project Service: `lfx-v2-project-service`
  - Committee Service: `lfx-v2-committee-service`
  - Member Service: `lfx-v2-member-service`
- **Expiration**: 5 minutes

## Monitoring

### Health Endpoints

- **`/livez`**: Liveness probe (always returns OK while service is running)
- **`/readyz`**: Readiness probe (checks NATS connection status)

### Logging

The service uses structured JSON logging with the following levels:

- **ERROR**: Critical errors that require attention
- **WARN**: Non-critical issues (e.g., user lookup failures)
- **INFO**: Important operations (e.g., successful project creation)
- **DEBUG**: Detailed operation information (enabled with `DEBUG=true`)

### Key Log Fields

- `key`: KV bucket key being processed
- `operation`: KV operation type (PUT, DELETE)
- `slug`/`sfid`: Object identifiers
- `project_uid`/`committee_uid`: Generated V2 UUIDs
- `username`: Extracted from v1 `lastmodifiedbyid`

## License

Copyright The Linux Foundation and each contributor to LFX.

This project’s source code is licensed under the MIT License. A copy of the
license is available in LICENSE.

This project’s documentation is licensed under the Creative Commons Attribution
4.0 International License \(CC-BY-4.0\). A copy of the license is available in
LICENSE-docs.
