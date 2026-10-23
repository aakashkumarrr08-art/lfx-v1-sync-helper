# AGENTS.md

This file provides essential information for AI agents working on the LFX v1 Sync Helper codebase. It focuses on development workflows, architecture understanding, and build processes needed for making code changes.

> **Important: invoke `/lfx-skills:lfx` for any cross-repo task or "where does X live" question.** Routes to owning repos and pulls in their CLAUDE.md, skills, and rules. If `/lfx-skills:lfx` is not found, install with `/plugin marketplace add linuxfoundation/lfx-skills` then `/plugin install lfx-skills@lfx-skills`.

## Repository Overview

The LFX v1 Sync Helper enables data synchronization between LFX v1 and LFX One (v2) platforms with three main components:

1. **Meltano ETL Pipeline**: Python-based data extraction and loading (DynamoDB → NATS KV)
2. **v1-sync-helper Service**: Go microservice for data synchronization (NATS KV → LFX One APIs)
3. **Helm Charts**: Kubernetes deployment manifests

## Architecture Overview

### Data Flows
```text
LFX v1 Sources → Meltano → NATS KV → v1-sync-helper → LFX One APIs
```

### Key Components
- **DynamoDB** + **PostgreSQL (Projects/Committees)** → **Meltano** → **NATS KV Bucket (`v1-objects`)**
- **NATS KV Watcher** → **v1-sync-helper** → **LFX One Project/Committee Services**
- **JWT Authentication** via Heimdall impersonation for secure API calls
- **ID Mappings** stored in NATS KV bucket (`v1-mappings`)
- **Data Encoding** supports both JSON and MessagePack formats with automatic detection

## Repository Structure

```text
lfx-v1-sync-helper/
├── meltano/                   # Python ETL pipeline
│   ├── meltano.yml            # Main Meltano configuration
│   └── load/target-nats-kv/   # Custom NATS KV target plugin
├── cmd/lfx-v1-sync-helper/    # Go microservice source
├── charts/lfx-v1-sync-helper/ # Helm deployment charts (Chart.yaml version is dynamic on release)
├── docker/                    # Docker build configurations
│   ├── Dockerfile.v1-sync-helper  # Go service container
│   └── Dockerfile.meltano         # Python ETL container
├── .github/workflows/         # CI/CD pipelines
├── Makefile                   # Build automation
└── pyproject.toml             # Python dependency management (uv)
```

## Development Workflow

### Local Setup

1. **Initialize Python environment:**
   ```bash
   uv sync
   ```

2. **Verify Meltano installation:**
   ```bash
   cd meltano
   uv run meltano dragon
   ```

3. **Build Go service:**
   ```bash
   make build
   ```

4. **Run all checks:**
   ```bash
   make check  # Runs fmt, vet, lint
   ```

## Build System (Makefile)

### Container Build Targets

| Target                        | Description                                             |
|-------------------------------|---------------------------------------------------------|
| `all`                         | Complete build pipeline (clean, deps, fmt, lint, build) |
| `build`                       | Compile optimized Go binary                             |
| `debug`                       | Build with debug symbols and race detection             |
| `clean`                       | Clean build artifacts                                   |
| `check`                       | Run formatting, vetting, and linting                    |
| `docker-build-v1-sync-helper` | Build Go service container                              |
| `docker-build-meltano`        | Build Python ETL container                              |
| `docker-build-all`            | Build both containers                                   |
| `docker-run-v1-sync-helper`   | Run Go service container (requires .env)                |
| `docker-run-meltano`          | Run Meltano container (shows dragon)                    |

### Container Configuration

- **v1-sync-helper Image:** `ghcr.io/linuxfoundation/lfx-v1-sync-helper/v1-sync-helper:latest`
- **Meltano Image:** `ghcr.io/linuxfoundation/lfx-v1-sync-helper/meltano:latest`

## Container Builds

### v1-sync-helper (Go Service)
- **Multi-stage build** with Chainguard base images
- **Multi-architecture:** linux/amd64, linux/arm64
- **Security:** Non-root execution, minimal attack surface
- **Build:** `make docker-build-v1-sync-helper`

### Meltano (Python ETL)
- **uv-based dependency management** with locked dependencies
- **Multi-stage build** for optimal image size
- **ADR-0001 compliant** Python containerization
- **Build:** `make docker-build-meltano`
- **Entry:** `ENTRYPOINT ["meltano"]` with flexible command support

## Code Architecture

### Go Service (v1-sync-helper)

#### Key Implementation Patterns
1. **KV Bucket Watcher**: Watches NATS KV bucket instead of direct message consumption
2. **Direct API Calls**: Routes data via LFX One API services
3. **JWT Authentication**: Reuses Heimdall's signing key for secure API calls
4. **Mapping Storage**: Maintains v1-to-v2 ID mappings in NATS KV

#### User Impersonation Logic
- **Machine Users** (`@clients` suffix): Principal `{client_id}@clients`, Subject `{client_id}`
- **Regular Users**: Lookup via LFX v1 User Service API, 6-hour cache with 10-minute refresh
- **Fallback**: `v1_sync_helper@clients` when lookup fails

#### Scaling Architecture
- **JetStream Pull Consumer** with delivery groups for horizontal scaling
- **Non-ephemeral consumer** for reliability
- **Load-balanced message processing** across instances

#### Data Format Handling
- **Automatic Detection**: Tries MessagePack first, falls back to JSON
- **Backward Compatible**: Can read both JSON and MessagePack encoded data
- **Format Agnostic**: Processing logic unchanged regardless of encoding format

#### v1-mappings Store (`mapping_store*.go`) — LFXV2-2985

The v1-mappings backing store is behind a `MappingStore` port so callers stay identical across the KV → Postgres migration. The store surface mirrors `jetstream.KeyValue` (Get / Put / Update / Create / Delete with revision-based optimistic concurrency), and every online call site that previously used `mappingsKV.<op>` will be routed through the package-global `mappingStore` variable.

Backends:

- **`kvMappingStore`** (`mapping_store_kv.go`) — thin adapter over the existing `jetstream.KeyValue` bucket. Translates jetstream sentinel errors (`ErrKeyNotFound`, `ErrKeyExists`, JS API code 10071 "wrong last sequence") to the port-level sentinels (`ErrKeyNotFound`, `ErrKeyExists`, `ErrRevisionMismatch`). Uses the existing `isRevisionMismatchError` helper for CAS mismatch detection.
- **`pgMappingStore`** (`mapping_store_pg.go`) — pgx over the `v1_mappings` table. Translates the KV tombstone sentinel (`[]byte("!del")`) to the `tombstoned` boolean column on write and re-materialises it on read, so `isTombstonedMapping(entry.Value)` fires identically across backends. Update uses `UPDATE ... WHERE version=$expected RETURNING version` (zero rows → `ErrRevisionMismatch`); Create uses `INSERT ... ON CONFLICT DO NOTHING RETURNING version` (zero rows → `ErrKeyExists`); Put unconditionally upserts. Every successful write draws its new `version` from a shared sequence (`v1_mappings_version_seq`, defined in `internal/schema/schema.sql`) so revisions advance monotonically across delete/recreate cycles — matching NATS KV semantics, and defending against a stale-revision CAS that would otherwise succeed after a delete-and-recreate.
- **`dualMappingStore`** (`mapping_store_dual.go`) — the safe steady state during rollout. **KV is authoritative** for reads and CAS; Postgres is a best-effort shadow written by a background worker so a diff scan can validate PG before flipping to `V1MappingsStoreModePostgres`. Every mutation acquires a per-key mutex, writes KV synchronously, and then enqueues a shadow task on a bounded FIFO queue and returns. A single mirror worker drains the queue and executes each PG op inside its own bounded child context (`defaultDualMirrorTimeout`, 5s). Callers are **never blocked by Postgres latency**, so an N-mutation event completes in O(N × KV latency) rather than O(N × mirror timeout) — degraded PG cannot push a handler past the NATS AckWait and trigger redelivery. Per-key ordering is preserved by (a) holding the per-key mutex across both KV write and enqueue, and (b) using a single worker + FIFO queue on the drain side. Backpressure: when the queue is full new tasks are dropped with an ERROR log; drift is accepted per the cutover contract. If KV fails the whole op fails and PG is untouched (so a rollback to `V1MappingsStoreModeKV` sees a consistent state). Reads never consult PG — this deliberately avoids the CAS-revision-mismatch, stale-PG-serves-old-row, and PG-delete-failure-resurrection problems inherent in a PG-primary-with-KV-fallback design. `main.go` type-asserts and calls `Close()` during graceful shutdown so pending shadow work is drained before the pod exits (bounded by `defaultDualMirrorDrainTimeout`, 5s).

Mode selection (`V1_MAPPINGS_STORE_MODE`, default `kv` while LFXV2-2985 is WIP):

- **`kv`**: adapter only, no pgxpool opened. Pre-migration behaviour bit-for-bit. **Default** until the online writer migration and cutover diff scan land.
- **`dual`**: opens the pgxpool, applies `internal/schema/schema.sql`, wraps both backends in `dualMappingStore`. KV-authoritative reads + best-effort PG shadow writes; safe to opt into once Postgres wiring is in place.
- **`postgres`**: opens the pgxpool and returns `pgMappingStore` directly. Only flip after a diff scan confirms PG matches KV row-for-row (silent stale reads are exactly what the `dual`-mode design defers to that diff).

Boot ordering (`main.go`): NATS + KV bucket handles → a KV-backed `mappingStore` is wired immediately so one-shot backfills that touch the store do not nil-deref → one-shot flags dispatch (each exits before the long-running service path) → the mode-configured `mappingStore` is wired via `initMappingStore(ctx, cfg, mappingsKV)` (which may open pgxpool + `schema.Apply`) → subscriptions. `pgPool` is a package-global closed in graceful shutdown; nil in `kv` mode.

**Adding a new caller.** Import the sentinel errors from mapping_store.go, use `mappingStore.<op>` in place of `mappingsKV.<op>`, and switch `err == jetstream.ErrKeyNotFound` checks to `errors.Is(err, ErrKeyNotFound)`. The `entry.Value()` method call becomes the `entry.Value` field access. Most online callers were migrated in LFXV2-2985; see `lookup_handler.go` for the reference example and `handlers.go` / `lfx_v1_client.go` / `ingest_indexer.go` for the retry / cache / indexer patterns.

**Chart wiring.** The Postgres cluster is provisioned by `charts/lfx-v1-sync-helper/templates/database.yaml`, selected via `.Values.database.mode` (`external` | `database` | `cluster+database`). The app deployment forwards CNPG operator-managed `<clusterName>-app` Secret keys (or external Secret keys, or a single-key `V1_MAPPINGS_DATABASE_URL`) as `V1_MAPPINGS_*` env vars (distinct from `V1_DB_*`, which addresses the read-only Salesforce replica) and the service composes the libpq DSN in-process via `Config.ResolveV1MappingsDatabaseURL` — never as a literal env-var value, to avoid leaking the password through `kubectl describe pod`. When `database.mode=external` with no `secretName`, the deployment injects `V1_MAPPINGS_STORE_MODE=kv` automatically so a chart install without Postgres wiring still boots. Setting `.Values.app.environment.V1_MAPPINGS_STORE_MODE.value` overrides that safety fallback.

#### Committee member voting status — lfx-self-serve#4079

- **"No voting status" is normalized at both sync boundaries** (`voting_status.go`). v2 represents it as an empty status. v1 may hold `""` or a legacy `"None"`, but v1 project-service POST/PATCH only accept `Alternate Voting Rep | Observer | Voting Rep | Emeritus` (`"None"` → 422) and cannot clear the field. An omitted status gets a v1-side default.
- **v2 → v1** (`syncCommitteeMemberCreateToV1` / `syncCommitteeMemberUpdateToV1` via `v1VotingFromV2EventData`): an empty, `null` or `"None"` v2 status is sent as an explicit `VotingStatus: "Observer"` (`v1NoVotingStatus`), the only non-eligible value v1 accepts. An indexer event without a `voting` key leaves `VotingStatus` unset.
- **v1 → v2** (`mapV1DataToCommitteeMember{Create,Update}Payload`): `"None"` (case-insensitive) or empty maps to a nil `Voting`. `"Observer"` stays `"Observer"`, because it is a real v2 status. `committeeMembersEqual` compares voting status through `normalizeVotingStatus`, so nil, `""` and `"None"` are equal.
  - Exception: `updateCommitteeMember` calls `preserveUnsetVotingStatus` first. An incoming `"Observer"` against a v2 member with no voting status is dropped (nil `Voting`), because it is the `v1NoVotingStatus` that the v2 → v1 path wrote. Otherwise any later v1 edit of the member would set v2 to `"Observer"`. A v1 admin explicitly setting `"Observer"` on such a member therefore does not reach v2, which matches the reconcile classifier.
- **Forward mapping for members created in Self Serve:** `syncCommitteeMemberCreateToV1` now keys `committee_member.sfid.<sfid>` on the create response's `ID` (the `platform-community__c` record SFID, the sfid the WAL emits), not on `MemberID` (the contact SFID). It writes the `committee_member.record_sfid.<uid>` companion first. Later v1 edits and deletes of the member therefore resolve to the v2 member.
  - If the companion write fails, or `ID` is empty, it falls back to the contact-SFID forward key. The reverse mapping (`projectSFID:committeeSFID:contactSFID`) is always written.
  - The v1 PATCH/DELETE member endpoints take the contact SFID (`MemberID`) as the path id, not `ID`.
  - Members created before this change are repaired by `--reconcile-committee-member-voting`.

#### Committee public visibility — lfx-self-serve-ops#945

- **v2 → v1** (`syncCommitteeCreateToV1` / `mapV2DataToV1CommitteeUpdatePayload`): v2 `public` is sent as `PublicEnabled` and v2 `display_name` as `PublicName`. v1 project-service defaults `public_enabled` to `false` when it is omitted, so a v2-created public committee was previously stored as private in v1.
  - On update, `PublicEnabled` is sent only when the event carries `public`, so a partial or legacy event cannot flip visibility. `PublicName` is sent only when non-empty, because v1 PATCH treats an empty name as unchanged and cannot clear it. v1 clears `public_name` itself when `PublicEnabled` is `false`, and fills it with the committee name when `PublicEnabled` is `true` and no name is sent.
  - This only affects future writes. Committees already out of sync need a separate, per-record repair that decides the source of truth from the last deliberate edit, not from timestamps (the sync client rewrites `lastmodifieddate` on both sides).

### Python ETL (Meltano)

#### Configuration Structure
- **`meltano.yml`**: Main project configuration
- **Environment-specific settings**: dev, staging, prod
- **Custom target plugin**: `load/target-nats-kv/` for NATS KV integration

#### Data Sources
- **DynamoDB**: Data extraction
- **PostgreSQL**: Projects and committees data
- **NATS KV**: Target for all extracted data

#### Data Format Support
- **JSON** (default): Standard JSON encoding for record storage
- **MessagePack**: Compact binary serialization with `msgpack: true` configuration (Meltano) or `USE_MSGPACK=true` (WAL handler)
- **Automatic Detection**: Both Go service and Python plugin automatically detect format when reading existing data

## CI/CD Integration

### GitHub Actions Workflows

1. **publish-main.yaml**: Builds on main branch push
   - Uses `ko` for efficient Go v1-sync-helper builds
   - Multi-architecture support (linux/amd64, linux/arm64)
   - SBOM generation
   - Tags: `{commit-sha}`, `development`

2. **publish-release.yaml**: Tagged release builds
   - **publish-v1-sync-helper**: Go service build using ko
   - **publish-meltano**: Python/Meltano Docker build (depends on v1-sync-helper)
   - **release-helm-chart**: Helm chart publishing (depends on both containers)
   - **create-ghcr-helm-provenance**: SLSA provenance for Helm chart
   - **create-meltano-provenance**: SLSA provenance for Meltano container
   - Multi-architecture support for v1-sync-helper (linux/amd64, linux/arm64)
   - Single architecture for Meltano (linux/amd64)
   - Artifact signing with Cosign
   - Complete SLSA provenance generation
   - Sequential execution: v1-sync-helper → meltano → helm-chart

3. **mega-linter.yml**: Code quality enforcement
   - Cupcake flavor (Go + Python)
   - Security scanning
   - License header validation

4. **license-header-check.yml**: Copyright validation

## Development Guidelines

### Go Code Standards
- Follow standard Go conventions
- Use structured JSON logging

### Python Code Standards
- Use `uv` for dependency management
- Follow Meltano best practices
- Maintain `pyproject.toml` and `uv.lock` consistency
- Environment-based configuration

### Data Serialization
- **target-nats-kv** supports both JSON and MessagePack encoding
- Set `msgpack: true` in Meltano configuration to enable MessagePack
- Set `USE_MSGPACK=true` environment variable for WAL handler to use MessagePack
- Boolean environment variables accept truthy values: "true", "yes", "t", "y", "1" (case-insensitive)
- Automatic format detection when reading existing data for compatibility
- Go service handles both formats transparently
- WAL handler respects the same encoding configuration as Meltano for consistency

### Container Standards
- Multi-stage builds for size optimization
- Non-root execution for security
- Chainguard base images when possible
- Selective file copying with .dockerignore

## Debugging and Monitoring

### Health Endpoints
- **`/livez`**: Liveness probe
- **`/readyz`**: Readiness probe with NATS connectivity check

### Logging Structure
JSON-formatted logs with consistent fields:
- `key`: KV bucket key being processed
- `operation`: KV operation type (PUT, DELETE)
- `slug`/`sfid`: Object identifiers
- `project_uid`/`committee_uid`: Generated v2 UUIDs
- `username`: Extracted from v1 `lastmodifiedbyid`

### Debug Mode
Enable with `DEBUG=true` environment variable for detailed operation logs.

## Adding New PostgreSQL Tables to Replication

When adding a new table from PostgreSQL to the `v1-objects` NATS KV replication pipeline, four places must be updated:

### 1. `meltano/meltano.yml` — Meltano backfill extractor

Add a `select` entry for the table under the `tap-postgres` extractor (use a wildcard, e.g. `myschema-mytable.*`), add the schema to `filter_schemas` if not already present, and add a `metadata` entry specifying `INCREMENTAL` replication with the appropriate replication key (`lastmodifieddate` or `systemmodstamp`).

After editing, or if the file was last written by the Meltano CLI (which uses non-standard sequence indentation), reformat it with prettier before committing:

```bash
npx prettier --write meltano/meltano.yml
```

### 2. `charts/lfx-v1-sync-helper/values.yaml` — WAL listener table filter

Add the table to `walListener.config.listener.filter.tables` with `insert`, `update`, and `delete` operations. Use only the bare table name (no schema prefix), since wal-listener filters on `item.Table` alone (e.g. `Account`, not `salesforce_b2b.Account`). Table names are case-sensitive.

### 3. PostgreSQL `wal-listener` publication — per-environment, ad hoc

The `wal-listener` publication on the PostgreSQL server is managed manually and must be updated in each environment (dev, staging, prod) by running:

```sql
ALTER PUBLICATION "wal-listener" ADD TABLE myschema."MyTable";
```

This is not managed by Helm or any IaC — it must be applied directly against the `sfdc` database in each environment. Verify the current publication contents with:

```sql
SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'wal-listener'
ORDER BY schemaname, tablename;
```

Note: The replication slot is named `lfx_v2` (not `wal-listener`). The publication and slot names differ.

### 4. `tap-postgres-catalog` ConfigMap — per-environment, ad hoc

The PostgreSQL Meltano CronJob runs with `--catalog /catalogs/tap-postgres/catalog.json`, so `meltano.yml` changes are not picked up at runtime until the manually managed `tap-postgres-catalog` ConfigMap is regenerated in each environment.

After updating `meltano/meltano.yml`, regenerate the per-environment ConfigMap, review the generated catalog for the new stream(s), and apply it manually to the target namespace.

Repeat for staging and prod with the matching Kubernetes context, AWS profile, and PostgreSQL credentials. This ConfigMap is intentionally not managed by Helm.

### Exception: `salesforce.user_skills` is WAL-only, no Meltano seed

`user_skills` is registered in step 2 (`values.yaml`) but deliberately **not** in step 1 (`meltano/meltano.yml`): the table has no `lastmodifieddate`/`systemmodstamp` column, so Meltano's `INCREMENTAL` replication key requirement can't be satisfied, and there is no bounded `FULL_TABLE` seed for it either. WAL is the only real-time path (`handleWALUpsert` falls back to `shouldUpdateBasedOnCommitTime` for tables listed in `walTimestamplessTables`), so a KV entry only exists once a row has replicated at least once via WAL after this feature shipped. This is an accepted trade-off, not an oversight — do not add a Meltano entry for this table expecting a working replication key.

Step 3 still applies as usual: `salesforce.user_skills` must be added to the `wal-listener` publication (with `REPLICA IDENTITY FULL`, so deletes carry enough of the row for `handleWALDelete`'s fallback tombstone path) in each environment. Step 4 (`tap-postgres-catalog` regeneration) is intentionally skipped for this table, since it has no `meltano.yml` entry to produce a catalog stream from.

## One-shot Backfill Commands

### `ScanSubjectData` — stream scan abstraction for all KV enumeration (`nats_scan.go`)

All one-shot backfill and reindex jobs that need to scan a KV-backed JetStream stream use `ScanSubjectData`. It uses sequential `stream.GetMsg` calls with `jetstream.WithGetMsgSubject` (`next_by_subj` API): each call is an independent NATS request-reply asking the server for the first message at seq ≥ N matching the subject filter.

**Why not an ephemeral consumer?** Both `KV_v1-objects` (54M sequences, 35.6M tombstones) and `KV_v1-mappings` (34M sequences) in prod are too large for consumer-based enumeration. A `DeliverAllPolicy` consumer streams all sequences through a single connection, saturating NATS server CPU (~357%), preventing heartbeat delivery, and causing `nats: no heartbeat received` failures.

**Signature**: `ScanSubjectData(ctx, js, streamName, subjectFilter, opTimeout) (map[string][]byte, error)`

Returns `map[string][]byte` (subject → latest payload) with LWW applied; tombstoned subjects are excluded. Callers get payloads directly — no second-pass point reads needed.

Key design decisions:
- **No consumer** — no heartbeat, no consumer lifecycle, no CPU spike from bulk streaming.
- **LWW via seq order** — messages are visited ascending; last write per subject overwrites earlier ones.
- **`ErrMsgNotFound` = end of stream** — the clean signal that no further messages match the filter.
- **Per-call deadline** — each `GetMsg` call uses `context.WithTimeout(ctx, opTimeout)`. Pass the appropriate per-call timeout: `cfg.NATSFetchMaxWait` (default 120s) for backfill scans, `cfg.ReindexNATSOpTimeout` (default 30s) for reindex scans. Avoid the SDK's 5s default — it is too short for in-cluster use.
- **Wildcard subjects** — the `next_by_subj` NATS server API accepts NATS subject wildcards.

**Streaming variant — `ScanSubjectDataStreamRange(ctx, js, streamName, subjectFilter, startSeq, endSeq, opTimeout, cb) (visits int, tombstoned int, err error)`**

Used only for jobs whose full result set is too large to fit in memory as a `map[string][]byte` (currently just `--backfill-v1-mappings-to-postgres`; the KV_v1-mappings bucket is ~5.8 GiB / ~38M subjects in prod). Key differences from `ScanSubjectData`:

- **Callback-based** — visits are delivered to `cb(subject, data, seq, deleted)` as they are read; nothing is retained in-scanner. Memory is O(1) per scanner.
- **No LWW resolved in-scanner** — the sink receives every visit including DEL/PURGE, tagged with the JetStream seq and a `deleted` flag. Callers implement LWW via `DISTINCT ON (subject) ORDER BY seq DESC` (or equivalent).
- **Half-open [startSeq, endSeq) range** — lets callers partition [1, `maxSeq`] across N concurrent workers for wall-clock parallelism. `endSeq=0` means unbounded (scan to end).
- Use `ScanSubjectData` when the result set fits comfortably in memory; use `ScanSubjectDataStreamRange` when it does not or when you want to parallelise across the sequence space.

### Auth0 Management API enumeration — pattern for user-centric backfills (`backfill_email_profile.go`)

Backfills whose outer loop is over **Auth0 users** (rather than NATS KV keys) use the Auth0 Management API `Search()` call with a Lucene query instead of `EnumerateLiveSubjects`. Use this pattern when the canonical source of iteration is the Auth0 user set, not a NATS KV bucket.

Key design decisions:
- **Connection filter**: `Username-Password-Authentication` only — social/enterprise connections are not v1 platform accounts.
- **Sort**: `updated_at:1` (ascending) so the cursor advances monotonically and recent changes are processed last.
- **Resumable cursor**: the `updated_at` value of the last processed user is stored as a plain string in the `v1-mappings` KV bucket (e.g. `backfill.alternate-emails.cursor`). On the next run an inclusive range query `[cursor TO *]` re-processes the last user from the previous run; all per-user operations are idempotent.
- **`--limit N`**: caps Auth0 users fetched per run (default 1000). Multiple runs advance the cursor through the full user population.
- **No `EnumerateLiveSubjects`**: NATS KV is used for side-lookups (e.g. fetching the v1 SFID mapping) and cursor storage, not as the enumeration source.

### `--backfill-acs-project [--dry-run]` (`ingest_acs_project.go`)

The `--backfill-acs-project` flag runs the project grants pass (`backfillACSProjectGrants`), which backfills ACS legacy user grants into v2 project settings:

- **SFID source**: `ScanSubjectData` on `KV_v1-mappings` with filter `$KV.v1-mappings.project.sfid.*`. Returns subject → payload map directly; no separate point reads.
- **ACS query**: `GET /acs/v1/api/grantusers?object_type=project&object_id={sfid}&rolename=admin,viewer,meetings-coordinator` (paginated). `admin` → `Writers`; `viewer` → `Auditors`; `meetings-coordinator` → `MeetingCoordinators`.
- **Settings API**: `GetProjectSettings` / `UpdateProjectSettings` via Goa project-service client.
- **Merge**: additive-only; existing v2-only entries are logged as "extra" but never removed.
- **Dry-run**: add `--dry-run` to preview without writing.
- **Summary log fields**: `processed`, `errors`.
- **Manifest**: `manifests/backfill-acs-job.yaml`.

### `--backfill-acs-org [--dry-run]` (`ingest_acs_org.go`)

Backfills ACS legacy org grants into v2 b2b_org settings:

- **SFID source**: `ScanSubjectData` on `KV_v1-objects` with filter `$KV.v1-objects.salesforce_b2b-Account.*`. Returns subject → payload map directly; each payload passed through `isLiveMemberOrgAccount` to filter deleted/non-member records.
- **UID resolution**: `sfutil.Normalize18(sfid)` — as of LFXV2-2049 the b2b_org UID is the 18-char normalized SFID. No network call.
- **ACS query**: `GET /acs/v1/api/grantusers?object_type=organization&object_id={sfid}&rolename=company-admin,viewer` (paginated). `company-admin` → `writer`; `viewer` → `auditor`.
- **Settings API**: raw HTTP `GET`/`PUT /b2b_orgs/{uid}/settings` via `client_members.go`. Requires `MEMBER_SERVICE_URL` env var.
- **Merge**: additive-only; existing v2-only entries are logged as "extra" but never removed.
- **Dry-run**: add `--dry-run` to preview without writing.
- **Summary log fields**: `orgs_total`, `orgs_changed`, `writers_added`, `auditors_added`, `orgs_skipped`, `errors`.

### `--backfill-projects [flags] [--dry-run]` (`backfill_projects.go`)

Re-emits v1 `salesforce-project__c` rows that have no live `project.sfid.*` v1-mappings entry (LFXV2-3220 removed the project allowlist, so projects created before 17 Aug 2026 and never modified since were never synced). Mechanism: re-**PUT** the existing `v1-objects` value; no create logic is duplicated here — the running deployment's durable KV consumer (`handleKVPut` → `handleProjectUpdate`) does the actual create.

**Why depth ordering is required and retries don't help**: `handleKVPut` unconditionally ACKs `salesforce-project__c` messages, so the NAK/redelivery path never fires for projects, and `mapV1DataToProjectCreatePayload` hard-errors (dropping the message) when either a project's `parent_project__c` mapping or its `parent_entity_relationship__c` (legal parent) mapping is missing. A flat re-emit of every unmapped project therefore silently fails for any candidate whose project or legal parent is itself unmapped. `orderCandidatesByDepth` re-emits level by level (depth 0 = no parents or both parents already mapped), rate-limited, then settle-polls `project.sfid.<sfid>` before emitting the next level — the emit loop itself re-checks both dependencies against the settled set before emitting each candidate, so a parent that never settles blocks its descendants (counted as `skipped_parent_blocked`) instead of hard-erroring downstream.

- **Candidate source**: `ScanSubjectData` on `KV_v1-objects` (`$KV.v1-objects.salesforce-project__c.*`) minus live and tombstoned `project.sfid.*` mappings (`KV_v1-mappings`).
- **Pre-filters** mirror `handleKVPut`'s exact order (empty value, decode failure, soft-delete, `shouldSkipSync`, missing `sfid`/`name`/`slug__c`) so the pass never counts or emits a row the live consumer would drop. `slug__c` is lowercased into `projectCandidate.slug` to match `mapV1DataToProjectCreatePayload`'s own canonicalization, so `--check-slugs` conflict lookups and duplicate-slug detection compare against the slug a create would actually use.
- **Safety floor**: on a live run, aborts unless `--force` if live+tombstoned mappings fall below ~1000 (prod has ~12K) — guards against a truncated scan or a stale mappings bucket mass-re-emitting the entire v1 project population. Dry-run never aborts on this; it reports the condition via `low_mappings_count` so an operator can validate counts in a smaller environment.
- **Formation split**: `stage_histogram`/`formation_candidates` are computed before the stage-prefix filter in `selectProjectCandidates`, so a run that always excludes Formation (e.g. the shipped manifest) still reports how many Formation projects exist. A live run without `--allow-formation` refuses to start if the *filtered* candidate set contains any Formation-staged project, unless the run was explicitly scoped with `--exclude-stage-prefix`/`--include-stage-prefix` *and* the resulting set is not mixed — an unrelated filter (e.g. `--exclude-stage-prefix Draft`) does not count as scoping Formation out, and an entirely unscoped run whose candidates happen to be all Formation is rejected too, forcing two explicit passes (`--exclude-stage-prefix Formation`, then `--include-stage-prefix Formation`) rather than mixing populations or bulk-emitting Formation projects unintentionally. These two passes alone are not always sufficient: a non-Formation child of a Formation-staged parent is excluded from candidates in the parent's pass (`skipped_stage_filtered`) but keeps its parent as an unresolved dependency in the exclude-Formation pass, so it lands in `remaining_unmapped` there instead of silently succeeding. If `remaining_unmapped` is still nonzero after the exclude-Formation pass, re-run that same pass once more after the Formation-only pass completes — the parent will now be a live mapping and the child will resolve at level 0.
- **Emit**: fresh `v1KV.Get` → re-apply pre-filters → revision-checked `v1KV.Update`; a revision mismatch (`isRevisionMismatchError`) counts as `skipped_revision_race`, not an error — the live path already handled it. Every level's emitted SFIDs are settle-polled even if the per-run `--limit` was hit partway through that level (otherwise those successful creates would be double-counted as unmapped); an emission that never settles (downstream create failure or a genuine settle-poll timeout) increments `skipped_settle_timeout` and causes the run to return a non-nil error, distinct from the expected/non-failure `skipped_limit` and `skipped_revision_race` outcomes. A `reEmitV1Object` result of `already_mapped` marks the SFID settled immediately (its own mapping lookup just confirmed a live mapping), and `revision_race` is settle-polled like a normal emission rather than left permanently unresolved, since either case can otherwise strand a child in `skipped_parent_blocked` for a parent that is actually (or about to be) mapped.
- **`settlePoll`**: bounds every `mappingsKV.Get` in a polling round to the overall settle deadline via a child context, so a NATS outage can't turn thousands of sequential unbounded lookup timeouts into a multi-hour stall of the nominal two-minute settle phase.
- **Flags**: `--exclude-stage-prefix`, `--include-stage-prefix` (comma-separated, case-insensitive `project_status__c` prefix match), `--emit-rate` (default 2.0/s), `--allow-formation`, `--check-slugs` (skip candidates whose slug already resolves to a v2 project — a lost-mapping case that needs repair, not a create; a transport error during the lookup fails the whole run rather than silently treating the slug as free), `--force`. Reuses `--dry-run` and `--limit`, defaulting `--limit` to unlimited (0) unless explicitly passed, since the acceptance criterion is that every unmapped v1 project is created in one run — this differs from the other one-shot flags, which default `--limit` to 1000.
- **Dry-run**: validates candidate selection and depth ordering only; it cannot observe the downstream create. Reports the full candidate/skip counters, the stage histogram, and duplicate/conflicting slugs.
- **Summary log fields**: `scanned`, `mappings_live`, `mappings_tombstoned`, `low_mappings_count`, `candidates`, `emitted`, `levels`, `formation_candidates`, `remaining_unmapped`, `errors`, plus the full `skipped_*` breakdown (`skipped_already_mapped`, `skipped_tombstoned`, `skipped_soft_deleted`, `skipped_empty_value`, `skipped_undecodable`, `skipped_no_sfid`, `skipped_missing_required`, `skipped_v2_authored`, `skipped_stage_filtered`, `skipped_parent_unmapped`, `skipped_parent_blocked`, `skipped_revision_race`, `skipped_limit`, `skipped_slug_conflict`, `skipped_missing`, `skipped_settle_timeout`), `slug_conflicts`, `duplicate_slugs`, and `stage_histogram` (the same fields are shared by the dry-run report and the final summary via `backfillProjectsResult.logFields()`). `remaining_unmapped` is recomputed from final settle state at the end of a live run, so it reflects candidates dropped by the limit, emit errors, revision races, or a blocked dependency — not just the structurally-unresolved count seeded before emission.
- **Manifest**: `manifests/backfill-projects-job.yaml`, shipped dry-run by default with `--exclude-stage-prefix Formation --check-slugs`. `--check-slugs` is always on in the shipped Job: without it, a project created successfully but whose mapping write then fails (`handleProjectUpdate` only logs a `mappingStore.Put` failure, it never retries) would look unmapped to a rerun and be re-emitted as a duplicate create.

### `--backfill-committees [flags] [--dry-run]` (`backfill_committees.go`)

Re-emits v1 `platform-collaboration__c` rows that have no live `committee.sfid.*` v1-mappings entry. Mechanism: re-**PUT** the existing `v1-objects` value; no create logic is duplicated here — the running deployment's durable KV consumer (`handleKVPut` → `handleCommitteeUpdate`) does the actual create.

**Hierarchy limitation:** This command only checks that the owning project (`project_name__c` → `project.sfid.*`) is mapped; it does **not** depth-order committees by their `committee_id` parent.
Live committee creation also requires a mapped parent committee, so a child emitted before its parent is skipped (ACK, no redelivery).
Do not use this command to repair parent links on mapped committees or assume it can create an unmapped hierarchy in one pass.
`scripts/detect_committee_parent_drift.py` compares exported v1/v2/mapping snapshots without writing either system; conflicts require an explicit source-of-truth decision before reconciliation.

- **Candidate source**: `ScanSubjectData` on `KV_v1-objects` (`$KV.v1-objects.platform-collaboration__c.*`) minus live and tombstoned `committee.sfid.*` mappings (`KV_v1-mappings`), further filtered to candidates whose parent `project.sfid.<project_name__c>` mapping is already live.
- **Pre-filters** mirror `handleKVPut`'s exact order (empty value, decode failure, soft-delete, `shouldSkipSync`, missing `mailing_list__c`/`project_name__c`) so the pass never counts or emits a row the live consumer would drop.
- **Safety floor**: on a live run, aborts unless `--force` if live+tombstoned committee mappings fall below `minCommitteeMappingsSafetyFloor` (100) — guards against a truncated scan or a stale mappings bucket mass-re-emitting the entire v1 committee population. Dry-run never aborts on this; it reports the condition via `low_mappings_count`.
- **Emit**: fresh `v1KV.Get` → re-apply pre-filters → re-check parent-project-mapping liveness → re-check the forward mapping → re-check `--check-committee-names` (if enabled) → revision-checked `v1KV.Update`; a revision mismatch (`isRevisionMismatchError`) counts as `skipped_revision_race`, not an error, and is still settle-polled (rather than left permanently unresolved) since the concurrent writer that won may itself be creating the mapping. The forward-mapping check runs *before* the name-conflict check: a mapping that appears mid-run (e.g. a concurrent writer completing its own create-and-map sequence for this SFID) must be classified `"already_mapped"` — which the caller marks settled — rather than `"name_conflict"`, which does not, and would otherwise strand an already-resolved committee in `remaining_unmapped` and misreport it as needing a mapping repair.
- **`settleCommitteePoll`**: bounds every `mappingsKV.Get` in a polling round to the overall settle deadline via a child context, the same guard `settlePoll` applies for projects.
- **`--check-committee-names`**: skips candidates whose project UID + name already resolves to a v2 committee UID via NATS `lfx.committee-api.name_to_uid` (implemented in `lfx-v2-committee-service` PR #209) — a lost-mapping case that needs repair, not a create. Filtered once in `filterCommitteeNameConflicts` right after candidate selection, and re-checked fresh inside `reEmitCommitteeV1Object` before emit, mirroring `--check-slugs`'s two-layer defense in `--backfill-projects` — a long rate-limited run can see a name resolve to a v2 committee between scan time and emit time, including one created by an earlier candidate in the same run. A transport/lookup error propagates as a hard error rather than silently falling through as "no conflict" (`errCommitteeNameNotFound`, from `nats_client.go`, is the only recognized confirmed-miss). The v1 project SFID is resolved to its v2 project UID via `projectUIDForSFID` (`mappingsKV.Get` on `project.sfid.<sfid>`, same key prefix as `--backfill-projects`) before the name lookup. Conflicts are recorded in `res.nameConflicts` (`committeeNameConflict{sfid, name, projectSFID, committeeUID}`) and counted via `skipped_name_conflict`.
- **Duplicate-name guard**: after candidate selection (and `--check-committee-names` filtering, if enabled), candidates are grouped by `(projectSFID, name)` via `committeeNamesBySFID`; any group with more than one SFID is recorded in `res.duplicateNames`, and a live run refuses to proceed (`validateNoDuplicateCommitteeNames`) if any exist. This mirrors `--backfill-projects`'s `validateNoDuplicateSlugs`: the v2 committee service enforces project+name uniqueness, so two *unmapped* candidates sharing a pair within the same run would let one succeed while its sibling is rejected and never settles. Dry-run reports `duplicate_names` (count of colliding groups) without aborting.
- **Flags**: `--emit-rate` (default 2.0/s, shared flag with `--backfill-projects`), `--force` (shared flag), `--check-committee-names`. Reuses `--dry-run` and `--limit`, defaulting `--limit` to unlimited (0) unless explicitly passed, matching `--backfill-projects`'s rationale that the acceptance criterion is every unmapped v1 committee created in one run.
- **Dry-run**: validates candidate selection only; it cannot observe the downstream create. Reports the full candidate/skip counters, including `name_conflicts` when `--check-committee-names` is passed and `duplicate_names`.
- **Summary log fields**: `scanned`, `mappings_live`, `mappings_tombstoned`, `low_mappings_count`, `candidates`, `emitted`, `remaining_unmapped`, `errors`, plus the `skipped_*` breakdown (`skipped_already_mapped`, `skipped_tombstoned`, `skipped_soft_deleted`, `skipped_empty_value`, `skipped_undecodable`, `skipped_no_sfid`, `skipped_missing_required`, `skipped_v2_authored`, `skipped_parent_unmapped`, `skipped_revision_race`, `skipped_limit`, `skipped_missing`, `skipped_settle_timeout`, `skipped_name_conflict`), `name_conflicts` (count of `res.nameConflicts`), and `duplicate_names` (count of `res.duplicateNames`) (shared via `backfillCommitteesResult.logFields()`). `remaining_unmapped` is recomputed from final settle state at the end of a live run.
- **Manifest**: `manifests/backfill-committees-job.yaml`, shipped dry-run by default with `--check-committee-names`. `--check-committee-names` is always on in the shipped Job for the same reason `--check-slugs` is always on for projects: a committee created successfully but whose mapping write then silently fails would look unmapped to a rerun and be re-emitted as a duplicate create. **Deployment-order prerequisite**: since `--check-committee-names` is always on, this manifest must not be applied until `lfx-v2-committee-service` PR #209 (which adds the `lfx.committee-api.name_to_uid` subject the check depends on) is merged and deployed to the target environment — otherwise every lookup gets "no responders" and the job (including a dry run) hard-errors before reporting any counters. This is an operational ordering note, not a code gate.

### `--reconcile-committee-member-voting [--dry-run]` (`reconcile_committee_member_voting.go`)

Reconciles committee member voting status between v2 (Self Serve) and v1 (lfx-self-serve#4079). Vote eligibility is decided from the v1 roster: itx-service-voting's default filter is "Voting Rep" only. So a member whose v1 status drifted from the Self Serve roster can be invited to, or left out of, votes. **v2 wins.**

- **Sources:** five `ScanSubjectData` scans across three KV streams, each with `cfg.NATSFetchMaxWait`:
  - `KV_committee-members` (`$KV.committee-members.*`, skipping `lookup/` keys);
  - `KV_v1-mappings` for `committee_member.uid.*` (reverse), `committee_member.record_sfid.*` (companion) and `committee_member.sfid.*` (forward);
  - `KV_v1-objects` (`$KV.v1-objects.platform-community__c.*`, soft-deleted rows excluded).
- **Join:** member UID → reverse mapping → `collaboration_name__c + ":" + contact_name__c`. Legacy reverse mappings that store the record SFID join on the record key instead. More than one live v1 record for the same committee + contact is counted as `ambiguous_v1_record` and skipped, on both join paths, because the PATCH takes the contact SFID and cannot target one of the duplicates. A member whose `committee_member.record_sfid.<uid>` companion names a different record than the one the join resolved (e.g. the v1 row was recreated) is counted as `companion_mismatch`, logged with both record SFIDs, and skipped: claiming the joined record's forward key would split it from the companion, so a later v2 delete would tombstone the wrong key. These members need a manual mapping repair.
- **Classifier** (`classifyVotingReconcile`): an empty v2 status matches a v1 `""`, `"None"` or `"Observer"`; any other v1 value is a mismatch with target `"Observer"`. A non-empty v2 status must equal v1 (case-insensitive), otherwise the target is the v2 status.
- **Live run:** each mismatch is paced by `--emit-rate` (default 2/s). Each member's mapping backfill, re-reads and PATCH share one `reconcileMemberTimeout` (2m) context, so a hung gateway call counts as `errored` and the run moves on.
  - Only the v2 member that owns the v1 record is PATCHed. Before the PATCH, the live record-keyed forward mapping `committee_member.sfid.<recordSFID>` must name this member: `committeeUID:memberUID`, or the legacy bare `memberUID` that `handleCommitteeMemberUpdate` still accepts (left as is, never upgraded here). A key owned by another member or tombstoned counts as `mapping_conflicts`, an absent key counts as `no_mapping`, and nothing is written. If the mapping backfill could not claim the forward key, the PATCH is skipped as well. Otherwise a member with no voting status that joins to another member's record would set that member's `"Voting Rep"` to `"Observer"`.
  - Before writing, the live v2 member (`fetchCommitteeMember`) and v1 record (`getV1ObjectData`) are re-read. If they now agree, the write is skipped and counted as `skipped_now_equal`.
  - Otherwise v1 gets `PATCH {VotingStatus: target}` on the contact-SFID path. The WAL echo is dropped by `shouldSkipSync`.
- **Mapping backfill (live run only):** a member is selected (`mappings_missing`) when its `committee_member.record_sfid.<uid>` companion is missing, or when the companion is present but the record-keyed forward mapping `committee_member.sfid.<recordSFID>` is absent (a partial write). A tombstoned forward key is left alone.
  - The live reverse mapping is re-read first. If it was removed or changed since the scan (e.g. the member was deleted in v2), nothing is written and the member counts as `no_mapping`.
  - The forward mapping (`committeeUID:memberUID`) is written first, then the companion if missing. Writes use `mappingStore.Create`, so an existing key is never overwritten. An existing forward key that already names this member (including the legacy bare `memberUID`) counts as claimed; any other value counts as `mapping_conflicts`. Create on the forward key is the ownership claim: if it conflicts or fails, no companion is written, so a later v2 delete cannot tombstone another member's forward mapping. A failed companion write is retried by the next run. A conflicting or failed forward write also skips that member's voting PATCH.
  - A pre-#4079 contact-SFID forward key is left in place.
- **Exit status:** the Job exits 1 when `errored > 0` (dry or live run), even though per-member failures do not stop the run. An undecodable `platform-community__c` value counts in both `v1_objects_undecodable` and `errored` and logs its record SFID, because the member it belongs to would otherwise be reported as `no_v1_record` without being checked.
- **Dry-run:** reports every mismatch and missing mapping, logging member UID, record SFID, both statuses and the target. It writes nothing.
- **Summary log fields** (`reconcileCommitteeMemberVotingResult.logFields()`): `dry_run`, `inspected`, `no_mapping`, `poisoned_mapping`, `no_v1_record`, `ambiguous_v1_record`, `companion_mismatch`, `v1_objects_undecodable`, `matched`, `mismatched`, `patched`, `skipped_now_equal`, `mappings_missing`, `mappings_backfilled` (mapping keys created), `mapping_conflicts`, `errored`.
- **Manifest:** `manifests/reconcile-committee-member-voting-job.yaml`, shipped with `--dry-run`. It sets `LFX_API_GW` explicitly, because the default is the dev gateway. The job loads the full config and the JWT, Goa and v1 API clients, but skips the v1 platform database and Auth0 Management clients (`main.go`), so the manifest carries no `V1_DB_*` credentials. Expected sequence: dry run, live run, then a second dry run that should report `mismatched=0`.

### `--sync-user <username> [--dry-run]` (`backfill_email_profile.go`)

Performs a full sync (profile + alternate emails) for a single user identified by their Auth0 username. Useful for debugging or targeted re-sync without a full backfill run.

> **The username is not the part after `auth0|`.** An Auth0 `user_id` of the form `auth0|<suffix>` is minted by the LDAP REST Proxy (the custom component fronting LDAP and Drupal for Auth0), which sanitizes the LDAP uid so the resulting identifier is within Auth0's spec. For uids that need no sanitizing the suffix happens to equal the username, which makes the two look interchangeable — but any uid that did need sanitizing (for example one containing a space, or a UTF-8 symbol, which may be present from historical, less-conservative signup requirements) is replaced by an opaque hash. Deriving a username by stripping the `auth0|` prefix has never been safe.
>
> When a username must be recovered from Auth0 logs, read the `user_name` field, which carries the real uid.

### `--sync-users-file <path> [--dry-run]` (`backfill_email_profile.go`)

Runs `syncSingleUser` for each username listed in a newline-delimited file (blank lines and `#`-prefixed lines are ignored), reusing the same authenticated clients for the whole batch. Paced by the shared `auth0RateLimiter` (see `--auth0-rate`). A per-user failure is logged and counted but does not abort the batch.

- **Summary log fields**: `processed`, `succeeded`, `failed`.
- **LFXV2-1507**: this is the targeted-cohort replacement for the retired Auth0-cursor-driven "all users" backfills (`--backfill-alternate-emails` / `--backfill-profiles`, removed). Those walked the *entire* Auth0 population via a persisted `updated_at` cursor in `v1-mappings`; `--sync-users-file` instead takes an explicit username list (e.g. a wave or chunk of affected users identified ad hoc), which is both faster (skips users with nothing to sync) and avoids depending on a v1-mappings cursor across the ongoing KV→Postgres migration (LFXV2-2985). There is intentionally no equivalent full-population walk anymore — operators are expected to build the username list themselves (e.g. from a query against the affected population) and drive it through this flag.

### `--backfill-v1-mappings-to-postgres [--dry-run]` (`backfill_v1_mappings_pg.go`)

One-shot copy of the entire `v1-mappings` NATS KV bucket into the Postgres `v1_mappings` table. First step of the LFXV2-2985 migration off NATS KV; runs before the online dual-write / cutover paths land.

Designed for the prod scale (~5.8 GiB / ~38M subjects). Streaming scan + parallel scanners + `COPY` into a staging table + single DISTINCT ON upsert. Wall-clock is dominated by NATS `next_by_subj` round-trip time and scales roughly with `maxSeq / workers`; measure on a representative snapshot before sizing job timeouts. The `elapsed` field in the summary log records actuals.

- **Config**: uses `LoadMinimalConfig()` (NATS + timeouts only) plus Postgres env: `V1_MAPPINGS_DATABASE_URL` OR the composed set (`V1_MAPPINGS_PGHOST`, `V1_MAPPINGS_PGPORT`, `V1_MAPPINGS_PGUSER`, `V1_MAPPINGS_PGPASSWORD`, `V1_MAPPINGS_PGDATABASE`). Password composition uses `url.UserPassword` to percent-encode special characters and to avoid embedding the CNPG-generated password as a literal substring in the pod spec.
- **Tuning env**: `BACKFILL_V1_MAPPINGS_WORKERS` (default 8, clamped [1, 64]) sets scanner concurrency; `BACKFILL_V1_MAPPINGS_BATCH_SIZE` (default 50000) sets rows-per-COPY-flush. Higher worker counts trade wall-clock for NATS server CPU — the prod incident that motivated this ticket showed the server saturates around 350% CPU with consumer-based enumeration, so leave headroom.
- **Schema**: `internal/schema/schema.sql` is embedded and applied at pool-init time via `schema.Apply` — pods bootstrap the table idempotently under a `pg_advisory_xact_lock` + `SET LOCAL statement_timeout='60s'` guard, mirroring lfx-v2-newsletter-service.
- **KV scan**: `ScanSubjectDataStreamRange` on `KV_v1-mappings` with filter `$KV.v1-mappings.>` and per-op timeout `cfg.NATSFetchMaxWait` (default 120s). The stream's [1, `maxSeq`] sequence space is partitioned into N disjoint half-open ranges; each worker drives an independent `next_by_subj` scan and streams visits through a shared channel to the writer goroutine. LWW is resolved in Postgres, not in-scanner, so no `map[string][]byte` snapshot is ever built in memory.
- **Tombstones**: app-level `!del` sentinel PUTs → written as `tombstoned=true, mapping_value=''`; native NATS DEL/PURGE → carried through as `deleted=true` rows in staging and excluded from the final table via `WHERE NOT deleted` on the DISTINCT ON winner.
- **Postgres load**: staging table `v1_mappings_staging` is `UNLOGGED` (no WAL) and dropped-and-recreated per run. Writer streams batches through `pgx.CopyFrom` (a single wire round-trip per batch instead of per-row parse+plan; typically materially faster than batched INSERTs, but the exact ratio is workload-dependent). After scanners finish, one `INSERT ... SELECT DISTINCT ON (mapping_key) ... FROM staging WHERE NOT deleted ORDER BY mapping_key, seq DESC ON CONFLICT DO UPDATE` resolves LWW into `v1_mappings`.
- **Dry-run**: `--dry-run` skips staging creation, `CopyFrom`, and the final upsert but preserves all scan/classification counters so operators can validate row counts before writing.
- **Summary log fields**: `visits`, `live`, `tombstoned`, `empty`, `native_del`, `staged`, `inserted_rows`, `batches`, `workers`, `max_seq`, `elapsed`, `dry_run`.
- **Idempotency caveat**: on re-runs against an already-populated `v1_mappings`, subjects whose latest KV revision is a native NATS DEL/PURGE will NOT be removed from Postgres (the WHERE NOT deleted filter excludes them from the INSERT but does not issue a DELETE). This is safe for the initial cutover (empty target) and for the pending online mutation path (LFXV2-2985 follow-ups) where the online DELETE handler owns removals.
- **Manifest**: `manifests/backfill-v1-mappings-to-postgres-job.yaml`.

### 4. `cmd/lfx-v1-sync-helper/handlers.go` — suppress unknown-object warnings (optional)

If the new table's records should only be stored in KV for downstream consumption (no v2 API side-effects), add the key prefix (e.g. `"myschema-mytable"`) as an explicit `case` in both `handleKVPut` and `handleResourceDelete` with a debug-level log statement. This prevents spurious "unknown object type" warnings in the logs.

## Contributing Workflow

1. **Code Changes**: Follow language-specific standards
2. **Formatting**: Use `make check` for Go code formatting
3. **Container Builds**: Test with `make docker-build-all`
4. **CI Validation**: Ensure MegaLinter and license checks pass

This documentation focuses specifically on the technical aspects needed for codebase development and modification.
