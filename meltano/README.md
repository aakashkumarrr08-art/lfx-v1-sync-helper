# LFX One data replication

This is a [Meltano project](https://docs.meltano.com/concepts/project/) with a standard directory layout created with `uv run meltano init "meltano"`.

## Overview

This Meltano project extracts data from LFX v1 sources and loads it into NATS Key-Value stores for processing by the LFX v2 platform. It supports extracting from multiple data sources including DynamoDB and PostgreSQL (for projects and committees data).

## Important files

- `meltano.yml`: The main configuration file for the Meltano project. It defines all sources and targets supported.
- `load/target-nats-kv/`: Contains the custom target plugin for loading data into a NATS Key-Value store.

## Prerequisites

- Python 3.12+ (managed automatically by uv from the repository root)
- AWS credentials configured (for DynamoDB access)
- PostgreSQL access credentials (for projects/committees data)
- Access to target NATS server
- 1Password CLI (`op`) for retrieving secrets

## Setup

From the repository root, initialize the Python environment:

```bash
uv sync
```

Test that Meltano is working correctly:

```bash
cd meltano
uv run meltano dragon
```

Install the plugins as the release image does (no source credentials needed for
installation):

```bash
uv run meltano install tap-postgres target-nats-kv --parallelism=1
UV_OVERRIDE="$PWD/uv-overrides.txt" uv run meltano install tap-dynamodb --parallelism=1
.meltano/extractors/tap-postgres/venv/bin/tap-postgres --help
.meltano/extractors/tap-dynamodb/venv/bin/tap-dynamodb --help
```

`uv-overrides.txt` selects a Singer SDK version **only for tap-dynamodb** that
supports the repository's
`urllib3>=2.8.0` security floor. The upstream tap-dynamodb 0.5.0 still pins
`singer-sdk~=0.41.0`, which requires `urllib3<2`; remove the override only
after that upstream requirement is updated and the image build passes without it.
tap-postgres needs its own, newer Singer SDK and must be installed without
the override.

## Usage

### Copy dev v1 DynamoDB data

Extract DynamoDB data and load into NATS KV:

```bash
aws-vault exec itx-dev -- uv run meltano --environment=dev run tap-dynamodb target-nats-kv
```

For other environments, replace both the AWS account (`itx-dev`) and the Meltano environment parameter.

### Copy dev v1 Projects & Committees

Extract projects and committees data from PostgreSQL and load into NATS KV:

```bash
TAP_POSTGRES_HOST="dev-platform-database.rds.host"
TAP_POSTGRES_PASSWORD="$(op item get ...)"
export TAP_POSTGRES_HOST TAP_POSTGRES_PASSWORD
uv run meltano --environment=dev run tap-postgres target-nats-kv
```

For other environments, replace the PostgreSQL host & credentials, and the Meltano environment parameter.

## Command modifiers

Options are available that affect both the sync strategy from the source and the refresh behavior on the target. _These settings can be combined as needed._

### Full source refresh

By default, this Meltano project tracks the last-synced timestamp from each table and only performs incremental syncs. For a full sync of all data, append `--full-refresh` immediately after `run`:

```bash
# Full refresh example for DynamoDB data
aws-vault exec itx-dev -- uv run meltano --environment=dev run --full-refresh tap-dynamodb target-nats-kv

# Full refresh example for projects/committees data
uv run meltano --environment=dev run --full-refresh tap-postgres target-nats-kv
```

### Target refresh mode

By default, the NATS KV loader only writes entries if the timestamp is newer. To refresh items that haven't changed (to trigger an updated attempt to write or update them in v2 by `v1-sync-helper`), set the `TARGET_NATS_KV_REFRESH_MODE` environment variable:

```bash
# Refresh unchanged items
TARGET_NATS_KV_REFRESH_MODE=same uv run meltano --environment=dev run tap-postgres target-nats-kv
```

## Configuration

The project supports multiple environments (dev, staging, prod) configured in `meltano.yml`. Environment-specific settings can be overridden using environment variables or by modifying the configuration file.

### Environment Variables

Common environment variables that may need to be set:

#### PostgreSQL Configuration

- `TAP_POSTGRES_HOST`: PostgreSQL server hostname
- `TAP_POSTGRES_PASSWORD`: PostgreSQL password
- `TAP_POSTGRES_USER`: PostgreSQL username (default: `lfit`)
- `TAP_POSTGRES_DATABASE`: PostgreSQL database name (default: `sfdc`)

For additional PostgreSQL extractor settings, see the [tap-postgres documentation](https://hub.meltano.com/extractors/tap-postgres--meltanolabs).

#### NATS Configuration

- `TARGET_NATS_KV_URL`: NATS server URL (default: `nats://lfx-platform-nats.lfx.svc.cluster.local:4222`)
- `TARGET_NATS_KV_BUCKET`: NATS KV bucket name (default: `v1-objects`)
- `TARGET_NATS_KV_USER`: NATS username (optional)
- `TARGET_NATS_KV_PASSWORD`: NATS password (optional)
- `TARGET_NATS_KV_CREDS`: Path to NATS credentials file (optional)
- `TARGET_NATS_KV_KEY_PREFIX`: Key prefix for NATS KV operations (optional)
- `TARGET_NATS_KV_REFRESH_MODE`: Controls how the NATS KV target handles existing entries

## Adding a new DynamoDB table

See [research/adding-dynamodb-table.md](../research/adding-dynamodb-table.md) for a step-by-step guide on adding a new DynamoDB table to this pipeline, including a worked example using `surveymonkey-surveys`.
