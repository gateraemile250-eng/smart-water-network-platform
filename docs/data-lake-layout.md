# S3 Data Lake Layout

**Status:** The bucket exists and its security settings are verified.
**No data has been published yet.** Publishing starts in Phase 4.

## 1. Layout

```text
s3://<bucket-name>/
├── raw/
│   └── battledim/2018/              source files, unchanged
├── processed/
│   └── sensor_readings/             typed, de-duplicated Parquet
│       └── year=2018/month=01/ ... month=12/
├── curated/
│   ├── anomaly_results/             from PostgreSQL
│   ├── localization_results/        from PostgreSQL
│   └── reference/
│       ├── sensors/
│       ├── network_nodes/
│       └── network_links/
└── athena-results/                  Athena query output
```

The real bucket name is account-specific and is never committed. Scripts
read it from configuration.

S3 has no real folders. Prefixes come into existence when the first
object is written, so nothing is created in advance.

## 2. Zones

| Zone | Purpose | Format | Queried by Athena |
|---|---|---|---|
| `raw/` | Immutable archive of source files exactly as received | Original CSV, TXT and INP files | **No.** It is never cataloged, so it can never cause scan costs. |
| `processed/` | Validated, typed, de-duplicated readings | Parquet (Snappy) | Yes |
| `curated/` | Analytics-ready tables and reference dimensions | Parquet | Yes |
| `athena-results/` | Query result files written by Athena | Athena default | No |

## 3. Conventions

- One table per prefix. A prefix never mixes files from different
  tables, because Athena reads **every** file under a table's location.
- Names are lowercase `snake_case`.
- Partitions use Hive style, for example `year=2018/month=01`.
- Nothing is written to the bucket root.

## 4. Planned tables

| Prefix | Source | Approximate rows | Partitioning |
|---|---|---|---|
| `processed/sensor_readings/` | Full-year BattLeDIM SCADA data, converted to long format | about 12.5 million | `year`, `month` |
| `curated/anomaly_results/` | PostgreSQL `network_anomaly_results` | 101,088 | None |
| `curated/localization_results/` | PostgreSQL `network_localization_results` | 121,700 | None |
| `curated/reference/sensors/` | PostgreSQL `sensors` | 119 | None |
| `curated/reference/network_nodes/` | PostgreSQL `network_nodes` | 785 | None |
| `curated/reference/network_links/` | PostgreSQL `network_links` | 909 | None |

## 5. Partition decision

`processed/sensor_readings` is partitioned by `year` and `month`, which
gives 12 partitions.

- **Why month:** analytics filter by time period, and a month is the
  natural boundary for hourly and daily demand analysis.
- **Why not finer:** daily partitions would create about 365 very small
  files, and per-sensor partitions about 119. Many small files make
  Athena slower and are a well-known anti-pattern.
- **Why `year` is included even though all data is from 2018:** the
  layout stays valid if more years are added.
- **Curated tables are not partitioned.** They are small.
- **Honest scope of the benefit:** the whole dataset is on the order of
  100 MB, so even a full scan costs a fraction of a cent. Partitioning is
  used because it is the correct pattern and scales, not because it saves
  meaningful money at this size.
- **Phase 5 will use partition projection,** so the Data Catalog stores
  no partition entries and no crawler is needed.

## 6. Cost controls on the bucket

| Control | Setting |
|---|---|
| Expire query results | `athena-results/` objects expire after 7 days |
| Abandoned uploads | Incomplete multipart uploads are aborted after 7 days |
| Versioning | Off, so overwritten objects do not accumulate |
| Raw zone size | About 89 MB of source files |

## 7. Security configuration (verified)

| Setting | Value |
|---|---|
| Region | Europe (Stockholm), `eu-north-1` |
| Block Public Access | All four options on |
| Object Ownership | Bucket owner enforced (ACLs disabled) |
| Default encryption | SSE-S3 (Amazon S3 managed keys), bucket key enabled |
| Versioning | Disabled |
| Bucket policy | Denies every request that does not use HTTPS |

## 8. Access model

- The CLI identity can list the bucket, read objects and write objects.
- It **cannot delete objects, change bucket policies or change
  lifecycle rules.** That is deliberate. Cleanup and configuration
  changes use the account owner's identity.
