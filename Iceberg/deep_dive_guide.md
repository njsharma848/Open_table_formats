# Apache Iceberg — Deep Dive Guide

> The open table format built for planet-scale analytics, multi-engine freedom, and true data reliability.

---

## Table of Contents

- [Why Apache Iceberg?](#why-apache-iceberg)
- [Key Features](#key-features)
- [Architecture Internals](#architecture-internals)
- [Code Examples](#code-examples)
- [Use Case Scenarios](#use-case-scenarios)
- [Pros and Cons](#pros-and-cons)
- [FAANG / MAANG Interview Questions](#faang--maang-interview-questions)

---

## Why Apache Iceberg?

Apache Iceberg was born out of **Netflix's pain** managing petabyte-scale Hive tables in production. Hive was the de facto standard — but it cracked under modern demands.

### The Problem with Hive Tables

| Problem | What Happens in Hive |
|---|---|
| **No atomicity** | A failed write leaves partial data visible to readers |
| **Partition listing is slow** | Discovering partitions requires listing millions of S3 paths |
| **Partition is exposed to users** | Queries must include partition columns or they scan everything |
| **No schema evolution** | Renaming or dropping columns breaks downstream readers |
| **No concurrent writes** | Two writers on the same table corrupt data |
| **No time travel** | A bad DELETE or OVERWRITE is permanent |
| **Statistics are inaccurate** | Hive stats go stale; the planner makes poor decisions |

### What Iceberg Brings

Iceberg is a **vendor-neutral, engine-agnostic open table format** donated to the Apache Software Foundation. It introduces a rich **metadata layer** above your Parquet/ORC/Avro files that gives any compute engine (Spark, Flink, Trino, DuckDB, Snowflake) a consistent, reliable view of the table.

```
Without Iceberg:   Object Store  ──▶  Hive partitions  ──▶  Slow, fragile, coupled

With Iceberg:      Object Store  ──▶  Parquet / ORC / Avro files
                                 ──▶  Manifest files (file-level stats)
                                 ──▶  Manifest list (snapshot index)
                                 ──▶  Metadata JSON (schema, partition spec, history)
                                 ──▶  ACID · Time Travel · Hidden Partitioning · Any Engine
```

> Originally created at **Netflix (2017)**, open-sourced and donated to Apache. Now used at Apple, Adobe, LinkedIn, Airbnb, AWS, Snowflake, and many others.

---

## Key Features

### 1. ACID Transactions

Iceberg uses **optimistic concurrency control** with atomic metadata swaps. Every write produces a new **snapshot**. Readers always see a consistent, complete snapshot — never a partial write.

```python
# Two writers append to the same table safely
# Iceberg resolves conflicts at the metadata level
df_a.writeTo("catalog.db.events").append()
df_b.writeTo("catalog.db.events").append()
# Both succeed if no row-level conflict; one retries if there is
```

---

### 2. Hidden Partitioning

In Hive, users must write `WHERE dt = '2024-01-01'` to get partition pruning. In Iceberg, **partitioning is invisible to the query writer** — Iceberg applies transforms automatically.

```python
# Define partitioning at table creation using transforms
spark.sql("""
  CREATE TABLE catalog.db.events (
    id        BIGINT,
    user_id   BIGINT,
    event_type STRING,
    ts        TIMESTAMP
  )
  USING iceberg
  PARTITIONED BY (days(ts), bucket(16, user_id))
""")

# Query writer does NOT need to know the partition scheme
# Iceberg prunes automatically using metadata
spark.sql("""
  SELECT * FROM catalog.db.events
  WHERE ts BETWEEN '2024-01-01' AND '2024-01-31'
""")
```

**Supported partition transforms:**

| Transform | Example | Description |
|---|---|---|
| `identity(col)` | `identity(country)` | Raw value partitioning |
| `days(ts)` | `days(event_ts)` | Partition by day from timestamp |
| `hours(ts)` | `hours(event_ts)` | Partition by hour |
| `months(ts)` | `months(created_at)` | Partition by month |
| `years(ts)` | `years(signup_date)` | Partition by year |
| `bucket(N, col)` | `bucket(16, user_id)` | Hash into N buckets |
| `truncate(N, col)` | `truncate(3, zip_code)` | Truncate string/int to prefix |

---

### 3. Partition Evolution

Change the partition scheme of a table **without rewriting a single byte of existing data**. New data uses the new scheme; old data stays with the old scheme. Iceberg handles both transparently.

```python
# Table currently partitioned by months(ts)
# Evolve to days(ts) for finer granularity — zero data rewrite
spark.sql("""
  ALTER TABLE catalog.db.events
  REPLACE PARTITION FIELD months(ts) WITH days(ts)
""")

# Old data: still in monthly partitions
# New data: written into daily partitions
# Queries on either window just work
```

---

### 4. Schema Evolution

Safely add, drop, rename, reorder, and widen columns. Iceberg tracks column identity by **column ID** (not name), so renames never confuse readers of old files.

```python
# Add a new column
spark.sql("ALTER TABLE catalog.db.events ADD COLUMN platform STRING")

# Rename a column (safe — tracked by ID, not name)
spark.sql("ALTER TABLE catalog.db.events RENAME COLUMN platform TO device_platform")

# Drop a column
spark.sql("ALTER TABLE catalog.db.events DROP COLUMN device_platform")

# Widen a data type (INT → BIGINT is safe)
spark.sql("ALTER TABLE catalog.db.events ALTER COLUMN user_id TYPE BIGINT")
```

---

### 5. Time Travel & Rollback

Every write creates a **snapshot**. Query any snapshot by ID or timestamp. Roll back the table to any previous snapshot.

```python
# Query at a specific snapshot ID
spark.read \
    .option("snapshot-id", 8516748052697465003) \
    .table("catalog.db.events")

# Query at a specific timestamp
spark.read \
    .option("as-of-timestamp", "2024-06-01T00:00:00") \
    .table("catalog.db.events")

# View all snapshots
spark.sql("SELECT * FROM catalog.db.events.snapshots")

# Roll back the table to a previous snapshot
spark.sql("""
  CALL catalog.system.rollback_to_snapshot(
    'db.events',
    8516748052697465003
  )
""")
```

---

### 6. Row-Level Operations (MERGE, UPDATE, DELETE)

Iceberg supports surgical row-level changes using two strategies: **Copy-on-Write (CoW)** and **Merge-on-Read (MoR)**.

```python
# MERGE — upsert pattern
spark.sql("""
  MERGE INTO catalog.db.customers AS t
  USING updates AS s
  ON t.customer_id = s.customer_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# UPDATE specific rows
spark.sql("""
  UPDATE catalog.db.customers
  SET status = 'inactive'
  WHERE last_login < '2023-01-01'
""")

# DELETE specific rows
spark.sql("""
  DELETE FROM catalog.db.customers
  WHERE country = 'XX'
""")
```

**Copy-on-Write vs Merge-on-Read:**

| Mode | Write Cost | Read Cost | Best For |
|---|---|---|---|
| Copy-on-Write (CoW) | High (rewrites files) | Low (clean Parquet) | Read-heavy tables |
| Merge-on-Read (MoR) | Low (writes delete files) | Slightly higher | Write-heavy, frequent updates |

```python
# Set the write mode per operation type
spark.sql("""
  ALTER TABLE catalog.db.events
  SET TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='copy-on-write'
  )
""")
```

---

### 7. Branching and Tagging (Git for Data)

Iceberg v2 introduces **named branches and tags** on your table history — like Git, but for data.

```python
# Create a branch for a risky ETL job
spark.sql("ALTER TABLE catalog.db.events CREATE BRANCH etl_experiment")

# Write to the branch, not main
df.writeTo("catalog.db.events.branch_etl_experiment").append()

# Promote branch to main if successful
spark.sql("""
  CALL catalog.system.fast_forward('db.events', 'main', 'etl_experiment')
""")

# Or just drop the branch if it fails
spark.sql("ALTER TABLE catalog.db.events DROP BRANCH etl_experiment")

# Tag a snapshot for compliance / auditing
spark.sql("""
  ALTER TABLE catalog.db.events
  CREATE TAG eom_jan_2024
  AS OF VERSION 42
  RETAIN 365 DAYS
""")
```

---

### 8. Incremental Read (Changelog Between Snapshots)

Read only the data that changed between two snapshots — without a full table scan.

```python
# Read rows added between snapshot 10 and snapshot 20
spark.read \
    .option("start-snapshot-id", 10) \
    .option("end-snapshot-id", 20) \
    .table("catalog.db.events")
```

---

### 9. Table Maintenance

```python
# Compact small files into larger ones
spark.sql("""
  CALL catalog.system.rewrite_data_files(
    table => 'db.events',
    strategy => 'sort',
    sort_order => 'zorder(user_id, ts)'
  )
""")

# Remove old snapshots (keep last 5 days)
spark.sql("""
  CALL catalog.system.expire_snapshots(
    table => 'db.events',
    older_than => TIMESTAMP '2024-01-20 00:00:00',
    retain_last => 5
  )
""")

# Remove orphan files not referenced by any snapshot
spark.sql("""
  CALL catalog.system.remove_orphan_files(table => 'db.events')
""")
```

---

## Architecture Internals

```
iceberg-table/
│
├── metadata/
│   ├── v1.metadata.json             ← Initial table metadata
│   ├── v2.metadata.json             ← After first write (new snapshot added)
│   ├── v3.metadata.json             ← After schema evolution
│   ├── snap-1234.avro               ← Manifest list for snapshot 1234
│   └── snap-5678.avro               ← Manifest list for snapshot 5678
│
├── data/
│   ├── country=US/
│   │   └── part-00000.parquet       ← Actual data files
│   └── country=IN/
│       └── part-00001.parquet
│
└── manifests/
    ├── manifest-abc.avro            ← Lists data files + per-file stats
    └── manifest-def.avro
```

### The Three-Layer Metadata Stack

```
┌──────────────────────────────────────────────┐
│            Metadata File (JSON)              │  ← Table schema, partition spec,
│  Current snapshot pointer, history,          │    current snapshot ID
│  sort orders, properties                     │
└──────────────────┬───────────────────────────┘
                   │ points to
                   ▼
┌──────────────────────────────────────────────┐
│           Manifest List (Avro)               │  ← One per snapshot
│  Lists all manifest files for this snapshot  │    partition-level stats for pruning
│  + partition summaries (min/max per part)    │
└──────────────────┬───────────────────────────┘
                   │ points to
                   ▼
┌──────────────────────────────────────────────┐
│           Manifest File (Avro)               │  ← One per group of data files
│  Lists data files + per-column statistics    │    (null count, min, max, row count)
│  (min, max, null_count, row_count per col)   │
└──────────────────┬───────────────────────────┘
                   │ points to
                   ▼
┌──────────────────────────────────────────────┐
│           Data Files (Parquet/ORC/Avro)      │  ← Actual columnar data
└──────────────────────────────────────────────┘
```

### How a Read Works (Query Planning)

```
1. Reader opens current metadata.json  → gets current snapshot ID
2. Opens manifest list (snap-XXXX.avro) → gets list of manifests
3. Prunes manifests using partition stats → skips irrelevant partitions
4. Opens surviving manifests            → gets list of data files
5. Prunes data files using column stats → skips files outside filter range
6. Reads only surviving data files      → minimal I/O
```

### How a Write Works (Atomic Commit)

```
1. Writer writes new Parquet data files to object store
2. Writer creates new manifest file listing the new data files
3. Writer creates new manifest list (snapshot) referencing all manifests
4. Writer atomically swaps the metadata pointer to the new metadata.json
5. Readers immediately see the new snapshot; old readers see the old one
```

---

## Code Examples

### Set Up Iceberg with PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergDemo") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-warehouse") \
    .getOrCreate()
```

---

### Create a Table and Write Data

```python
spark.sql("""
  CREATE TABLE local.db.orders (
    order_id   BIGINT,
    customer_id BIGINT,
    amount     DOUBLE,
    status     STRING,
    created_at TIMESTAMP
  )
  USING iceberg
  PARTITIONED BY (days(created_at), truncate(3, status))
""")

data = [(1, 101, 250.0, "NEW", "2024-01-15 10:00:00"),
        (2, 102, 99.5,  "NEW", "2024-01-16 11:00:00")]

df = spark.createDataFrame(data, ["order_id","customer_id","amount","status","created_at"])
df.writeTo("local.db.orders").append()
```

---

### Inspect Table Metadata

```python
# View snapshots
spark.sql("SELECT * FROM local.db.orders.snapshots").show()

# View data files in current snapshot
spark.sql("SELECT * FROM local.db.orders.files").show()

# View partition stats
spark.sql("SELECT * FROM local.db.orders.partitions").show()

# View full history
spark.sql("SELECT * FROM local.db.orders.history").show()
```

---

### Streaming Write with Flink SQL

```sql
-- Create an Iceberg table in Flink
CREATE TABLE orders (
  order_id   BIGINT,
  customer_id BIGINT,
  amount     DOUBLE,
  status     STRING,
  created_at TIMESTAMP(3)
) WITH (
  'connector'        = 'iceberg',
  'catalog-name'     = 'my_catalog',
  'catalog-type'     = 'hive',
  'warehouse'        = 's3://my-bucket/iceberg',
  'format-version'   = '2'
);

-- Stream from Kafka into Iceberg
INSERT INTO orders
SELECT * FROM kafka_orders_source;
```

---

### Query with Trino (No Spark Needed)

```sql
-- Trino reads Iceberg natively
SELECT
  date_trunc('day', created_at) AS day,
  COUNT(*) AS order_count,
  SUM(amount) AS revenue
FROM iceberg.db.orders
WHERE created_at >= DATE '2024-01-01'
GROUP BY 1
ORDER BY 1;
```

---

## Use Case Scenarios

### 1. Multi-Engine Data Lakehouse

**Scenario:** Ingestion team uses Flink, analytics team uses Trino, ML team uses Spark — all on the same table.

```
Flink  ──▶ Write streaming events   ──▶  Iceberg Table (S3)
Spark  ──▶ Run batch ETL / MERGE    ──▶       ▲
Trino  ──▶ Ad-hoc SQL analytics     ──▶       │
DuckDB ──▶ Local exploration        ──▶       │
                    All reading the SAME TABLE, SAME DATA
```

No data copies, no sync jobs. Iceberg's spec-level compatibility makes this possible.

---

### 2. Partition Evolution Without Downtime

**Scenario:** A table originally partitioned by `months(ts)` is growing too large per partition. The team wants daily partitions — without a multi-day backfill job.

```python
# Step 1: Evolve the partition spec (zero data rewrite)
spark.sql("""
  ALTER TABLE catalog.db.clickstream
  REPLACE PARTITION FIELD months(ts) WITH days(ts)
""")

# Step 2: New data automatically lands in daily partitions
# Step 3: Old data remains in monthly partitions but queries are transparent
# No downtime, no migration script, no dual-write period
```

---

### 3. Safe ETL with Branching

**Scenario:** A data engineer needs to run a complex, destructive backfill on a production table without risking data loss.

```python
# Create an isolated branch
spark.sql("ALTER TABLE catalog.db.sales CREATE BRANCH backfill_march")

# Run the risky backfill ONLY on the branch
spark.sql("""
  INSERT OVERWRITE catalog.db.sales.branch_backfill_march
  SELECT * FROM source WHERE month = '2024-03'
""")

# Validate
spark.sql("SELECT COUNT(*) FROM catalog.db.sales.branch_backfill_march").show()

# Promote to main if all good
spark.sql("CALL catalog.system.fast_forward('db.sales', 'main', 'backfill_march')")

# Or drop it if something is wrong
spark.sql("ALTER TABLE catalog.db.sales DROP BRANCH backfill_march")
```

---

### 4. Regulatory Compliance with Tagged Snapshots

**Scenario:** Financial regulations require preserving exact month-end state of all transaction tables for 7 years.

```python
# At month end, tag the current snapshot
spark.sql("""
  ALTER TABLE catalog.db.transactions
  CREATE TAG eom_2024_01
  AS OF VERSION 87
  RETAIN 2557 DAYS  -- 7 years
""")

# Auditor queries month-end state, years later
spark.read \
    .option("tag", "eom_2024_01") \
    .table("catalog.db.transactions")
```

---

### 5. GDPR Right-to-Erasure

**Scenario:** Delete a user's data across a massive partitioned table efficiently.

```python
# Delete using MoR mode — writes a delete file, no data rewrite
spark.sql("""
  DELETE FROM catalog.db.user_events
  WHERE user_id = 'abc-12345'
""")

# Later: compact and expire to physically remove data
spark.sql("""
  CALL catalog.system.rewrite_data_files(table => 'db.user_events')
""")
spark.sql("""
  CALL catalog.system.expire_snapshots(
    table => 'db.user_events',
    older_than => TIMESTAMP '2024-02-01 00:00:00'
  )
""")
```

---

### 6. ML Training with Point-in-Time Correct Features

**Scenario:** ML team needs features as they existed at a specific point in time to avoid training/serving skew.

```python
# Use snapshot at exact training cutoff — no data leakage
training_df = spark.read \
    .option("as-of-timestamp", "2024-03-01T00:00:00") \
    .table("catalog.featurestore.user_features")

model.fit(training_df)

# Tag the exact snapshot used for reproducibility
spark.sql("""
  ALTER TABLE catalog.featurestore.user_features
  CREATE TAG model_v7_training_snapshot
""")
```

---

### 7. Incremental ETL Pipeline

**Scenario:** Propagate only new/changed rows from a source Iceberg table to downstream tables — no full scans.

```python
# Get rows added since last run (snapshot 100 to current)
incremental_df = spark.read \
    .option("start-snapshot-id", 100) \
    .option("end-snapshot-id", 200) \
    .table("catalog.db.raw_events")

# Apply transformations and write downstream
incremental_df \
    .transform(enrich) \
    .writeTo("catalog.db.enriched_events") \
    .append()
```

---

## Pros and Cons

### ✅ Pros

| Advantage | Detail |
|---|---|
| **Broadest engine support** | Spark, Flink, Trino, Presto, DuckDB, Snowflake, BigQuery, Athena, StarRocks — all natively |
| **True vendor neutrality** | Apache-governed; no single company controls the spec |
| **Hidden partitioning** | Query writers never need to know or specify partition columns |
| **Partition evolution** | Change partition scheme with zero data rewrite — unique to Iceberg |
| **Full schema evolution** | Add, drop, rename, reorder columns safely using column IDs |
| **Branching & tagging** | Git-style data management for safe experimentation and compliance |
| **Rich metadata & stats** | Per-column, per-file statistics enable aggressive data skipping |
| **Multi-format support** | Parquet, ORC, and Avro as data file formats |
| **Spec-level compatibility** | Any engine reading the spec sees the same table — no proprietary extensions |
| **Cloud-native** | Works equally well on S3, GCS, ADLS, and HDFS |
| **Incremental reads** | Native changelog between any two snapshots |

---

### ❌ Cons

| Disadvantage | Detail |
|---|---|
| **More complex architecture** | Three-layer metadata (metadata → manifest list → manifest → data) has a steeper learning curve than Delta's flat log |
| **Catalog dependency** | Requires a catalog (Hive Metastore, AWS Glue, Nessie, REST) — more moving parts than Delta's self-contained log |
| **Slower adoption of new features in non-Spark engines** | Flink and Trino support lags Spark for newer Iceberg v2 features |
| **Tooling maturity** | Delta's Databricks tooling (auto-optimize, auto-compaction) is more polished out of the box |
| **Maintenance burden** | `expire_snapshots`, `rewrite_data_files`, `remove_orphan_files` must be run manually or scheduled |
| **Small-file problem still exists** | Streaming writes still produce small files; `rewrite_data_files` is manual (unlike Delta's auto-optimize) |
| **MoR read amplification** | Merge-on-Read tables require applying delete files at read time — can hurt performance without tuning |
| **Less mature CDC tooling** | No native CDF equivalent to Delta's Change Data Feed in all engines |

---

## FAANG / MAANG Interview Questions

### 🔵 Conceptual & Fundamentals

**Q1. What is the Iceberg metadata architecture? Explain the three layers.**

> Iceberg has a three-layer metadata stack above data files:
>
> 1. **Metadata file (JSON):** the entry point. Stores table schema, partition spec, current snapshot ID, snapshot history, sort orders, and table properties. Updated atomically on every commit.
> 2. **Manifest list (Avro):** one per snapshot. Lists all manifest files belonging to this snapshot, along with **partition-level summary statistics** (min/max per partition column) used to skip entire manifests.
> 3. **Manifest file (Avro):** lists individual data files with **per-column, per-file statistics** (null count, lower bound, upper bound, row count). Used for fine-grained file pruning.
>
> This layered design means a query planner can prune irrelevant data at **two levels** (partition and file) before reading a single byte of Parquet.

---

**Q2. How does Iceberg achieve ACID transactions without a lock manager?**

> Iceberg uses **optimistic concurrency control** with **atomic metadata pointer swaps**:
>
> 1. A writer reads the current metadata file pointer (stored in a catalog)
> 2. Writes new data files and a new metadata file referencing them
> 3. Attempts to atomically swap the catalog's metadata pointer to the new version (using the catalog's compare-and-swap or conditional PUT semantics)
> 4. If another writer committed between steps 1 and 3, the swap fails; the writer evaluates the conflict type and either retries or raises an exception
>
> Readers always see a complete, consistent snapshot because they only ever follow the committed metadata pointer — partial writes are invisible.

---

**Q3. What is hidden partitioning and why is it a major improvement over Hive?**

> In Hive, partitioning is exposed to users: to get partition pruning, a query **must** include the partition column in the `WHERE` clause, and the column values must exactly match the directory names. This leaks physical layout into query logic.
>
> In Iceberg, partition transforms (e.g., `days(ts)`) are defined once at the table level. When a user queries `WHERE ts BETWEEN '2024-01-01' AND '2024-01-31'`, Iceberg **automatically** derives the relevant partition values, prunes the manifest list, and reads only the needed files — the user never writes `WHERE date_partition = '2024-01-01'`. This decouples physical layout from query logic, enabling partition evolution without breaking queries.

---

**Q4. What is partition evolution and how does Iceberg implement it?**

> Partition evolution lets you change a table's partition scheme without rewriting existing data. Iceberg implements it by storing the **partition spec** in metadata, with a unique spec ID per version. Each data file records which spec ID was used to write it.
>
> When a partition spec is evolved (e.g., `months(ts)` → `days(ts)`), old files retain their old spec ID and old partition values; new files use the new spec. The query planner handles both specs transparently, applying the correct pruning logic for each file. No data migration is required.

---

**Q5. What is the difference between Copy-on-Write and Merge-on-Read in Iceberg?**

> Both are strategies for handling row-level deletes and updates.
>
> **Copy-on-Write (CoW):** On every DELETE/UPDATE, Iceberg reads the affected data files, filters out the changed rows, and rewrites entirely new data files. Reads are cheap (clean Parquet). Writes are expensive. Best for read-heavy tables with infrequent updates.
>
> **Merge-on-Read (MoR):** On DELETE/UPDATE, Iceberg writes a small **delete file** (positional or equality) that records which rows are removed, without touching the original data files. Reads must merge data files with delete files on-the-fly (slightly higher read cost). Writes are very fast. Best for write-heavy tables, CDC, and frequent row-level changes.

---

**Q6. How does schema evolution work in Iceberg and why is it safer than other formats?**

> Iceberg tracks columns by an internal **column ID**, not by name. This means:
> - **Rename:** safe — the ID stays the same; old files still map correctly
> - **Add:** safe — new column returns NULL for old files that don't have it
> - **Drop:** safe — the dropped column's data is ignored on read
> - **Reorder:** safe — columns are read by ID, not position
> - **Widen type:** safe for promotions (INT → LONG, FLOAT → DOUBLE)
>
> This is fundamentally safer than name-based tracking (Hive) or position-based tracking (Avro), where renames or reorders silently map wrong values.

---

### 🟡 Intermediate & Design

**Q7. How would you design an incremental ETL pipeline using Iceberg?**

> 1. Use Iceberg's **incremental scan** — read rows added between two snapshot IDs using `start-snapshot-id` and `end-snapshot-id` options
> 2. Persist the last processed snapshot ID in a state store (e.g., a metadata table or checkpoint)
> 3. On each pipeline run, load the last snapshot ID, scan incremental data, apply transformations, and write to the target table
> 4. Update the state store with the new snapshot ID
> 5. For streaming workloads, use **Flink + Iceberg** source connector which natively supports incremental reads as a continuous stream
>
> This avoids full-table scans and dramatically reduces compute cost for downstream pipelines.

---

**Q8. How does Iceberg handle the small-file problem in a streaming pipeline?**

> Streaming writes (short micro-batches) create many small Parquet files. Iceberg addresses this with:
>
> - **`rewrite_data_files` stored procedure:** compacts small files into larger target-size files (default ~512 MB). Can be run with `sort` or `zorder` strategy to also improve read locality.
> - **Flink's auto-compaction:** Flink's Iceberg sink has a built-in compaction option that triggers compaction after a configurable number of commits.
> - **Target file size property:** `write.target-file-size-bytes` controls the target size per file at write time.
> - **Bin-packing strategy:** `rewrite_data_files` with `bin-pack` strategy groups small files into bins up to the target size.
>
> Unlike Delta's auto-optimize (Databricks-native), Iceberg's compaction is **engine-agnostic** — Spark, Flink, or any conforming engine can run it.

---

**Q9. Explain Iceberg branching. How would you use it in a production ETL workflow?**

> Iceberg branches are **named pointers to snapshots**, like Git branches. Multiple branches can coexist on the same table; each has an independent snapshot history. Writes to a branch don't affect other branches.
>
> **Production ETL workflow:**
> 1. Create a branch: `CREATE BRANCH etl_run_march`
> 2. Run the ETL job, writing ONLY to the branch
> 3. Run data quality checks against the branch (row counts, null checks, business logic validation)
> 4. If checks pass: `fast_forward` the main branch to the ETL branch — atomic promotion with zero data copy
> 5. If checks fail: `DROP BRANCH` — production is untouched, no rollback needed
>
> This eliminates the need for staging tables and manual rollback procedures.

---

**Q10. How does Iceberg's multi-engine support work at the specification level?**

> Iceberg defines a **public open specification** (not just an open implementation). Any engine that implements the spec correctly can read and write Iceberg tables interoperably. The spec defines:
> - Metadata file format (JSON)
> - Manifest list and manifest file formats (Avro)
> - Supported data file formats (Parquet, ORC, Avro)
> - Partition transform semantics
> - Snapshot isolation and commit protocol
>
> This means Spark, Flink, Trino, DuckDB, Snowflake, and BigQuery all read the same table using the same metadata — no translation layer, no data copies, no proprietary extensions. **The spec is the API.**

---

**Q11. How would you implement GDPR right-to-erasure at petabyte scale with Iceberg?**

> At petabyte scale, a naive `DELETE WHERE user_id = X` rewrites many large files. The optimized approach:
>
> 1. Enable **Merge-on-Read** mode — deletes write small equality-delete files, not full rewrites
> 2. Run `DELETE FROM table WHERE user_id = 'X'` → produces a tiny delete file referencing the user's rows
> 3. Batch deletions across many users in a single pass (collect all GDPR requests for the day, run one `DELETE` with `user_id IN (...)`)
> 4. Schedule `rewrite_data_files` during off-peak hours to physically remove deleted rows from Parquet files
> 5. Run `expire_snapshots` to remove old snapshots that still reference the user's data
> 6. After expiry + rewrite, the user's data is **physically gone** from storage

---

### 🔴 Advanced & System Design

**Q12. Design a multi-engine lakehouse where Flink writes real-time events and Trino serves ad-hoc analytics on the same Iceberg table.**

> **Architecture:**
>
> ```
> Kafka ──▶ Flink Streaming Job ──▶ Iceberg Table (S3)
>                                        ▲
>                                   REST Catalog (e.g., Polaris / Nessie)
>                                        │
>                              Trino ────┘  (ad-hoc SQL)
>                              Spark ────┘  (batch ETL)
> ```
>
> **Key design decisions:**
> - Use a **REST catalog** (e.g., Apache Polaris, Project Nessie) so all engines share a consistent metadata view
> - Flink uses **format-version 2** (required for MoR and delete files)
> - Set Flink's commit interval to 1–5 minutes; Trino sees new snapshots immediately after each Flink commit
> - Run Spark-based `rewrite_data_files` asynchronously to compact Flink's small files without blocking reads
> - Use **snapshot isolation** — Trino queries always read a consistent snapshot, unaffected by concurrent Flink writes

---

**Q13. What are equality delete files and positional delete files in Iceberg v2? When would you use each?**

> Both are MoR mechanisms for recording deleted rows without rewriting data files.
>
> **Positional delete file:** records the **file path + row position** of deleted rows (e.g., "row 42 in `part-00001.parquet`"). Efficient for UPDATE/DELETE that touches specific rows at known offsets. Applied at read time by joining on file + position.
>
> **Equality delete file:** records the **values** of one or more columns that identify deleted rows (e.g., `user_id = 'abc'`). Efficient for CDC-style deletes where you know the key but not the physical position. Applied at read time by filtering rows matching the equality predicate.
>
> **When to use:**
> - Positional: Spark UPDATE and DELETE (Spark knows exact positions after planning)
> - Equality: Flink CDC sinks (arriving delete events carry key values, not positions)
>
> **Caveat:** many accumulated delete files cause read amplification. Regular `rewrite_data_files` merges them back into clean data files.

---

**Q14. Iceberg vs Delta Lake at FAANG scale — how would you make the architectural choice?**

> | Dimension | Iceberg | Delta Lake |
> |---|---|---|
> | **Engine diversity** | Mandatory (multi-team, multi-engine) | Single-engine or Databricks-first |
> | **Partition evolution** | First-class, zero-rewrite | Not supported (must rewrite) |
> | **Catalog** | REST, Hive, Glue, Nessie, Polaris | Hive, Unity Catalog |
> | **Branching** | Native (Iceberg v2) | Not available |
> | **Incremental read** | Native snapshot diff | CDF (Change Data Feed) |
> | **Streaming** | Flink-native, Spark | Spark-native |
> | **Cloud portability** | Maximum | Good |
> | **Auto-optimization** | Manual / scheduled | Auto (Databricks-only) |
>
> **At FAANG scale:** Netflix, Apple, Adobe, and LinkedIn standardized on Iceberg for its engine-agnostic spec and multi-cloud portability. Databricks-heavy shops (parts of Meta, enterprise customers) prefer Delta. **The right answer depends on your engine diversity and cloud strategy — not on the format alone.**

---

**Q15. How would you handle concurrent writes from Spark and Flink to the same Iceberg table without conflicts?**

> Iceberg's concurrency model is **optimistic**. Conflicts are resolved at commit time based on the **conflict type**:
>
> - **Append vs Append:** always succeeds — two appenders never conflict
> - **Append vs Overwrite:** conflicts if the overwrite scope overlaps the append's partition
> - **Delete vs Delete:** conflicts if both delete the same rows
> - **Delete vs Append:** may conflict depending on isolation level
>
> **Design for Spark + Flink co-writes:**
> 1. Partition writes so Flink writes to recent partitions (e.g., today's data) and Spark batch processes older partitions — **partition-level isolation**
> 2. Use Flink in append-only mode (no deletes/updates) to avoid conflicts with Spark's MERGE
> 3. Set `write.spark.fanout.enabled = true` on Spark to reduce partition overlap
> 4. For MERGE operations: serialize MERGE jobs or use Iceberg's **isolation level** property (`write.merge.isolation-level = snapshot` for less strict, `serializable` for strict)

---

**Q16. What happens when `expire_snapshots` is run? What are the risks and how do you mitigate them?**

> `expire_snapshots` removes snapshot metadata and **unreferenced data files** that are no longer pointed to by any retained snapshot.
>
> **What is deleted:**
> - JSON metadata entries for expired snapshots
> - Manifest lists no longer referenced
> - Manifest files no longer referenced
> - Data files no longer referenced by any retained snapshot
>
> **Risks:**
> 1. **Time travel loss:** expired snapshots can no longer be queried — set `older_than` conservatively
> 2. **Long-running reader conflict:** a reader holding an old snapshot reference may fail if its data files are expired mid-read — mitigate by setting `max-snapshot-age-ms` >= longest expected query runtime
> 3. **Branching interaction:** `expire_snapshots` respects branch retention — snapshots reachable from a live branch are NOT expired even if older than the threshold
>
> **Best practice:** run `expire_snapshots` with `retain_last >= 5`, set `older_than` to at least 2x your longest-running query SLA, and always run `remove_orphan_files` separately afterward to catch any files missed by the expiry.

---

*Last updated: 2025 | Format version: Apache Iceberg 2.x (Iceberg spec v2)*
