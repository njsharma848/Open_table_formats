# Delta Lake — Deep Dive Guide

> The battle-tested open table format powering lakehouse architectures at scale.

---

## Table of Contents

- [Why Delta Lake?](#why-delta-lake)
- [Key Features](#key-features)
- [Architecture Internals](#architecture-internals)
- [Code Examples](#code-examples)
- [Use Case Scenarios](#use-case-scenarios)
- [Pros and Cons](#pros-and-cons)
- [FAANG / MAANG Interview Questions](#faang--maang-interview-questions)

---

## Why Delta Lake?

Before Delta Lake, data lakes were essentially **write-only graveyards** — cheap to store, painful to manage.

### The Problem with Raw Data Lakes

| Problem | What Happens |
|---|---|
| No ACID transactions | Two writers corrupt data simultaneously |
| No schema enforcement | A bad pipeline silently writes garbage columns |
| No versioning | A bad `DELETE` is permanent |
| Inefficient reads | Spark scans every file even if only 1% is needed |
| Streaming + batch conflict | You can't reliably mix the two on the same table |

### What Delta Lake Brings

Delta Lake is an **open-source storage layer** developed by Databricks (2019) and donated to the Linux Foundation. It sits on top of your existing Parquet files and adds a **transaction log** (`_delta_log/`) that gives your data lake the reliability of a database.

```
Without Delta:   S3 / ADLS / GCS  ──▶  Raw Parquet files  ──▶  Chaos

With Delta:      S3 / ADLS / GCS  ──▶  Parquet files
                                   ──▶  _delta_log/ (JSON + Checkpoint)
                                   ──▶  ACID · Time Travel · Schema Control
```

---

## Key Features

### 1. ACID Transactions

Delta Lake uses **Optimistic Concurrency Control (OCC)**. Every write creates a new log entry. Conflicting writes are detected and rejected atomically — no partial writes, no corruption.

```python
# Two jobs writing simultaneously — Delta handles conflicts safely
# Job A and Job B both write to the same table
# One will succeed, one will retry — data is never corrupted
df_a.write.format("delta").mode("append").save("/data/events")
df_b.write.format("delta").mode("append").save("/data/events")
```

---

### 2. Time Travel (Data Versioning)

Every change to a Delta table creates a new **snapshot version**. You can query any past version.

```python
# Query table at version 5
spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("/data/events")

# Query table as it was yesterday
spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("/data/events")

# See full history
spark.sql("DESCRIBE HISTORY delta.`/data/events`")
```

---

### 3. Schema Enforcement & Evolution

**Schema Enforcement** (on by default) — rejects writes that don't match the table schema.

```python
# This will FAIL if df has extra/missing columns
df.write.format("delta").mode("append").save("/data/events")
# AnalysisException: A schema mismatch detected when writing to the Delta table.
```

**Schema Evolution** — opt-in merging of new columns.

```python
# This will SUCCEED and add the new column to the table schema
df_with_new_col.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/data/events")
```

---

### 4. Upserts with MERGE

The most powerful DML operation in Delta Lake — update, insert, and delete in a single atomic pass.

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/data/customers")

target.alias("t").merge(
    updates_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

### 5. OPTIMIZE & Z-Ordering

**OPTIMIZE** compacts many small files into fewer large ones (solves the small-file problem).  
**Z-ORDER** clusters data by column values for efficient skipping.

```python
# Compact small files
spark.sql("OPTIMIZE delta.`/data/events`")

# Compact + co-locate data by user_id and event_date
spark.sql("OPTIMIZE delta.`/data/events` ZORDER BY (user_id, event_date)")
```

---

### 6. Change Data Feed (CDF)

Emit a stream of row-level changes (INSERT, UPDATE, DELETE) from a Delta table — perfect for downstream CDC consumers.

```python
# Enable CDF on a table
spark.sql("""
  ALTER TABLE events
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Read changes since version 10
spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .table("events")
```

---

### 7. Vacuum (Data Retention Cleanup)

Remove old data files that are no longer referenced by any snapshot (default retention: 7 days).

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/data/events")

# Dry run — see what will be deleted
dt.vacuum(retentionHours=168)  # 7 days

# Actually delete (disable safety check for demo purposes)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dt.vacuum(retentionHours=0)
```

> ⚠️ After `VACUUM`, time travel to versions before the retention window is no longer possible.

---

### 8. Liquid Clustering (Modern Alternative to Z-Ordering)

Introduced in Delta 3.x — automatically manages clustering without requiring full rewrites.

```python
# Create a table with liquid clustering
spark.sql("""
  CREATE TABLE events (
    id BIGINT,
    user_id BIGINT,
    event_type STRING,
    ts TIMESTAMP
  )
  USING delta
  CLUSTER BY (user_id, event_type)
""")

# Incrementally cluster new data
spark.sql("OPTIMIZE events")
```

---

## Architecture Internals

```
delta-table/
│
├── _delta_log/                              ← Transaction Log (the brain)
│   ├── 00000000000000000000.json            ← Version 0: table creation
│   ├── 00000000000000000001.json            ← Version 1: first write
│   ├── 00000000000000000002.json            ← Version 2: MERGE operation
│   ├── ...
│   └── 00000000000000000010.checkpoint.parquet  ← Checkpoint every 10 commits
│
├── part-00000-abc123.snappy.parquet         ← Actual data files
├── part-00001-def456.snappy.parquet
└── part-00002-ghi789.snappy.parquet
```

### How a Write Works (Step by Step)

```
1. Writer reads latest version from _delta_log/
2. Writer generates new Parquet data files
3. Writer attempts to commit a new JSON log entry (atomic PUT)
4. If conflict detected → retry with conflict resolution
5. If success → new version is visible to all readers instantly
```

### Log Entry Anatomy

Each JSON log entry contains **actions**:

```json
{
  "add": {
    "path": "part-00001.snappy.parquet",
    "size": 1048576,
    "stats": "{\"numRecords\":10000,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":10000}}"
  },
  "remove": {
    "path": "part-00000.snappy.parquet",
    "deletionTimestamp": 1700000000000
  },
  "commitInfo": {
    "operation": "MERGE",
    "operationParameters": { "predicate": "id = source.id" }
  }
}
```

---

## Code Examples

### Create and Write a Delta Table

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaDemo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Write as Delta
df.write.format("delta").mode("overwrite").save("/tmp/delta/people")
```

---

### Read a Delta Table

```python
df = spark.read.format("delta").load("/tmp/delta/people")
df.show()
```

---

### Update and Delete Rows

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/tmp/delta/people")

# Update: set age = 31 where name = 'Alice'
dt.update(condition="name = 'Alice'", set={"age": "31"})

# Delete: remove Bob
dt.delete(condition="name = 'Bob'")
```

---

### Restore a Table to a Previous Version

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/tmp/delta/people")

# Roll back to version 0
dt.restoreToVersion(0)
```

---

### Streaming Read and Write

```python
# Stream into a Delta table
streaming_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/people") \
    .start("/tmp/delta/people")

# Stream from a Delta table
spark.readStream \
    .format("delta") \
    .load("/tmp/delta/people") \
    .writeStream \
    .format("console") \
    .start()
```

---

## Use Case Scenarios

### 1. CDC Pipeline (Database Replication)

**Scenario:** Replicate a MySQL `orders` table into your data lake, applying inserts, updates, and deletes in real time.

```
MySQL binlog  ──▶  Kafka  ──▶  Spark Structured Streaming  ──▶  Delta MERGE
```

```python
# Continuously apply CDC events from Kafka into Delta
def upsert_to_delta(batch_df, batch_id):
    dt = DeltaTable.forPath(spark, "/data/orders")
    dt.alias("t").merge(
        batch_df.alias("s"), "t.order_id = s.order_id"
    ).whenMatchedUpdate(condition="s.op = 'UPDATE'", set={"status": "s.status"}) \
     .whenNotMatchedInsert(condition="s.op = 'INSERT'", values={"order_id": "s.order_id", "status": "s.status"}) \
     .whenMatchedDelete(condition="s.op = 'DELETE'") \
     .execute()

kafka_stream.writeStream.foreachBatch(upsert_to_delta).start()
```

---

### 2. Financial Audit Trail

**Scenario:** A fintech company needs to prove the state of account balances at any point in time for regulatory compliance.

```python
# Auditors request: "Show us all balances as of March 15, 2024"
audit_df = spark.read.format("delta") \
    .option("timestampAsOf", "2024-03-15") \
    .table("account_balances")
```

Delta's time travel makes this trivial — no need for separate audit tables or event sourcing.

---

### 3. Machine Learning Feature Store

**Scenario:** ML team needs versioned, point-in-time correct features to avoid training/serving skew.

```python
# Training: use features as of the training date
train_features = spark.read.format("delta") \
    .option("timestampAsOf", training_cutoff_date) \
    .table("feature_store.user_features")

# Ensure the same version is used for batch scoring
model.fit(train_features)
```

---

### 4. Late-Arriving Data Correction

**Scenario:** An IoT sensor sends delayed data 2 days after the fact. The pipeline needs to correct historical partitions without full rewrites.

```python
# MERGE handles late arrivals cleanly
dt.alias("t").merge(
    late_data_df.alias("s"),
    "t.device_id = s.device_id AND t.event_ts = s.event_ts"
).whenNotMatchedInsertAll() \
 .execute()
```

---

### 5. GDPR Right-to-Erasure (Right to be Forgotten)

**Scenario:** A user requests deletion of all their data. The data is scattered across partitions.

```python
# Delete all records for a specific user across all partitions
dt.delete(condition="user_id = '12345'")

# Vacuum to physically remove the data from storage
dt.vacuum(retentionHours=0)
```

---

### 6. Slowly Changing Dimensions (SCD Type 2)

**Scenario:** Maintain a history of customer address changes in a dimension table.

```python
dt.alias("t").merge(
    new_data.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": "false",
    "end_date": "s.start_date"
}).whenNotMatchedInsertAll() \
 .execute()
```

---

## Pros and Cons

### ✅ Pros

| Advantage | Detail |
|---|---|
| **ACID on object storage** | Full transactional guarantees on S3/GCS/ADLS — previously impossible |
| **Mature & battle-tested** | Powering petabyte-scale workloads at Databricks customers since 2019 |
| **Simple architecture** | Just Parquet + JSON log — no external metastore required to get started |
| **First-class Spark integration** | Native DML (MERGE, UPDATE, DELETE) built into Spark's SQL engine |
| **Streaming + batch unified** | Same table, same API — no Lambda architecture needed |
| **Rich ecosystem** | Deep Databricks tooling, connectors for Kafka, Fivetran, dbt, Airbyte |
| **Change Data Feed** | Easy incremental consumption for downstream pipelines |
| **Auto-optimization** | Auto-compaction and auto-optimize available on Databricks |
| **Strong community** | Linux Foundation project with wide industry adoption |

---

### ❌ Cons

| Disadvantage | Detail |
|---|---|
| **Spark-centric** | Best performance and full features require Spark; other engines are secondary |
| **Databricks lock-in risk** | Some advanced features (Auto Optimize, Predictive IO) are Databricks-only |
| **Partition evolution is limited** | Cannot change the partition scheme without rewriting data (unlike Iceberg) |
| **Multi-engine gaps** | Trino, DuckDB, and Flink support exists but lags behind Iceberg's ecosystem |
| **Transaction log can grow** | `_delta_log/` accumulates over time; requires checkpoint tuning at scale |
| **No native catalog** | Requires Hive Metastore, Unity Catalog, or similar external catalog |
| **Cross-cloud complexity** | Cross-cloud Delta sharing adds operational overhead |
| **Vacuum is irreversible** | Once vacuumed, old versions are gone — easy to accidentally lose time travel |

---

## FAANG / MAANG Interview Questions

### 🔵 Conceptual & Fundamentals

**Q1. What is the Delta Lake transaction log and how does it work?**

> The `_delta_log/` is a directory of ordered JSON files, one per commit. Each file records **add** and **remove** actions for data files, along with metadata like statistics and operation info. Every 10 commits, a **Parquet checkpoint** is written to avoid replaying thousands of JSON files. When a reader opens a Delta table, it reads the latest checkpoint + subsequent JSON files to reconstruct the current table state.

---

**Q2. How does Delta Lake achieve ACID transactions on object storage, which is not natively transactional?**

> Object stores like S3 only guarantee **atomic PUT** for individual objects. Delta Lake exploits this by making each commit a single atomic PUT of a new JSON log file (e.g., `00000000000000000005.json`). Writers use **optimistic concurrency control** — they read the current version, do their work, then attempt to commit the next version number. If two writers race, only one succeeds; the other detects the conflict and either retries or fails with a `ConcurrentModificationException`, depending on the conflict type.

---

**Q3. What is the difference between `overwrite` and `MERGE` in Delta Lake?**

> `overwrite` replaces the entire table or partition — it's destructive and fast. `MERGE` is a row-level operation that atomically upserts, updates, and/or deletes specific records based on a matching condition. `MERGE` is far more expensive (requires a join), but essential for CDC, SCD, and incremental pipelines where you need surgical precision without touching unaffected records.

---

**Q4. What happens when two Spark jobs write to the same Delta table simultaneously?**

> Delta uses **optimistic concurrency**. Both jobs read the current version and proceed independently. When committing, each tries to write the next sequential log file. The second committer detects that the version it expected to write already exists (written by the first), and evaluates if there's a real conflict:
> - **No conflict** (e.g., both appending to different partitions) → retries and succeeds
> - **Real conflict** (e.g., both deleting the same records) → throws `ConcurrentModificationException`

---

**Q5. What is Z-Ordering and how does it improve query performance?**

> Z-Ordering is a **data locality technique** that co-locates related data in the same files using a Z-curve (space-filling curve) across multiple dimensions. For example, `ZORDER BY (country, city)` ensures records with the same country and city land in the same file. This enables **data skipping** — when a query filters on `country = 'US'`, Delta's statistics on each file let it skip files that contain no US records entirely, without scanning them.

---

**Q6. What is the difference between Schema Enforcement and Schema Evolution in Delta Lake?**

> **Schema Enforcement** is on by default. It rejects writes whose schema doesn't match the table's registered schema — prevents accidental data corruption.
> **Schema Evolution** is opt-in (`mergeSchema = true`). It allows new columns in the incoming data to be automatically added to the table schema. It does NOT allow dropping columns, changing data types (unless widening), or reordering columns without explicit `overwriteSchema`.

---

### 🟡 Intermediate & Design

**Q7. How would you design a CDC pipeline using Delta Lake?**

> 1. Ingest change events (INSERT/UPDATE/DELETE) from a source (MySQL binlog → Kafka)
> 2. Use **Spark Structured Streaming** to consume Kafka in micro-batches
> 3. Use `foreachBatch` to apply a **Delta MERGE** per batch — matching on primary key, applying the operation type
> 4. Enable **Change Data Feed** on the Delta table so downstream consumers can subscribe to further changes
> 5. Use **checkpointing** to ensure exactly-once semantics

---

**Q8. What is the small-file problem in Delta Lake and how do you solve it?**

> Streaming ingestion creates many tiny Parquet files (one per micro-batch per partition). Small files cause:
> - Slow reads (high file-open overhead, driver memory pressure from listing)
> - High cloud API costs (many GET requests)
>
> **Solutions:**
> - `OPTIMIZE` — manually compact small files into target-size files (~1 GB)
> - **Auto Optimize** (Databricks) — automatically compacts after each write
> - **Liquid Clustering** (Delta 3.x) — incrementally clusters without full rewrites
> - Tune `spark.sql.files.maxRecordsPerFile` to write fewer, larger files upfront

---

**Q9. How does Delta Lake handle late-arriving data in a streaming pipeline?**

> Delta Lake allows concurrent streaming writes and batch corrections. Late data can be inserted or merged using a **batch MERGE** without affecting the ongoing stream. If using Structured Streaming with watermarks, Delta supports `foreachBatch` for late-data upserts. Since Delta has ACID guarantees, the late-arriving batch write and the streaming write won't conflict as long as they operate on different records or partitions.

---

**Q10. What is the Change Data Feed (CDF) and when would you use it?**

> CDF records **row-level changes** (insert, update_preimage, update_postimage, delete) in special `_change_data/` files alongside normal data files. It's enabled per table via `delta.enableChangeDataFeed = true`.
>
> **Use cases:**
> - Propagating changes to downstream Delta tables without full table scans
> - Building audit logs with before/after values
> - Feeding materialized views or search indexes incrementally
> - Replicating only changed rows to a downstream data warehouse

---

**Q11. How does Delta Lake handle GDPR deletion requests efficiently?**

> 1. Run `DELETE FROM table WHERE user_id = 'X'` — Delta marks the relevant files as **removed** and writes new files without the deleted records
> 2. Run `VACUUM` after the retention period to **physically delete** the old Parquet files from storage
> 3. The combination of logical delete (via log) + physical delete (via vacuum) satisfies right-to-erasure
>
> **Challenge:** if user data is spread across many partitions, the DELETE triggers many file rewrites. Partitioning by a hash of `user_id` or using **Deletion Vectors** (Delta 2.3+) reduces the rewrite cost.

---

### 🔴 Advanced & System Design

**Q12. Design a Lakehouse ingestion architecture for 10 TB/day of event data with sub-5-minute latency requirements.**

> **Architecture:**
> - **Ingest:** Kafka → Spark Structured Streaming (micro-batch, 30-second intervals)
> - **Landing Zone:** Raw Delta table (append-only, partitioned by `date/hour`)
> - **Curated Layer:** Second Delta table, enriched via streaming MERGE
> - **Optimization:** Auto-optimize + Auto-compaction enabled; Z-ORDER on `user_id` for analytical queries
> - **Serving:** Databricks SQL Warehouse or Trino reading the curated Delta table
> - **Monitoring:** Delta table history + metrics on commit latency, file count, and query P99
>
> **Key design decisions:** separate raw and curated tables to isolate failures; use CDF to drive the curated layer from the raw layer; set watermarks for late data tolerance.

---

**Q13. What are Deletion Vectors in Delta Lake and how do they improve DELETE/UPDATE performance?**

> Traditionally, a `DELETE` in Delta requires reading all affected files, filtering out deleted rows, and rewriting the entire files — expensive for large files with few deletions.
>
> **Deletion Vectors** (introduced in Delta 2.3) store deleted row positions in a small **side file** (a bitmap) instead of rewriting the full data file. Readers apply the deletion vector to filter out deleted rows at read time. This makes deletes nearly instant. The data files are only physically rewritten during the next `OPTIMIZE` run.
>
> **Trade-off:** reads become slightly more expensive (must apply the bitmap), but writes are dramatically faster — critical for GDPR deletions and frequent updates.

---

**Q14. How would you handle the performance impact of a very large Delta table history (thousands of versions)?**

> - **Checkpoint tuning:** Lower `delta.checkpointInterval` (default 10) so checkpoints are written more frequently, reducing the number of JSON files to replay on table open
> - **`VACUUM` regularly:** Remove old data files to keep storage lean (doesn't remove log files)
> - **Log cleanup:** Delta automatically retains only the last N checkpoints; ensure `delta.logRetentionDuration` is set appropriately
> - **Avoid unbounded time travel windows:** Inform stakeholders that only the last 7–30 days are retained; vacuum accordingly
> - **Use table properties** to configure per-table retention: `delta.deletedFileRetentionDuration`

---

**Q15. Delta Lake vs Apache Iceberg — when would you choose one over the other at FAANG scale?**

> | Dimension | Choose Delta Lake | Choose Iceberg |
> |---|---|---|
> | **Engine** | Primarily Spark / Databricks | Multi-engine (Spark + Flink + Trino + DuckDB) |
> | **Platform** | Databricks-native | Cloud-agnostic, vendor-neutral |
> | **Upserts** | Strong (MERGE) | Strong (MERGE) |
> | **Partition evolution** | Limited | Full partition evolution |
> | **Ecosystem** | Databricks tooling, Unity Catalog | Nessie, AWS Glue, Snowflake, BigQuery |
> | **Streaming** | Good | Good |
> | **Multi-cloud** | Works, less seamless | First-class |
>
> At FAANG scale: **Google and Netflix** have pushed Iceberg; **Databricks shops** (large portions of Meta, Apple pipelines) prefer Delta. If you're not locked into Databricks, Iceberg offers better long-term portability.

---

**Q16. Explain how Delta Lake ensures exactly-once semantics in a Spark Structured Streaming pipeline.**

> Delta Lake integrates with Spark's **idempotent sink** mechanism:
> 1. Each micro-batch is assigned a unique `batchId` by Spark
> 2. Delta records the `batchId` in the commit metadata of the transaction log
> 3. Before committing, Delta checks if this `batchId` was already committed — if yes, it skips the write (idempotent)
> 4. Combined with Spark's **checkpoint** (which tracks the Kafka offset), this gives end-to-end exactly-once: even if the job crashes mid-batch and restarts, the same batch is replayed but not double-written

---

*Last updated: 2025 | Format version: Delta Lake 3.x*
