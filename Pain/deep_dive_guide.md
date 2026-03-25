# Apache Paimon — Deep Dive Guide

> The streaming-first open table format built for real-time lakehouses, high-throughput upserts, and unified batch + stream processing.

---

## Table of Contents

- [Why Apache Paimon?](#why-apache-paimon)
- [Key Features](#key-features)
- [Architecture Internals](#architecture-internals)
- [Code Examples](#code-examples)
- [Use Case Scenarios](#use-case-scenarios)
- [Pros and Cons](#pros-and-cons)
- [FAANG / MAANG Interview Questions](#faang--maang-interview-questions)

---

## Why Apache Paimon?

Apache Paimon was born inside **Alibaba's Flink team** to solve a problem that Iceberg, Delta Lake, and Hudi couldn't fully address: **real-time, low-latency streaming writes with high-frequency upserts into a data lake**, while still supporting efficient batch reads.

### The Problem with Existing Formats for Streaming

| Problem | What Happens with Iceberg / Delta in Streaming |
|---|---|
| **Small-file explosion** | Flink commits every few seconds → millions of tiny Parquet files |
| **High upsert cost** | MoR delete files accumulate; CoW rewrites entire files per update |
| **Changelog is lossy** | Formats expose only final state; intermediate changes are lost |
| **No native streaming read** | Batch-oriented formats need workarounds for continuous streaming reads |
| **Lookup join is expensive** | Joining a stream against a large table requires full scans or external stores |
| **No built-in compaction for streams** | Compaction must be scheduled externally; streaming writes keep degrading reads |

### What Paimon Brings

Apache Paimon (originally **Flink Table Store**, donated to Apache in 2023) is purpose-built for the **streaming lakehouse**. It uses an **LSM-Tree (Log-Structured Merge-Tree)** storage engine — the same architecture behind databases like RocksDB and LevelDB — which is inherently optimized for high-throughput writes and efficient compaction.

```
Without Paimon:   Flink Stream ──▶ Kafka (buffer) ──▶ Batch job ──▶ Iceberg/Delta
                  (minutes to hours of latency, operational complexity)

With Paimon:      Flink Stream ──▶ Paimon Table (S3 / HDFS)
                  (seconds of latency, native streaming read, built-in compaction)
```

> Originally called **Flink Table Store**, renamed to **Apache Paimon** and graduated as a top-level Apache project in **2023**. Backed by Alibaba, used at scale in Alibaba Cloud's real-time data platform.

---

## Key Features

### 1. LSM-Tree Storage Engine

Paimon's core differentiator. Instead of writing one large Parquet file per batch, it writes **sorted SST (Sorted String Table) files** in levels — exactly like a database engine. New writes land in **Level 0**; background **compaction** merges and sorts them into deeper levels.

```
Write Path:
  New data  ──▶  In-memory buffer (sorted)
                       │
                       ▼ flush
              Level-0 SST files (unsorted across files)
                       │
                       ▼ compaction
              Level-1 SST files (sorted, non-overlapping)
                       │
                       ▼ compaction
              Level-2 SST files (larger, fully sorted)

Read Path:
  Query ──▶ Merge Level-0 + Level-1 + Level-2  ──▶  Result
            (like a merge sort; recent values win)
```

This means **upserts are O(1) writes** (just append to the buffer), and **reads merge on-the-fly** — no file rewrites needed for updates.

---

### 2. Primary Key Tables (Upsert Tables)

Define a primary key and Paimon handles deduplication and upserts automatically. No MERGE statements needed.

```sql
-- Flink SQL: create a primary key table
CREATE TABLE orders (
    order_id   BIGINT,
    customer_id BIGINT,
    amount     DOUBLE,
    status     STRING,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector'     = 'paimon',
    'path'          = 's3://my-bucket/paimon/orders',
    'file.format'   = 'parquet'
);

-- Upsert: just INSERT — Paimon deduplicates by primary key automatically
INSERT INTO orders VALUES (1, 101, 250.0, 'SHIPPED', CURRENT_TIMESTAMP);
INSERT INTO orders VALUES (1, 101, 250.0, 'DELIVERED', CURRENT_TIMESTAMP);
-- Only the latest record for order_id=1 is visible on read
```

---

### 3. Append-Only Tables

For pure event streams where you never update records — logs, clickstreams, sensor data.

```sql
-- Append-only table: no primary key, no deduplication
CREATE TABLE clickstream (
    user_id    BIGINT,
    page_url   STRING,
    event_ts   TIMESTAMP(3),
    session_id STRING
) WITH (
    'connector'        = 'paimon',
    'path'             = 's3://my-bucket/paimon/clickstream',
    'write-mode'       = 'append-only',
    'bucket'           = '8'
);

INSERT INTO clickstream SELECT user_id, page_url, event_ts, session_id
FROM kafka_clickstream_source;
```

---

### 4. Changelog Mode — Full CDC Semantics

Paimon can emit **+I (insert), -U (before-update), +U (after-update), -D (delete)** changelog records. Unlike other formats that expose only the final state, Paimon preserves the **complete change history** as a first-class stream.

```sql
-- Enable full changelog on a primary key table
CREATE TABLE customers (
    customer_id BIGINT,
    name        STRING,
    email       STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector'      = 'paimon',
    'path'           = 's3://my-bucket/paimon/customers',
    'changelog-producer' = 'full-compaction'  -- emit changelogs during compaction
);

-- Read as a changelog stream in Flink
SELECT * FROM customers /*+ OPTIONS('scan.mode'='latest') */;
-- Returns: +I[1, Alice, alice@x.com]
-- After update: -U[1, Alice, alice@x.com], +U[1, Alice, newalice@x.com]
```

**Changelog producer options:**

| Option | Description | Cost |
|---|---|---|
| `none` | No changelog; read final state only | Lowest |
| `input` | Passes through input changelog if source provides it | Low |
| `lookup` | Queries existing state to produce before-image | Medium |
| `full-compaction` | Generates changelogs during compaction | Higher write cost, richest output |

---

### 5. Streaming Read & Write (Unified)

Paimon is the **only open table format** with first-class support for both streaming reads and streaming writes **natively**, without Kafka as an intermediary.

```sql
-- Stream continuously from a Paimon table (like tailing a Kafka topic)
SELECT order_id, status, updated_at
FROM orders /*+ OPTIONS('scan.mode'='latest') */;

-- Streaming write from Kafka into Paimon
INSERT INTO orders
SELECT order_id, customer_id, amount, status, updated_at
FROM kafka_orders_cdc_source;

-- Batch read (snapshot mode — reads current state, then stops)
SELECT * FROM orders /*+ OPTIONS('scan.mode'='latest-full') */;
```

---

### 6. Partial Update (Merge Functions)

Update only specific columns of a row without knowing or supplying the full row. Powered by pluggable **merge functions**.

```sql
-- Table with partial update merge function
CREATE TABLE product_metrics (
    product_id  BIGINT,
    view_count  BIGINT,
    cart_count  BIGINT,
    order_count BIGINT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector'              = 'paimon',
    'path'                   = 's3://my-bucket/paimon/product_metrics',
    'merge-function'         = 'partial-update'
);

-- Stream A: only updates view_count (cart_count and order_count stay unchanged)
INSERT INTO product_metrics VALUES (42, 1000, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT));

-- Stream B: only updates order_count
INSERT INTO product_metrics VALUES (42, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), 50);

-- Read: product_id=42 → view_count=1000, cart_count=NULL, order_count=50
```

**Available merge functions:**

| Function | Description |
|---|---|
| `deduplicate` | Keep the latest record (default) |
| `partial-update` | Merge non-null fields from multiple streams |
| `aggregation` | Apply aggregate functions (SUM, MAX, MIN, COUNT) per column |
| `first-row` | Keep the first record seen per key |

---

### 7. Lookup Join (Streaming Dimension Table)

Use a Paimon primary key table as a **real-time dimension table** in a streaming lookup join — without loading it into memory or hitting an external key-value store.

```sql
-- Paimon table as a dimension (e.g., product catalog)
CREATE TABLE product_catalog (
    product_id  BIGINT PRIMARY KEY NOT ENFORCED,
    name        STRING,
    category    STRING,
    price       DOUBLE
) WITH ('connector' = 'paimon', 'path' = 's3://my-bucket/paimon/products');

-- Streaming lookup join: enrich orders with product info in real time
SELECT
    o.order_id,
    o.amount,
    p.name        AS product_name,
    p.category    AS product_category
FROM kafka_orders AS o
LEFT JOIN product_catalog FOR SYSTEM_TIME AS OF o.proc_time AS p
    ON o.product_id = p.product_id;
```

Paimon serves the lookup using its **LSM index** — efficient point lookups without full scans.

---

### 8. Cross-Partition Upserts

Update records even when you don't know which partition they live in — without scanning the entire table.

```sql
-- Table partitioned by date
CREATE TABLE events (
    event_id   BIGINT,
    event_date DATE,
    status     STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) PARTITIONED BY (event_date)
WITH (
    'connector'                         = 'paimon',
    'path'                              = 's3://my-bucket/paimon/events',
    'cross-partition-upsert.enabled'    = 'true'
);

-- Update an event without knowing its partition — Paimon finds it via index
INSERT INTO events VALUES (9999, DATE '2024-01-10', 'PROCESSED');
```

---

### 9. Time Travel & Snapshot Queries

```sql
-- Query table at a specific snapshot ID
SELECT * FROM orders /*+ OPTIONS('scan.snapshot-id'='5') */;

-- Query table as of a specific timestamp
SELECT * FROM orders /*+ OPTIONS('scan.timestamp-millis'='1704067200000') */;

-- List all snapshots
SELECT * FROM orders$snapshots;

-- Rollback to a snapshot
CALL sys.rollback_to_snapshot('mydb.orders', 3);
```

---

### 10. Table Maintenance (Compaction & Expiry)

```sql
-- Manually trigger compaction for a table
CALL sys.compact('mydb.orders');

-- Compact a specific partition
CALL sys.compact('mydb.orders', 'event_date=2024-01-15');

-- Expire old snapshots
CALL sys.expire_snapshots('mydb.orders', 3);  -- retain last 3 snapshots

-- Remove orphan files
CALL sys.remove_orphan_files('mydb.orders');
```

---

## Architecture Internals

```
paimon-table/
│
├── schema/
│   └── schema-0                     ← Table schema definition (JSON-like)
│
├── snapshot/
│   ├── snapshot-1                   ← Snapshot metadata (manifest list pointer)
│   ├── snapshot-2
│   └── LATEST                       ← Pointer to latest snapshot ID
│
├── manifest/
│   ├── manifest-list-xxx.avro       ← Lists all manifest files per snapshot
│   └── manifest-yyy.avro            ← Lists SST data files + stats
│
└── bucket-0/                        ← Data partitioned into buckets
    ├── data-xxx-0.parquet           ← Level-0 SST file (recent, unsorted)
    ├── data-yyy-1.parquet           ← Level-1 SST file (compacted)
    └── index/
        └── index-zzz.index          ← Bloom filter / hash index for lookups
```

### LSM Compaction Strategy

```
                    WRITE
                      │
                      ▼
          ┌─────────────────────┐
          │   MemTable (sorted) │  ← In-memory write buffer
          └──────────┬──────────┘
                     │  flush when full
                     ▼
          ┌─────────────────────┐
          │  Level 0 SST Files  │  ← Small, may overlap, recent
          │  (L0-0, L0-1, L0-2) │
          └──────────┬──────────┘
                     │  compaction (merge + sort)
                     ▼
          ┌─────────────────────┐
          │  Level 1 SST Files  │  ← Larger, non-overlapping
          └──────────┬──────────┘
                     │  compaction
                     ▼
          ┌─────────────────────┐
          │  Level 2 SST Files  │  ← Largest, fully sorted
          └─────────────────────┘
                     │
                     ▼
                   READ
          (merge all levels; latest value wins per key)
```

### Snapshot Lifecycle

```
Every Flink checkpoint  ──▶  New snapshot committed
                                    │
                        ┌───────────┴──────────┐
                        │                      │
                  Snapshot N              Snapshot N+1
                  (manifest list)         (manifest list)
                        │                      │
                  [SST file refs]         [SST file refs + new files]

Compaction job  ──▶  Merges SST files  ──▶  Produces new compacted snapshot
Expiry job      ──▶  Removes old snapshots + unreferenced SST files
```

### Bucket Strategy

Paimon distributes data across **buckets** (like Hive bucketing) for parallelism and efficient lookups:

```
Primary Key Table:
  record → hash(primary_key) % num_buckets → assigned bucket

Append-Only Table:
  record → round-robin or hash(bucket_key) % num_buckets → assigned bucket

Benefit: point lookups only scan ONE bucket instead of all files
```

---

## Code Examples

### Environment Setup (Flink SQL CLI)

```sql
-- Add Paimon JAR to Flink
ADD JAR '/opt/flink/lib/paimon-flink-1.18-0.7.jar';

-- Create a Paimon catalog backed by Hive Metastore
CREATE CATALOG my_paimon WITH (
    'type'          = 'paimon',
    'metastore'     = 'hive',
    'uri'           = 'thrift://hive-metastore:9083',
    'warehouse'     = 's3://my-bucket/paimon'
);

USE CATALOG my_paimon;
CREATE DATABASE mydb;
USE mydb;
```

---

### Real-Time CDC Ingestion from MySQL

```sql
-- Source: MySQL CDC via Flink CDC connector
CREATE TABLE mysql_orders (
    order_id    BIGINT PRIMARY KEY NOT ENFORCED,
    customer_id BIGINT,
    amount      DOUBLE,
    status      STRING,
    updated_at  TIMESTAMP(3),
    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
) WITH (
    'connector'          = 'mysql-cdc',
    'hostname'           = 'mysql-host',
    'database-name'      = 'shop',
    'table-name'         = 'orders',
    'username'           = 'flink',
    'password'           = 'secret'
);

-- Sink: Paimon primary key table
CREATE TABLE paimon_orders (
    order_id    BIGINT,
    customer_id BIGINT,
    amount      DOUBLE,
    status      STRING,
    updated_at  TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'path'      = 's3://my-bucket/paimon/orders'
);

-- Stream CDC events directly into Paimon
INSERT INTO paimon_orders SELECT * FROM mysql_orders;
```

---

### Read from Paimon in Spark (Batch)

```python
# Read Paimon table in Spark for batch analytics
df = spark.read.format("paimon") \
    .load("s3://my-bucket/paimon/orders")

df.filter("status = 'DELIVERED'") \
  .groupBy("customer_id") \
  .agg({"amount": "sum"}) \
  .show()
```

---

### Aggregation Merge Function (Real-Time Metrics)

```sql
-- Real-time aggregation table: track running totals per product
CREATE TABLE product_sales_agg (
    product_id    BIGINT,
    total_revenue DOUBLE,
    order_count   BIGINT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector'                         = 'paimon',
    'path'                              = 's3://my-bucket/paimon/product_sales_agg',
    'merge-function'                    = 'aggregation',
    'fields.total_revenue.aggregate-function' = 'sum',
    'fields.order_count.aggregate-function'   = 'sum'
);

-- Each incoming record ADDS to existing totals — no pre-aggregation needed
INSERT INTO product_sales_agg
SELECT product_id, amount AS total_revenue, 1 AS order_count
FROM kafka_orders_source;
```

---

### Branching (Experimental Write Isolation)

```sql
-- Create a branch for a risky backfill
CALL sys.create_branch('mydb.orders', 'fix_march_data');

-- Write to branch only
INSERT INTO orders /*+ OPTIONS('branch'='fix_march_data') */
SELECT * FROM corrected_march_source;

-- Merge branch back to main after validation
CALL sys.fast_forward('mydb.orders', 'fix_march_data');

-- Drop branch if validation fails
CALL sys.delete_branch('mydb.orders', 'fix_march_data');
```

---

## Use Case Scenarios

### 1. Real-Time Database Replication (CDC Lakehouse)

**Scenario:** Replicate an entire MySQL/PostgreSQL database into a data lake with second-level latency, preserving all inserts, updates, and deletes.

```
MySQL/Postgres  ──▶  Flink CDC Connector  ──▶  Paimon Primary Key Tables
                                                       │
                                           Query with Trino / Spark / Hive
```

Paimon's LSM engine handles the high update rate natively. The result is a **queryable, always-fresh replica** of your operational database — without Kafka or a dedicated CDC pipeline store.

---

### 2. Real-Time Feature Store

**Scenario:** Compute and serve ML features in real time. Features are computed by a Flink job and must be readable by both an online serving layer and offline training.

```sql
-- Feature computation job writes to Paimon
INSERT INTO feature_store.user_features
SELECT
    user_id,
    COUNT(*) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS purchases_last_100,
    SUM(amount) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS spend_last_7d
FROM order_stream;
```

```python
# Offline training: batch read from Paimon via Spark
features = spark.read.format("paimon").load("s3://.../user_features")
model.fit(features.join(labels, "user_id"))
```

---

### 3. Streaming Metrics Dashboard (Real-Time OLAP)

**Scenario:** Power a live dashboard showing real-time revenue, order counts, and conversion rates — updated every few seconds.

```sql
-- Aggregation table updated by Flink in real time
CREATE TABLE dashboard_metrics (
    metric_date    DATE,
    metric_hour    INT,
    region         STRING,
    total_revenue  DOUBLE,
    order_count    BIGINT,
    PRIMARY KEY (metric_date, metric_hour, region) NOT ENFORCED
) WITH (
    'connector'    = 'paimon',
    'merge-function' = 'aggregation',
    'fields.total_revenue.aggregate-function' = 'sum',
    'fields.order_count.aggregate-function'   = 'sum'
);

-- Dashboard queries Paimon directly via Trino or StarRocks
SELECT region, SUM(total_revenue) FROM dashboard_metrics
WHERE metric_date = CURRENT_DATE GROUP BY region;
```

---

### 4. Multi-Stream Partial Update (Wide Table Enrichment)

**Scenario:** Build a single wide customer 360 table enriched from multiple independent Flink jobs — profile, behavior, and payment streams — each updating only their columns.

```sql
CREATE TABLE customer_360 (
    customer_id      BIGINT,
    -- Profile stream columns
    name             STRING,
    email            STRING,
    -- Behavior stream columns
    last_login       TIMESTAMP(3),
    page_views_30d   BIGINT,
    -- Payment stream columns
    total_spend      DOUBLE,
    last_payment_dt  DATE,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector'      = 'paimon',
    'merge-function' = 'partial-update'
);

-- Three independent Flink jobs write simultaneously, each touching only their columns
-- Paimon merges non-null fields — no coordination between jobs needed
```

---

### 5. Late-Arriving Event Correction

**Scenario:** IoT sensors send delayed data hours after the fact. Correct historical partitions without rewriting entire partition files.

```sql
-- Primary key table handles late arrivals as natural upserts
CREATE TABLE sensor_readings (
    sensor_id    BIGINT,
    reading_ts   TIMESTAMP(3),
    temperature  DOUBLE,
    humidity     DOUBLE,
    PRIMARY KEY (sensor_id, reading_ts) NOT ENFORCED
) PARTITIONED BY (DATE_FORMAT(reading_ts, 'yyyy-MM-dd'))
WITH ('connector' = 'paimon', 'path' = 's3://my-bucket/paimon/sensors');

-- Late-arriving data: just INSERT — Paimon's LSM handles deduplication and correction
INSERT INTO sensor_readings VALUES (42, TIMESTAMP '2024-01-10 08:00:00', 23.5, 65.2);
```

---

### 6. Streaming Dimension Enrichment (Lookup Join)

**Scenario:** Enrich a high-volume order stream with product and customer details from Paimon dimension tables — in real time, without Redis or external KV stores.

```sql
-- Enrich orders with product and customer info via Paimon lookup joins
SELECT
    o.order_id,
    o.amount,
    c.name           AS customer_name,
    c.tier           AS customer_tier,
    p.name           AS product_name,
    p.category       AS category
FROM kafka_orders AS o
LEFT JOIN paimon_customers FOR SYSTEM_TIME AS OF o.proc_time AS c
    ON o.customer_id = c.customer_id
LEFT JOIN paimon_products FOR SYSTEM_TIME AS OF o.proc_time AS p
    ON o.product_id = p.product_id;
```

---

## Pros and Cons

### ✅ Pros

| Advantage | Detail |
|---|---|
| **Streaming-first design** | Built from the ground up for Flink streaming; not an afterthought like in Iceberg/Delta |
| **LSM-Tree storage** | Handles high-frequency upserts natively; no file rewrite on update |
| **Native changelog semantics** | Full +I/-U/+U/-D changelog as a first-class stream; unique in the open table format space |
| **Partial updates** | Update individual columns from multiple independent streams without full-row knowledge |
| **Aggregation merge** | Built-in SUM/MAX/MIN/COUNT per column — eliminates separate aggregation layers |
| **Lookup join support** | Serve dimension tables directly from Paimon without Redis or external KV stores |
| **Cross-partition upserts** | Update records without knowing their partition — unique capability |
| **Unified batch + stream** | Same table, same API, same data — no Lambda architecture |
| **Compact by design** | Background compaction is automatic and continuous in Flink pipelines |
| **Low-latency writes** | Seconds-level freshness, not minutes; ideal for real-time dashboards |
| **Multi-engine reads** | Spark, Trino, Hive, StarRocks, and Doris can read Paimon tables |

---

### ❌ Cons

| Disadvantage | Detail |
|---|---|
| **Flink-centric** | Full feature set requires Flink; Spark write support is limited and less mature |
| **Smaller ecosystem** | Younger project (graduated 2023); fewer connectors, fewer managed cloud integrations than Iceberg or Delta |
| **LSM read amplification** | Reads merge multiple SST levels; heavy write workloads with no compaction degrade read performance |
| **Operational complexity** | LSM compaction must be monitored and tuned (level sizes, compaction triggers) — more ops overhead |
| **Limited cloud-native support** | Not natively supported by Snowflake, BigQuery, or Athena as of 2025 |
| **Smaller community** | Fewer Stack Overflow answers, fewer blog posts, fewer third-party tools compared to Iceberg/Delta |
| **Catalog maturity** | REST catalog and multi-cloud catalog support less mature than Iceberg's ecosystem |
| **No Liquid Clustering equivalent** | No adaptive automatic layout optimization like Delta's Liquid Clustering |
| **Less mature time travel tooling** | Time travel and snapshot management less polished than Delta or Iceberg |
| **Documentation gaps** | Advanced features (cross-partition upsert, partial update tuning) are sparsely documented |

---

## FAANG / MAANG Interview Questions

### 🔵 Conceptual & Fundamentals

**Q1. What is Apache Paimon and why was it created? What gap does it fill that Iceberg and Delta don't?**

> Apache Paimon (originally Flink Table Store) was created by Alibaba's Flink team to solve the **streaming upsert problem** in data lakes. Iceberg and Delta are batch-first formats that support streaming as an add-on — they struggle with high-frequency upserts because every update either rewrites large data files (CoW) or accumulates expensive delete files (MoR). Neither handles the small-file explosion from Flink's frequent micro-batch commits well.
>
> Paimon addresses this by using an **LSM-Tree storage engine** — identical to RocksDB/LevelDB — where new writes always append to a buffer, and background compaction merges data asynchronously. This makes upserts O(1) at write time. Additionally, Paimon is the only format with **native changelog semantics** (+I/-U/+U/-D) as a first-class concept, enabling real-time downstream consumers without post-processing.

---

**Q2. Explain the LSM-Tree architecture in Paimon. How does it handle upserts without rewriting files?**

> LSM (Log-Structured Merge-Tree) organizes data in multiple levels:
>
> 1. **MemTable:** incoming writes are buffered in memory in a sorted structure. Upserts for the same key simply overwrite the in-memory entry — no disk I/O.
> 2. **Level 0 (L0):** when the MemTable is full, it's flushed to a small SST (Sorted String Table) file on disk. Multiple L0 files may have overlapping key ranges.
> 3. **Level 1+ (L1, L2...):** background compaction jobs merge and sort L0 files into larger, non-overlapping SST files in deeper levels.
> 4. **Read:** a query scans all levels and merges results using a priority queue — the most recent value per key (from the highest/newest level) wins.
>
> Upserts are fast because they always write to the MemTable / L0 — no existing file is ever modified. Compaction, not individual writes, is responsible for organizing data.

---

**Q3. What is the difference between a Primary Key Table and an Append-Only Table in Paimon?**

> **Primary Key Table:**
> - Defines one or more columns as a primary key
> - Paimon guarantees exactly one record per key in the final read view
> - Upserts, updates, and deletes are handled via the LSM merge process
> - Supports merge functions: deduplicate, partial-update, aggregation, first-row
> - Used for: dimension tables, CDC sinks, aggregation tables, feature stores
>
> **Append-Only Table:**
> - No primary key; every record is stored as-is
> - No deduplication or merging; records are never updated or deleted
> - Storage is more efficient (no LSM merge overhead)
> - Supports bucket-based partitioning for parallelism
> - Used for: event logs, clickstreams, audit trails, raw ingestion landing zones

---

**Q4. What is the Changelog Producer in Paimon? Explain the four options.**

> The changelog producer determines how Paimon generates before/after images for change events:
>
> - **`none`:** no changelog produced. Consumers see only the current state on read. Lowest cost.
> - **`input`:** passes through the changelog from the input stream directly. Requires the source (e.g., a CDC connector) to already produce before/after records. Zero additional computation.
> - **`lookup`:** before writing a new value, Paimon looks up the existing value for the key to produce the before-image. Enables changelog output even when the source doesn't provide it. Medium cost — requires a point lookup per record.
> - **`full-compaction`:** changelogs are generated during compaction by comparing old and new SST files. Highest latency (changelogs appear at compaction time, not write time), but most complete and accurate. Used when exact before/after values are required.

---

**Q5. How does Paimon's partial-update merge function work? What problem does it solve?**

> The `partial-update` merge function allows **multiple independent streams** to write to different columns of the same row without knowing the full row state. When Paimon merges records for the same primary key, it takes the **latest non-null value** for each column across all incoming records.
>
> **Problem it solves:** in a microservices architecture, profile data, behavioral data, and payment data are produced by different services. Without partial update, you'd need to either join all streams into one (complex, brittle) or read-modify-write each row (expensive). With partial update, each service writes only its own columns; Paimon assembles the complete row transparently.
>
> **Limitation:** NULL is used as the "no update" sentinel — a column can't be intentionally set to NULL via partial update.

---

**Q6. What is a bucket in Paimon and how does it affect read and write performance?**

> A bucket is a **unit of parallelism and data locality** within a Paimon table (similar to Hive bucketing). Records are assigned to buckets by hashing the primary key (or a specified bucket key) modulo the bucket count.
>
> **Write:** each bucket is written independently by one Flink task. More buckets → more parallelism → higher write throughput.
>
> **Read:** a point lookup by primary key only needs to read **one bucket** (the hash determines which one). Range queries may need to read all buckets.
>
> **Tuning guidance:**
> - Too few buckets: bottleneck on write parallelism; large SST files per bucket
> - Too many buckets: many small files; overhead on snapshot metadata
> - Rule of thumb: 1 bucket per ~1 GB of expected data per partition
>
> **Special case:** `-1` (dynamic bucket) lets Paimon automatically manage bucket assignment — useful for cross-partition upserts.

---

### 🟡 Intermediate & Design

**Q7. How would you design a real-time CDC pipeline from MySQL into a queryable Paimon lakehouse?**

> **Architecture:**
>
> ```
> MySQL  ──▶  Flink CDC Source (mysql-cdc connector)
>                    │
>                    ▼  (changelog stream: +I/-U/+U/-D)
>             Flink Job (optional: transformations, routing)
>                    │
>                    ▼
>         Paimon Primary Key Tables (one per MySQL table)
>                    │
>         ┌──────────┴───────────┐
>     Trino/Spark             StarRocks
>     (ad-hoc SQL)         (real-time OLAP)
> ```
>
> **Key design decisions:**
> - Use `changelog-producer = 'input'` since the CDC source already produces full changelogs
> - Enable `write.merge-engine = 'deduplicate'` for standard upsert behavior
> - Set bucket count based on expected row count per partition
> - Run a dedicated **Flink compaction job** alongside the ingestion job to keep LSM levels balanced
> - Use Hive Metastore or REST catalog for multi-engine access

---

**Q8. How does Paimon handle the small-file problem inherent in Flink streaming writes?**

> Flink commits checkpoints every few seconds, each producing new L0 SST files. Without compaction, L0 fills up with thousands of tiny files, causing read amplification and slow metadata scans.
>
> Paimon addresses this through **continuous compaction**:
>
> 1. **Inline compaction:** the Flink writer task can trigger compaction automatically after reaching a threshold number of L0 files (`num-levels`, `compaction.min.file-num`, `compaction.max.file-num`)
> 2. **Dedicated compaction job:** a separate Flink job runs `CALL sys.compact(...)` continuously, freeing the write job from compaction overhead
> 3. **Full compaction for changelog:** when `changelog-producer = 'full-compaction'`, compaction is triggered at controlled intervals to produce accurate changelogs
>
> The LSM architecture inherently bounds the number of L0 files — compaction is triggered automatically when the threshold is reached, unlike Iceberg where small files are a purely external concern.

---

**Q9. Explain how Paimon's lookup join works internally. Why is it more efficient than broadcasting a dimension table?**

> In a Flink lookup join, Paimon builds a **local cache** of the dimension table in each task manager:
>
> 1. Paimon reads the dimension table's SST index files (bloom filter + hash index per bucket)
> 2. For each lookup key, Paimon checks the bloom filter to quickly eliminate non-matches
> 3. On a likely match, Paimon reads only the relevant bucket's SST files (not the full table)
> 4. Results are cached in the task manager's memory with a configurable TTL
>
> **Why it's better than broadcasting:**
> - Broadcasting requires loading the ENTIRE dimension table into memory of every task — impractical for large tables (millions of products, customers)
> - Paimon's LSM index + bucket structure means each lookup reads O(log N) data, not O(N)
> - The local cache handles temporal consistency via TTL refresh, not full re-broadcasts
>
> **Limitation:** cache TTL introduces staleness; very high cardinality dimensions with rapid changes may cause cache churn.

---

**Q10. What is the difference between Paimon and Hudi? Both target upserts — when would you pick one over the other?**

> | Dimension | Apache Paimon | Apache Hudi |
> |---|---|---|
> | **Storage engine** | LSM-Tree (database-style) | COW / MOR (file-level) |
> | **Primary engine** | Apache Flink | Apache Spark |
> | **Upsert mechanism** | LSM merge; no file rewrite | CoW: file rewrite; MoR: delta log |
> | **Changelog** | Native (+I/-U/+U/-D) | Via CDF; less native |
> | **Partial update** | Built-in merge function | Not natively supported |
> | **Lookup join** | Native Flink support | Not natively supported |
> | **Streaming read** | First-class | Available but less native |
> | **Batch read engines** | Spark, Trino, Hive | Spark, Trino, Hive (broader) |
> | **Ecosystem maturity** | Growing (newer) | More mature (since 2016) |
>
> **Choose Paimon when:** you're Flink-first, need sub-minute latency, need partial updates or aggregation merge, or need lookup joins against dimension tables.
>
> **Choose Hudi when:** you're Spark-first, need a proven solution at scale, have an established Hudi ecosystem, or need Spark-based CDC with strong MoR semantics.

---

**Q11. How does Paimon's snapshot mechanism differ from Iceberg's?**

> Both use snapshots for time travel and ACID reads, but the internals differ:
>
> **Iceberg:** snapshot is a **manifest list** pointing to manifest files pointing to data files. The three-layer tree is designed for maximum file pruning at query planning time via rich per-column statistics. Snapshots are committed by atomically swapping the metadata JSON pointer.
>
> **Paimon:** snapshot is a pointer to a **manifest list** of LSM SST file references per bucket. Snapshots are committed by writing a new snapshot file and atomically updating the `LATEST` pointer. The LSM structure means snapshot files reference SST files at different compaction levels — a reader must merge multiple levels to reconstruct the current state.
>
> **Key difference:** Paimon snapshots reflect LSM compaction state (levels); Iceberg snapshots reflect flat file additions/removals. Paimon's read requires level merging; Iceberg's read is a straightforward file list scan.

---

### 🔴 Advanced & System Design

**Q12. Design a real-time customer 360 platform using Apache Paimon that aggregates data from 5 independent microservices.**

> **Requirements:** profile, orders, payments, support tickets, and behavioral events — each from a separate service, each updating different attributes of a customer record.
>
> **Architecture:**
>
> ```
> Profile Service    ──▶  Kafka topic: profile-events
> Orders Service     ──▶  Kafka topic: order-events
> Payments Service   ──▶  Kafka topic: payment-events
> Support Service    ──▶  Kafka topic: support-events
> Behavior Service   ──▶  Kafka topic: behavior-events
>
> Each topic ──▶  Dedicated Flink job (lightweight)
>                        │
>                        ▼
>              Paimon customer_360 table (partial-update merge function)
>                        │
>             ┌──────────┴──────────┐
>         Trino (SQL)         StarRocks (real-time dashboard)
> ```
>
> **Key decisions:**
> - `merge-function = 'partial-update'` — each Flink job writes ONLY its service's columns; Paimon assembles the full row
> - `changelog-producer = 'lookup'` — so downstream consumers get before/after images for change propagation
> - Bucket count: ~100 buckets (for ~100M customers, ~1M customers/bucket)
> - Dedicated compaction Flink job to keep LSM balanced across all write streams
> - REST catalog for multi-engine access (Trino + StarRocks + Spark)

---

**Q13. How would you tune a Paimon table for a workload with 1 million upserts per second from Flink?**

> **Write-side tuning:**
> - Increase `write-buffer-size` to buffer more data in memory before flushing to L0 (reduces L0 file count)
> - Set `num-sorted-run.compaction-trigger` to trigger compaction sooner (e.g., 5 L0 files instead of default)
> - Use a **dedicated compaction Flink job** with higher parallelism than the write job
> - Increase bucket count to match write parallelism (e.g., 256 buckets for 256 Flink tasks)
> - Set `compaction.max-size-amplification-percent` to control write amplification vs space amplification trade-off
>
> **Read-side tuning:**
> - Schedule full compaction during off-peak to reduce read-time level merging
> - Enable bloom filters on primary key columns for fast point lookups (`file.bloom-filter-enabled = true`)
> - Use `scan.mode = 'latest-full'` for batch reads to get a clean snapshot without merging too many levels
>
> **Operational:**
> - Monitor L0 file count (high L0 = compaction lag = slower reads)
> - Monitor compaction write amplification (too high = storage cost blow-up)
> - Set snapshot expiry (`snapshot.num-retained.max`) to control metadata overhead

---

**Q14. What are the consistency guarantees when two Flink jobs write to the same Paimon primary key table simultaneously?**

> Paimon uses **optimistic concurrency at the snapshot level**, similar to Iceberg. Each Flink checkpoint generates a commit attempt for a new snapshot:
>
> 1. Both Flink jobs read the current `LATEST` snapshot ID
> 2. Both write their SST files to object storage
> 3. Both attempt to commit the next snapshot ID (atomically update `LATEST`)
> 4. One succeeds; the other detects the conflict and retries with the updated base snapshot
>
> **Conflict resolution for primary key tables:** since each bucket is owned by exactly one writer task and bucket assignment is deterministic (by primary key hash), **two writers writing to different keys never conflict** at the bucket level. Conflicts only arise if two jobs write to the same bucket concurrently.
>
> **Best practice for dual-writer scenarios:**
> - Partition writes by key range between jobs (Job A handles customer_id 1–50M, Job B handles 50M–100M)
> - Or use **partial-update** — two jobs writing different columns of the same key never produce a data conflict, only a snapshot ordering conflict (resolved by retry)

---

**Q15. How does Paimon compare to using Kafka + Iceberg for a streaming lakehouse? When would you use each?**

> **Kafka + Iceberg approach:**
>
> ```
> Source ──▶ Kafka (buffer) ──▶ Flink/Spark ──▶ Iceberg (batch-oriented writes)
> ```
> - Kafka acts as the durable, replayable changelog log; Iceberg stores the queryable state
> - Write latency: minutes (Iceberg compaction + small-file concern)
> - Operational complexity: two systems to manage (Kafka cluster + Iceberg maintenance)
> - Ecosystem: maximum engine support for the Iceberg side
>
> **Paimon approach:**
>
> ```
> Source ──▶ Flink ──▶ Paimon (streaming write + built-in changelog)
> ```
> - Paimon IS the changelog store AND the query store — one system
> - Write latency: seconds
> - Operational complexity: one system; compaction is built-in for Flink
> - Ecosystem: Flink-native; Spark and Trino reads are supported but secondary
>
> **Choose Kafka + Iceberg when:** you need maximum query engine flexibility, have existing Kafka infrastructure, or need long-term event replay (weeks/months).
>
> **Choose Paimon when:** you're Flink-native, need second-level freshness, want to eliminate Kafka for the lakehouse layer, or need partial updates and lookup joins that Iceberg can't natively provide.

---

**Q16. Explain how Paimon's cross-partition upsert works and what the performance implications are.**

> In a partitioned primary key table, a record's partition is usually determined by a partition column (e.g., `event_date`). A standard upsert only searches within the target partition for the existing record.
>
> **The problem:** in CDC scenarios, an UPDATE event may not include the partition column value, or the partition column may have changed (a record moves from partition `2024-01` to `2024-02`). A standard write would create a duplicate in the new partition without deleting the old one.
>
> **How cross-partition upsert works:**
>
> 1. Enable `cross-partition-upsert.enabled = true` on the table
> 2. Paimon maintains a **global secondary index** (`index/` directory) mapping each primary key to its current partition + bucket
> 3. On each upsert, Paimon: (a) looks up the current partition for the key in the global index, (b) issues a delete in the old partition if the partition changed, (c) writes the new record to the correct new partition, (d) updates the global index
>
> **Performance implications:**
> - Each upsert now requires an additional global index read → higher latency per record
> - Global index itself grows with cardinality and must be maintained (adds storage overhead)
> - Not suitable for ultra-high-throughput scenarios (millions of cross-partition updates/second)
> - Best for moderate update rates where data correctness across partitions is critical (e.g., customer records that change status months)

---

*Last updated: 2025 | Apache Paimon version: 0.8.x*
