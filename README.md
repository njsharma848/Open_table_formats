# 🗄️ Open Table Formats

> A comprehensive guide and reference repository for open lakehouse table formats — covering Apache Iceberg, Delta Lake, Apache Hudi, Apache Paimon, and more.

---

## 📖 Table of Contents

- [Overview](#overview)
- [Why Open Table Formats?](#why-open-table-formats)
- [Formats at a Glance](#formats-at-a-glance)
- [Apache Iceberg](#apache-iceberg)
- [Delta Lake](#delta-lake)
- [Apache Hudi](#apache-hudi)
- [Apache Paimon](#apache-paimon)
- [Apache XTable (Interoperability)](#apache-xtable-interoperability)
- [Feature Comparison](#feature-comparison)
- [Choosing the Right Format](#choosing-the-right-format)
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Open table formats are a foundational layer of the modern **data lakehouse** architecture. They bring database-like capabilities — ACID transactions, schema evolution, time travel, and efficient querying — directly to files stored in cloud object stores (S3, GCS, ADLS) or HDFS, without locking you into a proprietary system.

This repository serves as a central reference for understanding, comparing, and working with the major open table formats in the ecosystem.

---

## Why Open Table Formats?

Traditional data lakes store raw files (Parquet, ORC, Avro) with no management layer. This leads to:

- ❌ No ACID guarantees — concurrent writes cause corruption
- ❌ No schema enforcement — bad data silently breaks pipelines
- ❌ Expensive full-table scans — no metadata pruning
- ❌ No versioning — mistakes are irreversible

Open table formats solve all of this by introducing a **metadata layer** on top of your files:

- ✅ **ACID Transactions** — safe concurrent reads and writes
- ✅ **Time Travel** — query historical snapshots of your data
- ✅ **Schema Evolution** — add, rename, or drop columns safely
- ✅ **Partition Evolution** — change partitioning without rewriting data
- ✅ **Data Skipping** — skip irrelevant files using metadata statistics
- ✅ **Multi-Engine Support** — query with Spark, Flink, Trino, DuckDB, and more

---

## Formats at a Glance

| Format | Created By | Year | Primary Strength | Best Engine Fit |
|---|---|---|---|---|
| [Apache Iceberg](#apache-iceberg) | Netflix → Apache | 2018 | General-purpose, broadest ecosystem | Spark, Trino, Flink, DuckDB |
| [Delta Lake](#delta-lake) | Databricks → Linux Foundation | 2019 | Databricks ecosystem, maturity | Apache Spark |
| [Apache Hudi](#apache-hudi) | Uber → Apache | 2016 | Upserts, CDC, incremental pipelines | Apache Spark, Flink |
| [Apache Paimon](#apache-paimon) | Apache (Alibaba roots) | 2022 | Streaming-first, real-time ingestion | Apache Flink |

---

## Apache Iceberg

**Website:** https://iceberg.apache.org  
**GitHub:** https://github.com/apache/iceberg

Apache Iceberg is a high-performance open table format for huge analytic datasets. Originally built at Netflix to solve massive-scale challenges, it is now the most widely adopted open table format and is supported natively by virtually every major cloud data platform.

### Key Features

- **Hidden Partitioning** — partition data without exposing it to query writers; Iceberg handles pruning automatically
- **Partition Evolution** — change the partition scheme of a table without rewriting existing data
- **Schema Evolution** — safely add, drop, rename, reorder, and widen columns
- **Time Travel & Rollback** — query any historical snapshot; roll back to a previous state
- **Row-level Deletes** — supports position and equality deletes for efficient updates
- **Branching & Tagging** — create named branches and tags on your table history (like Git for data)

### Architecture

```
Table
├── metadata/
│   ├── v1.metadata.json       ← Table metadata (schema, partition spec, snapshots)
│   ├── v2.metadata.json
│   └── snap-*.avro            ← Manifest lists
├── data/
│   └── *.parquet              ← Actual data files
└── manifests/
    └── *.avro                 ← Manifest files (file-level stats)
```

### Engine Support

| Engine | Read | Write |
|---|---|---|
| Apache Spark | ✅ | ✅ |
| Apache Flink | ✅ | ✅ |
| Trino / Presto | ✅ | ✅ |
| DuckDB | ✅ | ✅ |
| StarRocks | ✅ | ✅ |
| Snowflake | ✅ | ✅ |
| BigQuery | ✅ | ✅ |

### Quick Start (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/warehouse") \
    .getOrCreate()

# Create a table
spark.sql("""
  CREATE TABLE local.db.events (
    id BIGINT,
    event_type STRING,
    ts TIMESTAMP
  ) USING iceberg
  PARTITIONED BY (days(ts))
""")

# Time travel
spark.sql("SELECT * FROM local.db.events VERSION AS OF 1")
```

---

## Delta Lake

**Website:** https://delta.io  
**GitHub:** https://github.com/delta-io/delta

Delta Lake was created by Databricks and open-sourced under the Linux Foundation in 2019. It is the native table format of the Databricks Lakehouse Platform and is deeply integrated with Apache Spark. Delta Lake has a large, mature ecosystem with extensive tooling.

### Key Features

- **ACID Transactions** — full serializable isolation for reads and writes
- **Scalable Metadata Handling** — uses Spark itself to process large transaction logs
- **Time Travel** — query and restore previous versions via `VERSION AS OF` or `TIMESTAMP AS OF`
- **Schema Enforcement & Evolution** — strict schema-on-write with controlled evolution
- **Z-Ordering** — multi-dimensional clustering for efficient data skipping
- **Change Data Feed (CDF)** — stream row-level changes out of a Delta table
- **Liquid Clustering** — newer automatic, incremental clustering (replaces Z-ordering)

### Architecture

```
delta-table/
├── _delta_log/
│   ├── 00000000000000000000.json   ← Transaction log entries (JSON)
│   ├── 00000000000000000001.json
│   └── 00000000000000000010.checkpoint.parquet  ← Checkpoint (every N commits)
└── part-*.parquet                  ← Data files
```

### Quick Start (PySpark)

```python
# Write a Delta table
df.write.format("delta").save("/tmp/delta-table")

# Read with time travel
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/tmp/delta-table")

# Optimize with Z-Ordering
spark.sql("OPTIMIZE delta.`/tmp/delta-table` ZORDER BY (user_id)")
```

---

## Apache Hudi

**Website:** https://hudi.apache.org  
**GitHub:** https://github.com/apache/hudi

Apache Hudi (Hadoop Upserts Deletes and Incrementals) was created at Uber to handle massive-scale upsert workloads and was open-sourced in 2017. It is purpose-built for **incremental data processing** and **Change Data Capture (CDC)** pipelines, making it ideal for streaming ingestion from databases and event logs.

### Key Features

- **Upsert-Optimized** — efficient record-level upserts using primary keys and precombine fields
- **Two Table Types** — Copy-on-Write (read-optimized) and Merge-on-Read (write-optimized)
- **Incremental Queries** — pull only the records that changed since a given commit
- **Compaction** — background merging of log files into columnar files (MoR tables)
- **Clustering** — reorganize data for better read performance
- **Multi-Modal Index** — Bloom filter, simple, bucket, HBase, and metadata table indexes

### Table Types

| Type | Write Performance | Read Performance | Best For |
|---|---|---|---|
| **Copy-on-Write (CoW)** | Slower (rewrites files) | Fastest | Read-heavy, batch pipelines |
| **Merge-on-Read (MoR)** | Fastest (appends logs) | Slightly slower | Write-heavy, streaming, CDC |

### Quick Start (PySpark)

```python
# Upsert into a Hudi table
df.write.format("hudi") \
    .option("hoodie.table.name", "events") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "updated_at") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save("/tmp/hudi-table")

# Incremental query
spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240101000000") \
    .load("/tmp/hudi-table")
```

---

## Apache Paimon

**Website:** https://paimon.apache.org  
**GitHub:** https://github.com/apache/paimon

Apache Paimon is a **streaming-first** lakehouse table format that enables building real-time data lakes with high-throughput streaming writes and low-latency queries. Originally incubated as Flink Table Store, it graduated to a top-level Apache project and is ideal for teams building on Apache Flink.

### Key Features

- **Streaming-First Design** — native support for streaming reads and writes via Flink CDC
- **LSM-Tree Storage** — Log-Structured Merge-Tree for efficient high-throughput writes
- **Changelog Mode** — produces changelog streams for downstream consumers
- **Lookup Joins** — efficient streaming lookup joins against Paimon tables
- **Append and Primary Key Tables** — flexible table types for different workloads
- **Cross-Partition Upserts** — update records across partition boundaries

### Quick Start (Flink SQL)

```sql
-- Create a Paimon table
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY NOT ENFORCED,
    customer_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'paimon',
    'path' = 's3://my-bucket/paimon/orders'
);

-- Stream inserts
INSERT INTO orders SELECT * FROM kafka_orders_source;
```

---

## Apache XTable (Interoperability)

**Website:** https://xtable.apache.org  
**GitHub:** https://github.com/apache/incubator-xtable

Apache XTable (formerly OneTable, incubated at Apache) is **not a table format itself** — it is a **metadata translation layer** that converts table metadata between Iceberg, Delta Lake, and Hudi, enabling true multi-format interoperability without data movement.

### How It Works

```
Source Format (e.g., Iceberg)
        │
        ▼
   XTable Sync
        │
        ├──▶ Delta Lake metadata (generated)
        └──▶ Hudi metadata (generated)
```

Your data files remain unchanged. XTable generates the metadata each format needs to read those files natively.

---

## Feature Comparison

| Feature | Iceberg | Delta Lake | Hudi | Paimon |
|---|---|---|---|---|
| **ACID Transactions** | ✅ | ✅ | ✅ | ✅ |
| **Time Travel** | ✅ | ✅ | ✅ | ✅ |
| **Schema Evolution** | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **Partition Evolution** | ✅ | ⚠️ Limited | ⚠️ Limited | ✅ |
| **Row-Level Deletes** | ✅ | ✅ | ✅ | ✅ |
| **Upserts** | ✅ | ✅ | ✅ (optimized) | ✅ (optimized) |
| **Streaming Writes** | ✅ | ✅ | ✅ | ✅ (native) |
| **Incremental Reads** | ✅ | ✅ (CDF) | ✅ (native) | ✅ (changelog) |
| **Data Skipping** | ✅ | ✅ | ✅ | ✅ |
| **Multi-Engine Support** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Catalog Integration** | Broad | Good | Good | Good |
| **Community Size** | Large | Large | Large | Growing |
| **Cloud-Native Support** | Broadest | Strong | Strong | Growing |

---

## Choosing the Right Format

```
Are you on Databricks?
    └─ Yes ──▶ Delta Lake (native, best-in-class tooling)
    └─ No
        │
        Do you need high-throughput streaming upserts / CDC?
            └─ Yes ──▶ Apache Hudi (MoR) or Apache Paimon (if Flink-first)
            └─ No
                │
                Do you need maximum engine portability?
                    └─ Yes ──▶ Apache Iceberg (broadest support)
                    └─ No ──▶ Apache Iceberg (still a safe default)
```

**General guidance:**

- 🥇 **Apache Iceberg** — best default for new projects; broadest ecosystem, vendor-neutral
- 🏢 **Delta Lake** — best if you're invested in Databricks or the Spark ecosystem
- 🔄 **Apache Hudi** — best for CDC pipelines, database replication, and upsert-heavy workloads
- ⚡ **Apache Paimon** — best for Flink-native, streaming-first architectures
- 🔀 **Apache XTable** — use when you need to bridge multiple formats in the same lake

---

## Getting Started

### Prerequisites

- Java 8+ or Python 3.8+
- Apache Spark 3.x (for most examples)
- Apache Flink 1.17+ (for Paimon examples)

### Repository Structure

```
open-table-formats/
├── docs/                    # Deep-dive documentation per format
│   ├── iceberg/
│   ├── delta-lake/
│   ├── hudi/
│   └── paimon/
├── examples/                # Runnable code examples
│   ├── spark/
│   ├── flink/
│   └── trino/
├── benchmarks/              # Performance comparison benchmarks
├── docker/                  # Docker Compose environments for local testing
│   ├── iceberg/
│   └── hudi/
└── notebooks/               # Jupyter notebooks for exploration
```

### Run Locally

```bash
# Clone the repository
git clone https://github.com/your-org/open-table-formats.git
cd open-table-formats

# Start a local Spark + Iceberg environment
docker-compose -f docker/iceberg/docker-compose.yml up

# Run an example
cd examples/spark
./run_iceberg_example.sh
```

---

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

- 🐛 **Bug reports** — open an issue with a reproducible example
- 📝 **Documentation** — fixes, additions, and translations are appreciated
- 💡 **Examples** — new runnable examples for any format or engine
- 📊 **Benchmarks** — reproducible performance comparisons

Please follow the existing code style and include tests where applicable.

---

## Resources

| Format | Docs | Slack / Community |
|---|---|---|
| Apache Iceberg | [iceberg.apache.org](https://iceberg.apache.org) | [Apache Iceberg Slack](https://apache-iceberg.slack.com) |
| Delta Lake | [delta.io](https://delta.io) | [Delta Users Slack](https://go.delta.io/slack) |
| Apache Hudi | [hudi.apache.org](https://hudi.apache.org) | [Apache Hudi Slack](https://apache-hudi.slack.com) |
| Apache Paimon | [paimon.apache.org](https://paimon.apache.org) | [Apache Paimon Slack](https://the-asf.slack.com) |

---

## License

This repository is licensed under the [Apache License 2.0](./LICENSE).

All referenced projects (Iceberg, Delta Lake, Hudi, Paimon) are independent open source projects with their own respective licenses.

---

<p align="center">
  Made with ❤️ for the open data lakehouse community
</p>
