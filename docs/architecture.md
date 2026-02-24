# Retail Loyalty Analytics Platform

## End-to-End Data Architecture

### 1. High-Level Architecture

This document defines the target architecture for a cloud-based retail loyalty analytics platform in a fintech context. The design emphasizes scalability, governance, security, and operational reliability.

Core principles:
- Layered data architecture (Bronze, Silver, Gold) with clear ownership boundaries.
- Streaming-first ingestion with batch parity for reconciliation and backfill.
- Separation of storage, compute, and serving responsibilities.
- Security by default with least-privilege access controls and strong encryption.
- Cost-aware operation through lifecycle, autoscaling, and workload controls.

### 2. ASCII Architecture Diagram

```text
+---------------------+    +------------------------+    +-----------------------------+
| Source Systems      |    | Ingestion Layer        |    | Processing Layer            |
| POS, eCom, CRM,     |--->| Kafka (streaming)      |--->| PySpark Structured Streaming|
| Mobile App, Loyalty |    | Batch APIs/SFTP/Files  |    | PySpark Batch Jobs          |
+---------------------+    +------------------------+    +-----------------------------+
                                      |                                  |
                                      v                                  v
                           +----------------------+         +----------------------+
                           | S3 Data Lake Bronze  |-------> | S3 Data Lake Silver  |
                           | Raw, immutable,      |         | Cleansed, conformed, |
                           | schema-on-read       |         | validated            |
                           +----------------------+         +----------------------+
                                                                      |
                                                                      v
                                                           +----------------------+
                                                           | S3 Data Lake Gold    |
                                                           | Business-ready marts |
                                                           +----------------------+
                                                                      |
                                                                      v
                                                           +----------------------+
                                                           | Warehouse Layer      |
                                                           | Redshift (cloud)     |
                                                           | Postgres (local dev) |
                                                           | Star schema          |
                                                           +----------------------+
                                                                      |
                                                                      v
                                                           +----------------------+
                                                           | BI / Analytics Layer |
                                                           | Streamlit or React   |
                                                           | KPIs, cohort, churn  |
                                                           +----------------------+

+---------------------------------------------------------------------------------+
| CI/CD, Governance, Monitoring                                                   |
| GitHub Actions | Tests | Data Quality | Lineage | CloudWatch/Alerts | Audit Logs|
+---------------------------------------------------------------------------------+
```

### 3. Layer Design

#### 3.1 Ingestion Layer (Batch + Streaming)

- Streaming ingestion is handled by Kafka topics for high-volume event streams (earn, redeem, transaction, and engagement events).
- Batch ingestion is used for partner feeds, file drops, API pulls, and historical backfills.
- Contract validation is applied at ingress to enforce schema compatibility.
- Malformed records are routed to quarantine/dead-letter datasets for controlled triage.

#### 3.2 Processing Layer (Spark Batch + Streaming)

- PySpark Structured Streaming handles near-real-time normalization and enrichment.
- PySpark batch pipelines handle heavy transformations, late-arriving corrections, and historical rebuilds.
- Common transformation logic is shared between batch and streaming paths to maintain metric parity.
- Data quality checks gate promotion from Bronze to Silver and Silver to Gold.

#### 3.3 Storage Layer (Bronze / Silver / Gold)

- Bronze: raw, immutable, append-only data with full source traceability.
- Silver: standardized, deduplicated, validated, and conformed datasets.
- Gold: business-ready models, aggregates, and subject-area marts optimized for analytics.

#### 3.4 Data Warehouse Layer (Star Schema)

- Redshift is the cloud analytical warehouse for scale and enterprise BI serving.
- Postgres is used locally for lightweight development and semantic model validation.
- Star schema pattern:
  - Fact tables: `fact_loyalty_txn`, `fact_redemption`, `fact_engagement`
  - Dimension tables: `dim_customer` (SCD2), `dim_store`, `dim_product`, `dim_campaign`, `dim_date`

#### 3.5 BI / Analytics Layer

- Streamlit supports rapid internal dashboard and analytics use cases.
- React is suitable for production-grade, externally facing analytics applications.
- BI objects consume curated Gold/warehouse models via governed semantic definitions.

#### 3.6 CI/CD and Monitoring Layer

- GitHub Actions orchestrates validation and deployment pipelines across environments.
- CI gates include linting, unit tests, integration tests, and schema compatibility checks.
- Operational monitoring tracks data freshness, Kafka lag, Spark SLA, and warehouse performance.
- Centralized alerting and audit logging support incident response and compliance.

### 4. Technology Choices and Justification

| Technology | Role | Justification |
|---|---|---|
| Kafka | Streaming backbone | High-throughput event ingestion, partition-based scale, and durable replay support. |
| PySpark | Processing engine | Unified distributed framework for batch and streaming data transformation. |
| AWS S3 | Data lake storage | Durable, low-cost object storage with near-unlimited horizontal scale. |
| Delta Lake (preferred) / Parquet (fallback) | Lake table format | Delta adds ACID transactions, schema enforcement, and time travel; Parquet is broad fallback. |
| Postgres (local) + Redshift (cloud) | Warehouse serving | Postgres supports local development; Redshift supports enterprise cloud analytics at scale. |
| Streamlit or React | Analytics interface | Streamlit for fast internal delivery; React for rich, product-grade analytics UX. |
| GitHub Actions | CI/CD orchestration | Native Git workflow integration with repeatable, policy-enforced delivery pipelines. |

### 5. Data Lake Design

#### 5.1 Folder Structure

- `s3://retail-loyalty-lake/{env}/bronze/{domain}/{dataset}/ingest_date=YYYY-MM-DD/hour=HH/`
- `s3://retail-loyalty-lake/{env}/silver/{domain}/{entity}/event_date=YYYY-MM-DD/`
- `s3://retail-loyalty-lake/{env}/gold/{subject_area}/{mart}/snapshot_date=YYYY-MM-DD/`
- `s3://retail-loyalty-lake/{env}/_checkpoints/{pipeline}/`
- `s3://retail-loyalty-lake/{env}/_quarantine/{dataset}/`
- `s3://retail-loyalty-lake/{env}/_metadata/contracts/`

#### 5.2 Partition Strategy

- Bronze partitions: `ingest_date`, `hour`, and source identifier.
- Silver partitions: `event_date` plus low-cardinality analytical dimension (for example `program_id` or `region`).
- Gold partitions: reporting date plus subject-specific access dimension.
- Target post-compaction file sizes: 128 MB to 512 MB for balanced throughput and query efficiency.

#### 5.3 File Format Justification

- Use Delta Lake where available for ACID writes, robust upserts/merges, schema enforcement, and rollback capability.
- Use Parquet where Delta runtime is unavailable, with explicit metadata controls and disciplined schema/version management.
- Columnar compression reduces storage and improves query scan efficiency.

#### 5.4 Schema Evolution Strategy

- Contract-first schema governance at source and ingestion boundaries.
- Additive (backward-compatible) changes are auto-accepted where safe.
- Breaking changes require versioned datasets/topics and phased consumer migration.
- Compatibility checks are enforced in CI/CD before production promotion.

### 6. Scalability Strategy

#### 6.1 Horizontal Scaling Approach

- Kafka scales through partition increases and broker scaling.
- Spark scales through executor autoscaling and workload isolation by criticality.
- S3 scales elastically by design.
- Redshift scales through node sizing and concurrency scaling.

#### 6.2 Partition Sizing Strategy

- Design partitions for predictable query pruning and balanced data distribution.
- Run periodic compaction to correct small-file proliferation.
- Apply salting/hash distribution for skewed high-cardinality keys.

#### 6.3 Handling 100M+ Records/Day

- Capacity model targets sustained ingestion above 100M records/day with burst headroom.
- Streaming pipelines maintain bounded latency through topic partitioning and consumer parallelism.
- Incremental processing and merge/upsert patterns avoid full-table rewrites.
- SLA classes differentiate near-real-time, hourly, and daily workloads.

### 7. Security Design

#### 7.1 IAM Least Privilege

- Separate roles per environment (`dev`, `stage`, `prod`) and per function (ingest, transform, serve).
- Explicitly scoped permissions with deny-by-default posture.
- No direct write access from consumers to upstream raw layers.

#### 7.2 Encryption at Rest

- S3 encryption via SSE-KMS.
- Redshift encryption enabled with managed key controls.
- Encrypted transport (TLS) enforced for Kafka clients, Spark connectors, and BI access.

#### 7.3 Secrets Management

- Secrets stored in AWS Secrets Manager with rotation policies.
- No secrets in code repositories, pipeline definitions, or plain-text config artifacts.

#### 7.4 Data Masking for PII

- Tokenization or irreversible hashing for direct identifiers in Silver/Gold when full identity is not required.
- Restrict clear-text PII access to controlled zones and audited service accounts.
- Apply column-level and row-level controls in warehouse and BI serving paths.

### 8. Cost Awareness

#### 8.1 Storage Cost Considerations

- Lifecycle policies move cold data from standard tiers to lower-cost tiers based on age and access patterns.
- Compression and compaction reduce footprint and downstream compute scan costs.
- Bronze retention is optimized for replay and compliance without indefinite hot storage.

#### 8.2 Compute Cost Controls

- Autoscaling and scheduled shutdown for non-production and non-critical workloads.
- Spot capacity for tolerant batch pipelines; on-demand for critical streaming workloads.
- Query controls and workload management in warehouse to cap runaway costs.

#### 8.3 Logging Retention Policy

- Application and operational logs: 30 days.
- Pipeline and data quality logs: 90 days.
- Security and audit logs: 365 days (or stricter regulatory requirement).
- Archived logs transition to lower-cost object storage tiers.

### 9. Implementation Notes

- This architecture supports current delivery and future expansion into advanced use cases such as real-time personalization and ML feature serving.
- Platform governance should be formalized through data contracts, ownership registry, and incident playbooks.
- Environment promotion should require automated validation, lineage checks, and rollback readiness.
