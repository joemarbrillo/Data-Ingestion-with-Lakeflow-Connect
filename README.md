# Welcome to my Data Ingestion with Lakeflow Connect

### This is sponsored by Databricks Academy.

## Info
This contains my note-taking activities during the course.

## Lesson 1 Summary:
In this lecture, you learned the fundamentals of data engineering in Databricks with a focus on LakeFlow Connect and Delta Lake:

1. LakeFlow Connect is the ingestion layer within the Databricks Data Intelligence Platform that replaces the traditional patchwork of ingestion tools with a unified, managed solution.
2. LakeFlow Connect provides three types of connectors: Upload Files, Standard Connectors, and Managed Connectors, each designed for different data source types.
3. Three ingestion methods are available: Batch (processes all records), Incremental Batch (processes only new records), and Streaming (continuous, near real-time ingestion).
4. Delta Lake is an open-source protocol that stores data as Parquet files with transaction logs, enabling ACID transactions, time travel, DML support, and schema enforcement.
5. The Medallion Architecture (Bronze, Silver, Gold) progressively improves data quality as it moves through each layer.