# Module 3 Homework: Data Warehousing & BigQuery

This repository contains the SQL queries and solutions for the Module 3 homework of the Data Engineering Zoomcamp.

## Resource Links
- **Data Source:** [NYC Taxi Data (Yellow Taxi Records Jan 2024 - Jun 2024)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## BigQuery Setup

Before running the homework queries, the following setup was performed to load the data into BigQuery.

```sql
-- 1. Create an external table referring to GCS path
CREATE OR REPLACE EXTERNAL TABLE `your_project.your_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket-name/yellow_tripdata_2024-*.parquet']
);

-- 2. Create a non-partitioned table from external table
CREATE OR REPLACE TABLE `your_project.your_dataset.yellow_tripdata_non_partitioned` AS
SELECT * FROM `your_project.your_dataset.external_yellow_tripdata`;

-- 3. Counting records
SELECT COUNT(*) FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`;

--- 4. Data Read Estimation
-- External Table Query
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `your_project.your_dataset.external_yellow_tripdata`;

-- Materialized Table Query
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`;

--- 5. Counting zero fare trips
SELECT count(*) 
FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;

--- 6. Partition Benefits
-- Scan on Non-Partitioned Table
SELECT DISTINCT VendorID
FROM `your_project.your_dataset.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Scan on Partitioned Table
SELECT DISTINCT VendorID
FROM `your_project.your_dataset.yellow_tripdata_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';