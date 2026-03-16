# Data Engineering Zoomcamp (2026)

*This repository contains my code, notes, and architectural experiments completed during the [2026 Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) hosted by DataTalksClub.*

### Tech Stack & Architecture

This repository covers the complete lifecycle of a modern data pipeline:

* **Infrastructure as Code (IaC):** Terraform, Docker
* **Workflow Orchestration:** Kestra, Bruin
* **Data Warehousing:** Google BigQuery
* **Data Transformation:** dbt (Data Build Tool)
* **Streaming & Processing:** Kafka
* **Languages:** Python, SQL, Jupyter Notebooks

---

## Repository Structure

### [Module 1: Docker & Infrastructure Setup](./module-1-docker)
Focuses on containerizing data environments and setting up local infrastructure.
* **Key Concepts:** Docker, Postgres, Terraform.

### [Module 2: Workflow Orchestration](./module-2-kestra)
Explores automated scheduling, dependency management, and pipeline execution.
* **Key Concepts:** Kestra, API data ingestion, Google Cloud Storage (GCS) integration.

### [Module 3: Data Warehousing](./module-3-bigquery)
Deep dive into cloud-native analytical databases and performance optimization.
* **Key Concepts:** Google BigQuery, partitioning, clustering, external tables, and optimized SQL queries.

### [Module 4: Analytics Engineering](./module-4-dbt)
Transforms raw data into clean, production-ready models.
* **Key Concepts:** dbt (Data Build Tool), data modeling, testing, macros, and staging/core layers.

### [Module 5: Data Orchestration with Bruin](./module-5-bruin)
Focuses on defining, running, and orchestrating data assets using Python and SQL.
* **Key Concepts:** Bruin, pipeline orchestration, data assets, dependency tracking.

### [Workshop 1: Data Ingestion with dlt](./workshop-1-dlt)
Covers the creation of an end-to-end data pipeline ingesting data from a paginated API into a local database.
* **Key Concepts:** dlt (data load tool), DuckDB, Marimo, paginated JSON APIs.

### [Module 6: Batch Processing with PySpark](./module-6-batch/)
Examines distributed batch processing using PySpark on NYC Taxi datasets.
* **Key Concepts:** DataFrame transformations, Jupyter, reading and writing Parquet, repartitioning, StructType.
---

## How to Use This Repository

Because this is a monorepo, each module contains its own isolated environment and instructions. 

To explore a specific concept:
1. Clone the repository:
   ```bash
   git clone https://github.com/middaycoffee/data-engineering-zoomcamp.git
   cd data-engineering-zoomcamp
2. Navigate into the specific module folder (e.g., `cd module-1-docker`).

3. Follow the isolated README.md instructions inside that specific folder to spin up the local containers or connect to the cloud resources.
