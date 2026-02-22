# Module 2: Workflow Orchestration with Kestra

This folder contains the solution for the Module 2 homework. It implements a fully containerized ETL pipeline using Kestra, PostgreSQL, and Docker.

## 🛠️ Architecture

The pipeline consists of two main flows:

1.  **`02_postgres_taxi`**: The generic worker flow.
    * **Extract:** Downloads taxi data (Yellow/Green) from GitHub.
    * **Transform:** Uncompresses and cleans the data.
    * **Load:** Uses a "Staging Table" + `MERGE` operation to ensure idempotency (preventing duplicates).
2.  **`03_taxi_scheduler`**: The orchestration flow.
    * Iterates through a list of months.
    * Triggers the worker flow for each month sequentially.

## 🚀 How to Run

### 1. Start the Containers
This setup requires running Kestra as `root` to allow Docker-in-Docker interaction for file handling.

```bash
docker-compose up -d