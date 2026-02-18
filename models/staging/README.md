---

```markdown
# 🦠 Antimicrobial Resistance (AMR) Data Pipeline
**Data Engineering Zoomcamp 2026 - Capstone Project**

## 📖 Project Overview
Antibiotic resistance is a growing global health crisis. This project is an end-to-end Data Engineering ELT pipeline that tracks the global spread and genetic mutations of bacterial "superbugs". 

By extracting daily updated metadata from the **NCBI Pathogen Detection Project**, this pipeline processes geospatial and genetic data to visualize outbreak clusters and resistance trends, utilizing a highly cost-optimized, zero-footprint cloud architecture.

## 🏗️ Architecture & Technologies
This project strictly follows the **ELT (Extract, Load, Transform)** pattern.

* **Data Source:** NCBI Pathogen Detection API
* **Infrastructure as Code (IaC):** Terraform
* **Workflow Orchestration:** Kestra (via Docker)
* **Data Lake:** Google Cloud Storage (GCS)
* **Data Warehouse:** Google BigQuery
* **Data Transformation:** dbt (Data Build Tool)
* **Visualization:** Looker Studio / Metabase

**Architecture Flow:**
`NCBI API` ➔ `Kestra (Python)` ➔ `GCS (Raw Data)` ➔ `BigQuery (Native Tables)` ➔ `dbt (Modeling)` ➔ `Dashboard`

---

## 💾 Dataset
* **Source:** National Center for Biotechnology Information (NCBI)
* **Format:** Tabular Metadata (TSV/CSV) & JSON
* **Optimization:** To adhere to GCP Free-Tier limits, Kestra utilizes **Targeted API Extraction**. Instead of downloading terabytes of raw FASTA DNA sequences, the pipeline filters at the source, downloading only the highly compressed tabular metadata and specific mutated gene names.

---

## 🚀 Setup & Execution Instructions

### 1. Cloud Infrastructure Setup (Terraform)
Navigate to the `terraform/` directory to build the GCP infrastructure.
```bash
cd terraform/

# Initialize Terraform and download Google Cloud providers
terraform init

# Preview the infrastructure changes
terraform plan

# Deploy the GCS Bucket and BigQuery Dataset
terraform apply

```

### 2. Data Extraction & Loading (Kestra)

Start your local Kestra cluster using Docker, then run the extraction flow.

```bash
# Start Kestra locally
docker-compose up -d

# Inside the Kestra UI, execute the `amr_daily_extraction` flow
# This runs the Python script to fetch NCBI data, dumps it to GCS, and loads it to BigQuery

```

### 3. Data Transformation (dbt)

Navigate to the `dbt/` directory. Ensure your `profiles.yml` is connected to your BigQuery project.

**Install Dependencies:**
First, install the `dbt_utils` package for surrogate key generation.

```bash
dbt deps

```

**Run the Transformations:**
Execute the full pipeline (Seeds ➔ Staging ➔ Intermediate ➔ Marts).

```bash
dbt build

```

---

## 🧩 dbt SQL Modeling

Here is a breakdown of the core transformation logic used in the dbt pipeline.

### Step 1: Source Declaration (`models/staging/_sources.yml`)

```yaml
version: 2

sources:
  - name: raw_amr_data
    description: Raw metadata extracted from NCBI
    database: de-zoomcamp-capstone-project # Replace with your GCP Project ID
    schema: ncbi_raw_lake
    tables:
      - name: pathogen_metadata

```

### Step 2: Staging Layer (`models/staging/stg_pathogen_metadata.sql`)

Cleans raw column names, handles null values, and casts data types.

```sql
{{ config(materialized='view') }}

SELECT
    isolate_id AS target_id,
    scientific_name AS pathogen_name,
    CAST(collection_date AS DATE) AS collection_date,
    COALESCE(geo_loc_name, 'Unknown') AS location_country,
    isolation_source AS sample_source,
    AMR_genotypes AS resistance_genes
FROM {{ source('raw_amr_data', 'pathogen_metadata') }}
WHERE isolate_id IS NOT NULL

```

### Step 3: Core Fact Table (`models/marts/fct_outbreaks.sql`)

Generates unique IDs and aggregates the outbreak data for the visualization layer.

```sql
{{ config(
    materialized='table',
    partition_by={
      "field": "collection_date",
      "data_type": "date",
      "granularity": "month"
    }
) }}

WITH staged_data AS (
    SELECT * FROM {{ ref('stg_pathogen_metadata') }}
)

SELECT
    -- Generate unique surrogate key using dbt_utils
    {{ dbt_utils.generate_surrogate_key(['target_id', 'pathogen_name', 'collection_date']) }} AS outbreak_id,
    
    target_id,
    pathogen_name,
    collection_date,
    location_country,
    sample_source,
    
    -- Feature Engineering: Flag high-risk superbugs
    CASE 
        WHEN resistance_genes LIKE '%blaNDM%' OR resistance_genes LIKE '%mcr-1%' THEN TRUE 
        ELSE FALSE 
    END AS is_high_risk_superbug

FROM staged_data
WHERE collection_date >= '2020-01-01'

```

---

## 📊 Dashboard & Visualization

The final tables are connected to Looker Studio.
**Key Dashboard Metrics:**

1. Global heat map of isolate collection sites.
2. Time-series chart tracking the emergence of specific high-risk resistance genes over the last 5 years.
3. Breakdown of pathogens by source (e.g., Clinical, Environmental, Agricultural).

---

*Developed by Barış Aslan for the 2026 Data Engineering Zoomcamp.*

```
