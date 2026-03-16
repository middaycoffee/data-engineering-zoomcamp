## Module 6: Batch Processing with Apache Spark

This folder contains my work for Module 6 of the Data Engineering Zoomcamp 2026, covering batch processing with PySpark.

### Key Concepts

- Setting up a local Spark environment (JDK 17 + Hadoop on Windows)
- Reading raw CSV data with explicit schemas using PySpark
- Converting CSV to Parquet format with repartitioning
- Spark DataFrame API: `select`, `filter`, `withColumn`
- Built-in Spark functions (`pyspark.sql.functions`)
- User-Defined Functions (UDFs)
- Spark SQL for querying DataFrames

### Project Structure

```
module-6-batch/
├── taxi_schema.ipynb       # Schema definition & CSV → Parquet conversion (Green/Yellow taxi)
├── code/
│   ├── pyspark.ipynb       # Core PySpark intro: reading, filtering, UDFs (FHVHV data)
│   └── spark_sql.ipynb     # Spark SQL queries on taxi data
├── data/
│   ├── raw/                # Downloaded raw CSV files (gitignored)
│   └── pq/                 # Converted Parquet output (gitignored)
└── download_data.sh        # Bash script to download NYC Taxi data by type/year
```

### Prerequisites

- Java JDK 17
- Hadoop binaries (for Windows `winutils.exe`)
- Python 3.x with PySpark installed

### Setup

1. Install dependencies into the virtual environment:
   ```bash
   pip install pyspark pandas jupyter
   ```

2. Set environment variables (already handled in notebooks):
   ```python
   os.environ["JAVA_HOME"] = "C:/tools/jdk-17.0.18+8"
   os.environ["HADOOP_HOME"] = "C:/tools/hadoop"
   ```

### Download Data

Use the provided script to download NYC Taxi data:

```bash
bash download_data.sh yellow 2020
bash download_data.sh green 2020
```

Data is saved to `data/raw/{taxi_type}/{year}/{month}/`.

### Data Source

- [NYC TLC Trip Record Data (DataTalksClub mirror)](https://github.com/DataTalksClub/nyc-tlc-data)
