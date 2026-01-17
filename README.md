# Batch Food Sales Analytics Platform

## 📌 Project Overview

**Batch Food Sales Analytics Platform** is a pet project that demonstrates end-to-end **batch data processing** using **Apache Spark**.  
The project processes food (pizza) sales data from multiple structured and semi-structured sources, performs transformations and analytics in batch mode, and stores results in Parquet files and a relational database.

The project fully covers:
- Reading structured and semi-structured data
- Batch processing with partitioning and distributed execution
- Spark-based analytics
- Batch job orchestration and scheduling
- Data collection handling, logging, and error management

---

## 🎯 Project Goals

- Read data from different data sources (CSV, JSON, Parquet, JDBC).
- Process data in batch mode using Apache Spark.
- Apply transformations, aggregations, and joins.
- Answer analytical business questions using Spark.
- Store processed results efficiently.
- Orchestrate and monitor batch jobs.
- Implement logging, error handling, and basic security practices.

---

## 🏗 Architecture & Data Flow
```
Raw Data Sources
├── CSV (pizza sales dataset)
├── JSON (pizza ingredients metadata)
├── Parquet (previous batch results)
└── Relational Database (PostgreSQL via JDBC)
    ↓
Apache Spark (Batch Processing)
Validation
Partitioning
Transformations
Aggregations
  ↓
Processed Outputs
├── Parquet files (analytics results)
├── Relational DB tables (aggregated summaries)
└── Logs & execution metrics
```

---

## 🧱 Block 1: Reading Structured & Semi-Structured Data

### Description
The application reads data from multiple structured and semi-structured sources using Apache Spark.

### Data Sources
- CSV files (pizza sales data)
- JSON files (ingredients metadata)
- Parquet files (processed results)
- Relational database (PostgreSQL via JDBC)
- Optional ODBC-based data source

### Implementation Steps
1. Create and configure a SparkSession.
2. Read CSV files with explicit schema definition.
3. Read JSON files with nested structures.
4. Read Parquet files for historical data.
5. Read relational database tables using JDBC.
6. Validate schemas and data availability.
7. Handle errors and log read operations.

### Acceptance Criteria
- Spark successfully reads CSV, JSON, and Parquet files.
- JDBC connection to a relational database works correctly.
- Errors such as missing files or schema mismatches are handled gracefully.
- Logs indicate successful or failed data loading.

---

## 🧱 Block 2: Simple Batch Processing Application

### Description
A batch processing Spark application that processes data end-to-end from raw input to transformed output.

### Implementation Steps
1. Execute Spark in batch mode (non-streaming).
2. Partition input data for parallel processing.
3. Filter data based on business conditions (e.g., date ranges).
4. Join multiple datasets (CSV + JSON).
5. Perform aggregations and transformations.
6. Write processed results to storage.

### Acceptance Criteria
- The application runs as a batch job.
- Data partitioning is applied.
- Distributed processing is enabled (Spark local mode).
- Concurrency is utilized via Spark executors.
- Data flows correctly from source to output.

---

## 🧱 Block 3: Spark Application (Analytical Tasks)

### Description
A Spark application that answers specific business questions using the provided Pizza Sales dataset.

### Business Questions
1. How many `cali_ckn` pizzas were ordered on **2015-01-04**?
2. What ingredients does the pizza ordered on **2015-01-02 at 18:27:50** have?
3. What is the most sold pizza category between **2015-01-01** and **2015-01-08**?

### Implementation Steps
1. Load the Pizza Sales CSV dataset into a DataFrame.
2. Filter data by date and time.
3. Aggregate and count pizza orders.
4. Join sales data with ingredients metadata.
5. Trigger Spark actions to compute results.
6. Save results to Parquet format.

### Acceptance Criteria
- Spark session runs in local mode.
- CSV data is loaded into a DataFrame.
- Transformations and actions are applied.
- Results are saved as Parquet files.
- Errors are handled and logged.

---

## 🧱 Block 4: Execute Batch Processing Jobs (Orchestration)

### Description
Batch job orchestration, scheduling, and monitoring.

### Implementation Steps
1. Create a shell or Python script to run Spark jobs.
2. Pass runtime parameters (date ranges, input/output paths).
3. Configure job scheduling (cron or Airflow).
4. Define execution order of batch tasks.
5. Capture execution logs and job status.

### Acceptance Criteria
- Batch jobs can be started via scripts.
- Jobs accept configurable parameters.
- Scheduling is configured and documented.
- Logs track execution status and errors.
- Failures are detected and reported.

---

## 🧱 Block 5: Working with Data Collections in Batch Processing

### Description
Handling large datasets efficiently with transformations, monitoring, and security practices.

### Implementation Steps
1. Apply partitioning strategies for large datasets.
2. Use filtering, mapping, and aggregation operations.
3. Join datasets and perform advanced transformations.
4. Manage data flow between processing stages.
5. Capture runtime metrics (row counts, execution time).
6. Secure credentials using environment variables.
7. Implement retry logic and error alerts.

### Acceptance Criteria
- Large datasets are processed efficiently.
- Data transformations are correctly applied.
- Data flow between stages is controlled and optimized.
- Errors are logged and handled properly.
- Sensitive credentials are not hardcoded.
- Runtime metrics are recorded.

---

## 📁 Project Structure
```
batch-food-analytics/
│
├── data/
│ ├── raw/
│ │ ├── pizza_sales.csv
│ │ └── ingredients.json
│ ├── processed/
│ └── parquet/
│
├── spark_jobs/
│ ├── read_sources.py
│ ├── transformations.py
│ ├── aggregations.py
│ └── main.py
│
├── orchestration/
│ ├── run_batch.sh
│ └── schedule.cron
│
├── logs/
│ └── batch.log
│
├── config/
│ └── application.yml
│
└── README.md
```


---

## 🛠 Technologies Used

- Apache Spark
- Python (PySpark)
- PostgreSQL (JDBC)
- Parquet
- Bash / cron (or Airflow)
- Logging frameworks

---

## ✅ Final Outcome

This project demonstrates:
- End-to-end batch processing
- Distributed data processing with Spark
- Structured and semi-structured data handling
- Job orchestration and monitoring
- Error handling, logging, and security best practices

It is suitable for:
- Academic evaluation
- Technical interviews
- Data engineering portfolio (GitHub)

