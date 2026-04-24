# Healthcare Insurance Data System

## Overview

This project implements an end-to-end data pipeline for a healthcare insurance company. The objective is to process large volumes of data, perform data cleaning and transformations using PySpark, and generate analytical insights that support business decision-making.

The processed data is stored in Amazon S3 and loaded into Amazon Redshift for querying and reporting.

---

## Architecture

S3 → PySpark → S3 → Redshift

* **Amazon S3** is used for storing raw and processed data.
* **PySpark** is used for data cleaning and implementing business logic.
* **Amazon Redshift** is used as a data warehouse for storing final results.
* **Databricks** is used for visualization and presentation purposes.

---

## Project Structure

```text
Health-Insurance-Data-System/
│
├── Data Cleaning/           # PySpark scripts for cleaning datasets
├── Data Transformations/    # Use case implementation scripts
├── Raw_Data/                # Input datasets
├── Redshift/                # Schema and COPY SQL scripts
├── Docs/                    # SRS, design documents, presentation
└── main.py                  # Entry or test script
```

---

## Data Pipeline

1. Raw datasets are uploaded to Amazon S3.
2. PySpark reads data from S3.
3. Data cleaning is performed:

   * Handling null values
   * Removing duplicates
   * Standardizing formats
4. Cleaned data is written back to S3.
5. Business use cases are implemented using PySpark.
6. Output results are stored in S3.
7. Data is loaded into Redshift using the COPY command.
8. Final results are queried from Redshift.

---

## Datasets Used

* Claims data
* Patient records
* Subscriber data
* Group and subgroup mapping
* Hospital data
* Disease data

---

## Use Cases Implemented

1. Disease with maximum number of claims
2. Subscribers with age less than 30 and subgroup
3. Group with maximum number of subgroups
4. Hospital serving the highest number of patients
5. Subgroup with the highest number of subscriptions
6. Total number of rejected claims
7. City with the highest number of claims
8. Most subscribed policy type
9. Average monthly premium
10. Most profitable insurance group
11. Patients below 18 diagnosed with cancer

---

## Data Cleaning

Data cleaning was performed using PySpark with the following steps:

* Identification and handling of null values
* Removal of duplicate records
* Trimming and standardization of string values
* Ensuring consistency across datasets

---

## Redshift Implementation

Two schemas are created:

* **project_cleaned**
  Stores cleaned datasets

* **project_output**
  Stores results of all analytical use cases

Data is loaded into Redshift using the COPY command from S3.

---

## Databricks Usage

Databricks is used to:

* Load processed data from S3 or local uploads
* Display tabular outputs
* Create visualizations such as bar charts
* Capture screenshots for presentation

---
## Visualization

Data visualizations were created using Databricks to analyze the processed results.

The screenshots of these visualizations are available in the **Docs** folder of this repository.

These include:

* Subscriber distribution by gender
* Subscriber distribution by city
* City with maximum insurance claims

These visualizations help in understanding customer distribution and claim patterns across different regions.

---
## How to Run

### Run PySpark scripts

```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026 script_name.py
```

### Load data into Redshift

```sql
COPY table_name
FROM 's3://bucket-path/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;
```

---

## Key Outcomes

* Built a scalable data processing pipeline
* Generated business insights from healthcare data
* Implemented multiple analytical use cases
* Stored final results in a data warehouse
* Enabled visualization for reporting

---

## Conclusion

This project demonstrates how big data technologies can be used to process and analyze healthcare insurance data. The system provides meaningful insights that can help organizations improve decision-making and optimize their operations.
