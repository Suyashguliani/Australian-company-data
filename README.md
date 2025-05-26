# Australian Company Data Pipeline

## Overview

This project builds a data pipeline to extract, process, and store Australian company information from two primary data sources:

* **Common Crawl** : Extracting Australian company websites with fields such as website URL, company name.
* **Australian Business Register (ABR)**: Processing bulk XML extracts to retrieve official company data including ABN, entity name, type, status, address, postcode, state, and start date.

The data from both sources is processed using **Apache Spark (PySpark)** to handle large-scale data efficiently, producing consolidated CSV files (`abr_combined_extracted.csv` and `australian_companies.csv`). The cleaned and merged data is then stored locally in a PostgreSQL database for querying and analysis.

---

## Database Schema (PostgreSQL DDL)

```sql
CREATE TABLE companies (
    abn VARCHAR(20) PRIMARY KEY,            
    entity_name TEXT NOT NULL,               
    entity_type TEXT,                        
    entity_status TEXT,                      
    entity_start_date DATE,                  
    state VARCHAR(10),                       
    postcode VARCHAR(10)
);

CREATE TABLE websites (
    website_url TEXT NOT NULL,       
    extracted_company_name TEXT
);
```

---

## Pipeline Architecture

### Diagram

```
[ Common Crawl ]                         [ Australian Business Register (XML) ]
           │                                                 │
           └──────────────┐                ┌────────────────┘
                          ▼                ▼
                 [ PySpark Processing and Cleaning ]
                          │
                          ▼
         [ CSV Outputs: abr_combined_extracted.csv & australian_companies_website.csv ]
                          │
                          ▼
               [ PostgreSQL Database (local) ]
                          │
                          ▼
               [ Data Querying and Analytics ]
```

### Description

1. **Data Extraction:**

   * Common Crawl dataset is filtered for Australian websites, extracting relevant fields.
   * ABR bulk XML files are parsed to extract official company information.

2. **Data Processing:**

   * PySpark processes large datasets efficiently, cleans data, merges sources, and produces consistent CSV output files.

3. **Storage:**

   * Final CSV data is ingested into PostgreSQL tables `companies` and `websites` for persistent storage and querying.

4. **Analysis:**

   * Stored data can be used for business intelligence, analytics, or integration with other applications.

---

Procedure

extract.py- extracts data from ABR and saves it to abr_combined_extracted.csv

listing.py- parses the .wet file from common crawl and saves all .wrac url to url.txt 

python-spark.py- installs the .wrac files locally and extract data from them and saves it to australian_companies.csv

loading.py/loading2.py- loads the data to PostgreSQL tables `companies` and `websites` for persistent storage and querying



## Technology Stack Justification

* **Common Crawl:** Provides a massive open web crawl dataset essential for capturing real-world Australian company websites at scale.

* **Australian Business Register (ABR):** Authoritative source of official company registration data, ensuring accuracy and completeness.

* **Apache Spark (PySpark):** Chosen for its ability to process large datasets distributedly and efficiently. Its Python API facilitates flexible, fast data transformations and joins on multi-gigabyte data that would be slow or impossible in-memory otherwise.

* **CSV Format:** Easy interchange format that bridges PySpark processing and PostgreSQL ingestion, allowing for simple and effective ETL pipeline steps.

* **PostgreSQL:** A powerful, open-source relational database system selected for its reliability, ACID compliance, and support for complex queries — ideal for structured company data with strict uniqueness (ABN) and indexing requirements.

* **Python:** Used for writing ingestion scripts due to its readability and rich ecosystem, including libraries like `psycopg2` for seamless PostgreSQL integration.

This technology stack is designed to handle large-scale, real-world data with robustness, scalability, and maintainability.

---

## Setup and Running Instructions

### Prerequisites

* Python 3.7+
* Apache Spark installed with PySpark
* PostgreSQL installed and running locally
* `psycopg2` Python package installed (`pip install psycopg2-binary`)
* Access to:

  * Common Crawl Index data
  * ABR bulk extract XML files

### Steps

1. **Data Extraction and Processing:**

   Use PySpark scripts (not included here) to:

   * Extract Australian websites from Common Crawl data.
   * Parse ABR XML files.
   * Clean and join datasets.
   * Export consolidated CSV files: `abr_combined_extracted.csv` and `australian_companies.csv`.

2. **Database Setup:**

   Connect to your PostgreSQL instance and run the provided SQL schema commands to create `companies` and `websites` tables.

3. **Data Ingestion:**

   Use Python scripts with `psycopg2` to read CSV files and insert data into PostgreSQL tables. 


4. **Querying and Usage:**

   Access and analyze your data using SQL queries or integrate with applications as needed.

ChatGPT and Deekseek were used for development and debugging.

