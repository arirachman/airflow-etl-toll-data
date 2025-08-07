# Airflow ETL Pipeline: Decongesting National Highways

## Project Overview

This project is a part of the [**IBM Data Engineering Specialization**](https://www.coursera.org/professional-certificates/ibm-data-engineer) on Coursera. It demonstrates a complete **ETL (Extract, Transform, Load) pipeline** using [**Apache Airflow**](https://airflow.apache.org) to process road traffic data from various toll operators. While this is a **dummy project** provided for educational purposes, it simulates a realistic scenario in which a data engineer is tasked with consolidating data from disparate IT systems.

The goal is to prepare the data for downstream analytics that can help understand and reduce congestion on national highways.

---

## Problem Context

Different toll plazas across the highway network are operated by different vendors, each using their own IT systems. As a result, traffic data is stored in **various formats**:

* CSV for vehicle logs, perhaps chosen for simplicity and compatibiliy with most tools.
* TSV for toll plaza logs, perhaps chosen to prevent delimiter conflicts or was exported from SQL tools (which was set by default to export in TSV)
* Fixed-width TXT for payment records. Chosem for legacy accounting or payment systems that depend on field alignment

Each data file captures only part of the full traffic event. By merging them, we reconstruct a comprehensive dataset that includes **vehicle type**, **location**, **timestamp**, and **payment method**.

---

## Project Structure

```bash
├── docker-compose.yaml          # Dockerized Airflow environment
├── ETL_toll_data_py.py          # Python scripts for ETL logic
├── etl_bash.py                  # Bash-based alternative ETL logic
├── README.md                    # Project documentation (this file)
│
├── dags/                        # Airflow DAGs directory
│   ├── toll_data_etl_py.py      # Main DAG using PythonOperators
│   ├── toll_data_etl_bash.py    # Alternate DAG using BashOperators
│
├── data/                        # Input data files
│   ├── toll_data.tgz            # Archived dataset
│   ├── vehicle-data.csv         # CSV format traffic data
│   ├── tollplaza-data.tsv       # TSV format toll plaza data
│   ├── payment-data.txt         # Fixed-width format payment data
│   └── staging/                 # Output directory for cleaned data
│
├── config/                      # Airflow configuration
│   └── airflow.cfg              # Custom config file (optional)
│
├── assets/                      # images used in this readme
├── logs/                        # Logs from DAG runs
└── plugins/                     # Custom Airflow plugins (if any)
```

---

## Dataset Description

After extracting `toll_data.tgz`, you will see the following input files:

### `vehicle-data.csv` (CSV)

* Format: Comma-separated values (no headers)
* Fields:

  1. Rowid
  2. Timestamp
  3. Anonymized Vehicle Number
  4. Vehicle Type
  5. Number of Axles
  6. Vehicle Code

### `tollplaza-data.tsv` (TSV)

* Format: Tab-separated values (no headers)
* Fields:

  1. Rowid
  2. Timestamp
  3. Anonymized Vehicle Number
  4. Vehicle Type
  5. Number of Axles
  6. Tollplaza ID
  7. Tollplaza Code

### `payment-data.txt` (Fixed-width TXT)

* Format: Fixed width fields (no headers)
* Fields:

  1. Rowid
  2. Timestamp
  3. Anonymized Vehicle Number
  4. Tollplaza ID
  5. Tollplaza Code
  6. Type of Payment Code
  7. Vehicle Code

### Final Output Format

The consolidated output includes the following fields:

```text
Rowid, Timestamp, Anonymized Vehicle Number, Vehicle Type, Number of Axles, Tollplaza ID, Tollplaza Code, Type of Payment Code, Vehicle Code
```

---

## DAG Workflow Overview

The DAG executes the following steps:

1. **Download Dataset** – Pull archived data from an online source
2. **Unzip Archive** – Extract `toll_data.tgz`
3. **Extract Tasks**:

   * Extract from CSV (vehicle data)
   * Extract from TSV (toll plaza data)
   * Extract from TXT (payment data)
4. **Consolidate Data** – Merge extracted files by `Rowid`
5. **Transform Data** – Perform basic cleaning or formatting
6. **Export Output** – Save to `data/staging/final_output.csv`

Pipeline diagram:
![DAG_workflow.png](https://github.com/arirachman/toll-data-etl/blob/4578c2fe733860389af6a69eaa21e0b3f62b6309/assets/DAG_workflow.png)
DAG runs successfully in 32 seconds.
![overview_dag_runs.png](https://github.com/arirachman/toll-data-etl/blob/4578c2fe733860389af6a69eaa21e0b3f62b6309/assets/overview_dag_runs.png)

The first 20 records of pipeline output as `transformed_data.csv` in the staging directory would look like this:
![output_csv_head.png](https://github.com/arirachman/toll-data-etl/blob/4578c2fe733860389af6a69eaa21e0b3f62b6309/assets/output_csv_head.png)

---

## Technologies Used

* **Apache Airflow**: DAG scheduling and orchestration
* **Python + Pandas**: Data parsing and transformation
* **Docker Compose**: Containerized development environment

---

## Running the Project

### Prerequisites:

* Docker
* Docker Compose

### Run Airflow Locally:

```bash
git clone https://github.com/arirachman/toll-data-etl.git
cd airflow-etl-toll-data
docker-compose up -d
```

### Access Airflow UI:

* Navigate to: `http://localhost:8080`
* Use your Airflow credentials you to sign in to Airflow UI.
* Trigger `ETL_toll_data_py` DAG manually or set a schedule

---
## Disclaimer
This project is for educational purposes only and part of the IBM Data Engineering Specialization on Coursera. **Do not plagiarize or submit this code as part of any academic assignment.**

---
## Acknowledgements
Thanks to **IBM** and **Coursera** for the course content and dataset that inspired this hands-on project.
