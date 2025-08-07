# Data ETL Process to Decongest National Highways Project
## Project Overview
---
1. This project is the part of an assignment inside the IBM Data Engineering Specialization on Coursera, Therefore this is a dummy project, not a real one.
2. This project is a part of a larger project aims to decongest national highways by analyzing the road traffic data.
3. The road traffic data obtained from several toll plazas in the form of 3 data, each with different formats that is CSV, TSV, and TXT. Apparently, each comes from different toll operator or vendor and each has different purpose.
4. The data comprises of a vehicle data in CSV file that records logs of vehicle type, axles, and when they were passing through a toll plaza, a toll-plaza data in TSV file that records which toll plaza the vehicle passed, and a payment data in TXT file that records about how the vehicle paid for the bill.
5. This differences in file formats possibly caused by different operation run by different toll operator or vendor. One might running the vehicle monitoring system with the output data to be simple, easy to read and parse with any tool or programming language, therefore they chose to output it as CSV file. One might running the toll plaza operation system with the output data as TSV file to avoid delimiter issue, or they just export it straight from their SQL tool (which defaults to TSV). And the last one might running the Payment or accounting system that cares about how the vehicle paid the bill and the output data need to be in fixed with (.txt) for legacy accounting system (they usually have strict layout matter).
6. Disclaimer: I don't encourage current learner in IBM Data Engineering to copy my work, or any form of misuse that will violate Coursera Code of Conduct. I'm here to advocate learning in public to fellow developers to publish anything you're genuinely interested in. **I do not condone or take responsibility for any form of academic dishonesty, including plagiarism or unauthorized use of this work in academic submissions.** Anyone using this code is expected to adhere to Coursera Code of Conduct.
## Project Structure
---
This repository contains the following files and folders:

- `dags`: all DAGs in the Airflow environment of this project. Files in this folder will be parsed by the Airflow scheduler when looking for DAGs to add to the environment. We can add our own dagfiles in this folder.
    - `etl-roadtraffic_dag.py`: a DAG to run the whole ETL process using python operators.
    - `etl-roadtraffic_alternate_dag.py`: an alternative DAG to run the whole ETL process using bash operators.
- `data`: folder for dataset used in this project and processed data through the ETL pipeline.
	- `staging`: folder to place the output of the whole pipeline, for further use of another project.
	- `tolldata.tgz`: dataset used in this project provided by the IBM course.
- `logs`: folder for log process of running dags.
- `config`: folder to place Airflow configuration files.
- `.env`: environment variables used in the project.
- `.gitignore`: list of files to ignore for git.
- `docker-compose.yaml`: Docker 
- `README.md`: this Readme

## Dataset overview
---
When we extract `tolldata.tgz`, we should see the following 3 files:
1. `vehicle-data.csv` is a comma-separated values file, has 6 fields with no header.
	1. Rowid  - This uniquely identifies each row. This is consistent across all the three files.
	2. Timestamp - What time did the vehicle pass through the toll gate.
	3. Anonymized Vehicle number - Anonymized registration number of the vehicle 
	4. Vehicle type - Type of the vehicle
	5. Number of axles - Number of axles of the vehicle
	6. Vehicle code - Category of the vehicle as per the toll plaza.
2. `tollplaza-data.tsv` is a tab-separated values file. It has 7 fields with no header.
	1. Rowid  - This uniquely identifies each row. This is consistent across all the three files.
	2. Timestamp - What time did the vehicle pass through the toll gate.
	3. Anonymized Vehicle number - Anonymized registration number of the vehicle 
	4. Vehicle type - Type of the vehicle
	5. Number of axles - Number of axles of the vehicle
	6. Tollplaza id - Id of the toll plaza
	7. Tollplaza code - Tollplaza accounting code.
3. `payment-data.txt` is a fixed width file. Each field occupies a fixed number of characters. It has 7 fields with no header
	1. Rowid  - This uniquely identifies each row. This is consistent across all the three files.
	2. Timestamp - What time did the vehicle pass through the toll gate.
	3. Anonymized Vehicle number - Anonymized registration number of the vehicle 
	4. Tollplaza id - Id of the toll plaza
	5. Tollplaza code - Tollplaza accounting code.
	6. Type of Payment code - Code to indicate the type of payment. Example : Prepaid, Cash.
	7. Vehicle Code -  Category of the vehicle as per the toll plaza.

The final csv file should use the fields in the order given below:
- `Rowid`
- `Timestamp`
- `Anonymized Vehicle number`
- `Vehicle type`
- `Number of axles`
- `Tollplaza id`
- `Tollplaza code`
- `Type of Payment code`, and
- `Vehicle Code`
## DAG runs
---
![[dag_runs.png]]

## Acknowledgement
---
I acknowledge IBM Data Engineering Specialization for taught me the hands-on labs, and provide me the dataset to experiment on it.

# Airflow ETL Pipeline: Decongesting National Highways

## Project Overview

This project is a part of the **IBM Data Engineering Specialization** on Coursera. It demonstrates a complete **ETL (Extract, Transform, Load) pipeline** using **Apache Airflow** to process road traffic data from various toll operators. While this is a **dummy project** provided for educational purposes, it simulates a realistic scenario in which a data engineer is tasked with consolidating data from disparate IT systems.

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
![[DAG_workflow.png]]
DAG runs successfully in 32 seconds.
![[overview_dag_runs.png]]

The first 20 records of pipeline output as `transformed_data.csv` in the staging directory would look like this:
![[output_csv_head.png]]

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
