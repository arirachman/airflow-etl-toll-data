# Import the standard libraries
import requests
import csv
import os
import tarfile
from datetime import timedelta
from datetime import datetime

# Import the third-party libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

import pandas as pd

# DEFINE FUNCTIONS FOR THE ETL PROCESS
# Function to download data from a URL and save it to a local path
import requests

def download_data(url, source_path):
    """
    Downloads a file from the specified URL and saves it to the given local path.

    Args:
        url (str): The URL of the file to download.
        source_path (str): The local path where the downloaded file will be saved.
    """
    print("[START] Downloading data...")

    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses

        with open(source_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        print("[SUCCESS] File downloaded successfully to:", source_path)

    except requests.exceptions.HTTPError as http_err:
        print(f"[FAILURE] HTTP error occurred: {http_err}")
        raise

    except requests.exceptions.ConnectionError as conn_err:
        print(f"[FAILURE] Connection error occurred: {conn_err}")
        raise

    except requests.exceptions.Timeout as timeout_err:
        print(f"[FAILURE] Timeout error occurred: {timeout_err}")
        raise

    except requests.exceptions.RequestException as req_err:
        print(f"[FAILURE] Request error occurred: {req_err}")
        raise

    except IOError as io_err:
        print(f"[FAILURE] File I/O error occurred: {io_err}")
        raise

    except Exception as err:
        print(f"[FAILURE] Unexpected error: {err}")
        raise

# Function to untar a dataset from a given source path to a base path
def untar_dataset(source_path, base_path):
    """
    Extracts a tar.gz archive to the specified base path.

    Args:
        source_path (str): The path to the tar.gz file to extract.
        base_path (str): The directory where the contents will be extracted.
    """
    print("[START] Extracting archive...")

    try:
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Archive not found: {source_path}")

        with tarfile.open(source_path, 'r:gz') as tar:
            tar.extractall(path=base_path)

        print(f"[SUCCESS] Archive extracted successfully to: {base_path}")

    except FileNotFoundError as fnf_err:
        print(f"[FAILURE] File not found: {fnf_err}")
        raise

    except tarfile.ReadError as read_err:
        print(f"[FAILURE] Invalid tar file or corrupted archive: {read_err}")
        raise

    except PermissionError as perm_err:
        print(f"[FAILURE] Permission denied: {perm_err}")
        raise

    except Exception as err:
        print(f"[FAILURE] Unexpected error while extracting: {err}")
        raise

# Function to extract data from various file formats
def extract_from_csv(infile, outfile=None):
    """
    Extracts specified data from a CSV file and saves it to an output CSV file.

    Args:
        infile (str): Path to the input CSV file.
        outfile (str, optional): Path to save the extracted data. Defaults to None.
    """
    print(f"[START] Extracting columns 1–4 from CSV: {infile}")

    try:
        with open(infile, 'r', encoding='utf-8') as readfile:
            lines = readfile.readlines()

        if not lines:
            raise ValueError("CSV file is empty.")

        selected_lines = []
        for line in lines:
            selected_columns = ",".join(line.strip().split(",")[:4])
            selected_lines.append(selected_columns)

        if outfile:
            with open(outfile, 'w', encoding='utf-8') as writefile:
                writefile.write("\n".join(selected_lines) + "\n")
            print(f"[SUCCESS] Data written to output CSV: {outfile}")
        else:
            print("[SUCCESS] Data extracted successfully (no output file provided)")

    except FileNotFoundError as fnf_err:
        print(f"[FAILURE] File not found: {fnf_err}")
        raise

    except UnicodeDecodeError as decode_err:
        print(f"[FAILURE] Encoding error: {decode_err}")
        raise

    except ValueError as val_err:
        print(f"[FAILURE] Data error: {val_err}")
        raise

    except Exception as err:
        print(f"[FAILURE] Unexpected error: {err}")
        raise

# Function to extract data from a TSV file
def extract_from_tsv(infile, outfile=None):
    """
    Extracts specified data from a TSV file and saves it to an output CSV file.
    
    Args:
        infile (str): Path to the input TSV file.
        outfile (str, optional): Path to save the extracted data. Defaults to None.
    """
    print(f"[START] Extracting columns 5–7 from TSV: {infile}")

    try:
        with open(infile, 'r', encoding='utf-8') as readfile:
            lines = readfile.readlines()

        if not lines:
            raise ValueError("TSV file is empty.")

        selected_lines = []
        for line in lines:
            selected_columns = ",".join(line.strip().split("\t")[4:7])
            selected_lines.append(selected_columns)

        if outfile:
            with open(outfile, 'w', encoding='utf-8') as writefile:
                writefile.write("\n".join(selected_lines) + "\n")
            print(f"[SUCCESS] TSV data written to: {outfile}")
        else:
            print("[SUCCESS] TSV data extracted successfully (no output file provided)")

    except FileNotFoundError as fnf_err:
        print(f"[FAILURE] File not found: {fnf_err}")
        raise
    except UnicodeDecodeError as decode_err:
        print(f"[FAILURE] Encoding error: {decode_err}")
        raise
    except ValueError as val_err:
        print(f"[FAILURE] Data error: {val_err}")
        raise
    except Exception as err:
        print(f"[FAILURE] Unexpected error: {err}")
        raise

def extract_from_fixed_width(infile, outfile=None):
    """
    Extracts specified data from a fixed-width file and saves it to an output CSV file.
    
    Args:
        infile (str): Path to the input fixed-width file.
        outfile (str, optional): Path to save the extracted data. Defaults to None.
    """
    print(f"[START] Extracting columns 10–11 from fixed-width file: {infile}")

    try:
        with open(infile, 'r', encoding='utf-8') as readfile:
            lines = readfile.readlines()

        if not lines:
            raise ValueError("Fixed-width file is empty.")

        selected_lines = []
        for line in lines:
            cleaned_line = " ".join(line.split())
            selected_columns = cleaned_line.split(" ")[9:11]
            selected_lines.append(",".join(selected_columns))

        if outfile:
            with open(outfile, 'w', encoding='utf-8') as writefile:
                writefile.write("\n".join(selected_lines) + "\n")
            print(f"[SUCCESS] Fixed-width data written to: {outfile}")
        else:
            print("[SUCCESS] Fixed-width data extracted successfully (no output file provided)")

    except FileNotFoundError as fnf_err:
        print(f"[FAILURE] File not found: {fnf_err}")
        raise
    except UnicodeDecodeError as decode_err:
        print(f"[FAILURE] Encoding error: {decode_err}")
        raise
    except ValueError as val_err:
        print(f"[FAILURE] Data error: {val_err}")
        raise
    except Exception as err:
        print(f"[FAILURE] Unexpected error: {err}")
        raise

def consolidate_data(infile, outfile):
    """
    Consolidates data extracted from multiple files into a single CSV file.
    
    Args:
        infile (list): List of input file paths.
        outfile (str): Path to the output CSV file.
    """
    print(f"[START] Consolidating data from {len(infile)} files into: {outfile}")

    try:
        combined_data = pd.concat([pd.read_csv(file) for file in infile], axis=1)
        combined_data.to_csv(outfile, index=False)
        print(f"[SUCCESS] Consolidated CSV written to: {outfile}")

    except FileNotFoundError as fnf_err:
        print(f"[FAILURE] File not found: {fnf_err}")
        raise
    except pd.errors.EmptyDataError as empty_err:
        print(f"[FAILURE] Empty data error: {empty_err}")
        raise
    except Exception as err:
        print(f"[FAILURE] Unexpected error during consolidation: {err}")
        raise

def transform_data(infile, outfile):
    """
    Transforms the 4th column in the consolidated data to uppercase
    and saves the result to the staging directory.
    
    Args:
        infile (str): Path to the input consolidated CSV file.
        outfile (str): Path to the output transformed CSV file.
    """
    print(f"[START] Transforming column 4 to uppercase in file: {infile}")

    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            reader = csv.reader(readfile)
            writer = csv.writer(writefile)

            for row in reader:
                if len(row) > 3:
                    row[3] = row[3].upper()
                writer.writerow(row)

        print(f"[SUCCESS] Transformed data written to: {outfile}")

    except FileNotFoundError as fnf_err:
        print(f"[FAILURE] File not found: {fnf_err}")
        raise
    except pd.errors.EmptyDataError as empty_err:
        print(f"[FAILURE] Empty data error: {empty_err}")
        raise
    except Exception as err:
        print(f"[FAILURE] Unexpected error during transformation: {err}")
        raise


#  Define DAG arguments
default_args = {
    'owner': 'arirachman',
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
    'email': ['arirachman@mail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
dag = DAG(
    dag_id = 'roadtraffic_etl_dag',
    default_args = default_args,
    description = "Batch ETL process for road traffic data using Python Operators",
)


# Declare some known variables
source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
BASE_PATH = '/opt/airflow/data'
DESTINATION_PATH = os.path.join(BASE_PATH, 'staging')
source_file = os.path.join(BASE_PATH, 'toll_data.tgz')
vehicle_data = os.path.join(BASE_PATH, 'vehicle-data.csv')
tollplaza_data = os.path.join(BASE_PATH, 'tollplaza-data.tsv')
payment_data = os.path.join(BASE_PATH, 'payment-data.txt')
csv_data = os.path.join(BASE_PATH, 'csv_data.csv')
tsv_data = os.path.join(BASE_PATH, 'tsv_data.csv')
fixed_width_data = os.path.join(BASE_PATH, 'fixed_width_data.csv')
extracted_data = os.path.join(BASE_PATH, 'extracted_data.csv')
transformed_data = os.path.join(DESTINATION_PATH, 'transformed_data.csv')


# DEFINE TASKS
# Task 1. Download the data
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    op_args=[source_url, source_file],
    dag=dag,
)

# Task 2. Untar the dataset
untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    op_args=[source_file, BASE_PATH],
    dag=dag,
)

# Task 3. Extract data from a csv file
extract_from_csv_task = PythonOperator(
    task_id='extract_from_csv',
    python_callable=extract_from_csv,
    op_args=[vehicle_data, csv_data],
    dag=dag,
)

# Task 4. Extract data from TSV
extract_from_tsv_task = PythonOperator(
    task_id='extract_from_tsv',
    python_callable=extract_from_tsv,
    op_args=[tollplaza_data, tsv_data],
    dag=dag,
)

# Task 5. Extract data from fixed width file
extract_from_fixed_width_task = PythonOperator(
    task_id='extract_from_fixed_width',
    python_callable=extract_from_fixed_width,
    op_args=[payment_data, fixed_width_data],
    dag=dag,
)

# Task 6. Consolidate data from previous tasks
consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    op_args=[[csv_data, tsv_data, fixed_width_data], extracted_data],
    dag=dag,
)   

# Task 7. Transform and load the data
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[extracted_data, transformed_data],
    dag=dag,
)   

# Set task dependencies
download_task >> untar_task >> [extract_from_csv_task, extract_from_tsv_task, extract_from_fixed_width_task] >> consolidate_task >> transform_task