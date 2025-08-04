# Import required libraries and modules
import pendulum
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define DAG arguments
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
    dag_id = 'ETL_toll_data',
    default_args = default_args,
    description = "Batch ETL process for toll data"
)

#Task 1: Download the dataset
# This command downloads the tolldata.tgz file from the specified URL
download_data = BashOperator(
    task_id='download_data',
    bash_command="""
    echo "🔄 Downloading tolldata.tgz from the URL..."
    if curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /opt/airflow/data/tolldata.tgz; then
        echo "✅ tolldata.tgz downloaded successfully."
    else
        echo "❌ Failed to download tolldata.tgz" >&2
        exit 1
    fi
    """,
    dag=dag,
)

# Task 2: Unzip the tolldata.tgz
# This command extracts the tolldata archive file into the ./etl_toll directory
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command="""
    echo "🔄 Unzipping tolldata archive..."

    if tar -xzf /opt/airflow/data/tolldata.tgz -C /opt/airflow/data; then
        echo "✅ tolldata.tgz extracted successfully."
    else
        echo "❌ Failed to extract tolldata.tgz" >&2
        exit 1
    fi
    """,
    dag=dag,
)

# Task 3: Extract data from vehicle-data.csv to a new CSV
# This command uses comma as the delimiter and extracts the first 4 columns into csv_data.csv
extract_from_csv = BashOperator(
    task_id='extract_from_csv',
    bash_command="""
    echo "🔄 Extracting first 4 columns from vehicle-data.csv..."

    if cut -d "," -f1-4 /opt/airflow/data/vehicle-data.csv > /opt/airflow/data/csv_data.csv; then
        echo "✅ Data extracted from vehicle-data.csv into csv_data.csv"
    else
        echo "❌ Failed to extract data from vehicle-data.csv" >&2
        exit 1
    fi
    """,
    dag=dag,
)

# Task 4: Extract data from tollplaza-data.tsv to a new CSV
# Steps:
#   1. Extract columns 5 to 7 from the TSV
#   2. Remove carriage return characters (\r)
#   3. Replace tabs with commas to create a proper CSV
extract_from_tsv = BashOperator(
    task_id='extract_from_tsv',
    bash_command="""
    echo "🔄 Extracting columns 5-7 from tollplaza-data.tsv and transforming to CSV..."

    if cut -f5-7 /opt/airflow/data/tollplaza-data.tsv | \
       awk '{gsub(/\\r/, ""); print}' | \
       tr "\\t" "," > /opt/airflow/data/tsv_data.csv; then
        echo "✅ Data extracted from tollplaza-data.tsv into tsv_data.csv"
    else
        echo "❌ Failed to extract data from tollplaza-data.tsv" >&2
        exit 1
    fi
    """,
    dag=dag,
)

# Task 5: Extract data from fixed-width file to a new CSV
# Steps:
#   1. Extract characters from position 59 onward
#   2. Replace all space characters with commas to simulate CSV formatting
extract_from_fixed_width = BashOperator(
    task_id='extract_from_fixed_width',
    bash_command="""
    echo "🔄 Extracting data from the fixed-width file..."

    if cut -c 59- /opt/airflow/data/payment-data.txt | \
       tr " " "," > /opt/airflow/data/fixed_width_data.csv; then
        echo "✅ Data extracted from fixed-width file into fixed_width_data.csv"
    else
        echo "❌ Failed to extract data from fixed-width file" >&2
        exit 1
    fi
    """,
    dag=dag,
)

# Task 6: Consolidate all extracted data into one CSV file
# This command merges csv_data.csv, tsv_data.csv, and fixed_width_data.csv
# horizontally using commas and writes the result to extracted_data.csv
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    echo "🔄 Merging extracted data files into one CSV..."

    if paste -d "," /opt/airflow/data/csv_data.csv /opt/airflow/data/tsv_data.csv /opt/airflow/data/fixed_width_data.csv > /opt/airflow/data/extracted_data.csv; then
        echo "✅ Data successfully merged into extracted_data.csv"
    else
        echo "❌ Failed to merge data files." >&2
        exit 1
    fi
    """,
    dag=dag,
)

# Task 7: Transform and load all data
# This command:
#   1. Converts the 'Vehicle type' field (column 4) to uppercase
#   2. Reorders the columns into the correct sequence
#   3. Writes the result to transformed_data.csv
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    echo "🔄 Reordering fields and transforming vehicle type to uppercase..."

    if awk -F',' '{
        $4 = toupper($4);  # Vehicle type -> uppercase
        print $1","$2","$3","$4","$5","$6","$7","$8","$9
    }' OFS=',' /opt/airflow/data/extracted_data.csv > /opt/airflow/data/staging/transformed_data.csv; then
        echo "✅ Final CSV created with reordered fields and transformed vehicle type."
    else
        echo "❌ Failed to transform and reorder extracted data." >&2
        exit 1
    fi

    echo "🎉 The whole data processing is successfully completed!"
    """,
    dag=dag,
)

# Task Pipeline
download_data >> unzip_data >> extract_from_csv >> \
    extract_from_tsv >> extract_from_fixed_width >> consolidate_data >> transform_data