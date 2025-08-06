# Data ETL Process to Decongest National Highways Project
## Project Overview
1. This project is the part of an assignment inside the IBM Data Engineering Specialization on Coursera, Therefore this is a dummy project, not a real one.
2. This project is a part of a larger project aims to decongest national highways by analyzing the road traffic data.
3. The road traffic data obtained from several toll plazas in the form of 3 data, each with different formats that is CSV, TSV, and TXT. Apparently, each comes from different toll operator or vendor and each has different purpose.
4. The data comprises of a vehicle data in CSV file that records logs of vehicle type, axles, and when they were passing through a toll plaza, a toll-plaza data in TSV file that records which toll plaza the vehicle passed, and a payment data in TXT file that records about how the vehicle paid for the bill.
5. This differences in file formats possibly caused by different operation run by different toll operator or vendor. One might running the vehicle monitoring system with the output data to be simple, easy to read and parse with any tool or programming language, therefore they chose to output it as CSV file. One might running the toll plaza operation system with the output data as TSV file to avoid delimiter issue, or they just export it straight from their SQL tool (which defaults to TSV). And the last one might running the Payment or accounting system that cares about how the vehicle paid the bill and the output data need to be in fixed with (.txt) for legacy accounting system (they usually have strict layout matter).
6. Disclaimer: I don't encourage current learner in IBM Data Engineering to copy my work, or any form of misuse that will violate Coursera Code of Conduct. I'm here to advocate learning in public to fellow developers to publish anything you're genuinely interested in. **I do not condone or take responsibility for any form of academic dishonesty, including plagiarism or unauthorized use of this work in academic submissions.** Anyone using this code is expected to adhere to Coursera Code of Conduct.
## Project Structure
```
opt/
├── README.md
├── dags/
│   ├── etl_roadtraffic_alternate_dag.py
│   ├── toll_data_etl_bash.py
│   ├── toll_data_etl_py.py
│   └── __pycache__/
├── data/
│   ├── fileformats.txt
│   ├── payment-data.txt
│   ├── toll_data.tgz
│   ├── tollplaza-data.tsv
│   ├── vehicle-data.csv
│   └── staging/
├── logs/
│   ├── dag_id=create_working_directory/
│   ├── dag_id=ETL_toll_data/
│   ├── dag_id=ETL_toll_data_py_operators/
│   └── dag_processor/
├── plugins/
```
## Acknowledgement
I acknowledge IBM Data Engineering Specialization for taught me the hands-on labs, and provide me the dataset to experiment on it.