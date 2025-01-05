Air Quality Pipeline

This project demonstrates the ETL (Extract, Transform, Load) process for air quality data from the Ville de Montreal dataset. The pipeline pulls data from an API, transforms it, and loads it into a CSV file. The pipeline is orchestrated using Apache Airflow and ensures that only data from the last 15 days is maintained.

Setup
Install Dependencies
Clone this repository.
Install the required Python packages using:
bash
Copy code
pip install -r requirements.txt

Configuration
API: The script fetches air quality data from the Ville de Montreal API.
CSV File: The processed data is stored in a CSV file named vdm_air_quality_index.csv. This file will contain only data from the last 15 days. Any data older than that will be removed during the loading process.

How It Works
Data Extraction: The pipeline fetches real-time air quality data from the Ville de Montreal dataset using an API. The data is updated every hour, and this pipeline ensures that the most recent data is processed.
Data Transformation: The extracted data is cleaned and transformed into a consistent format, including:
Flattening the nested JSON response.
Converting date and time columns into appropriate datetime formats.
Creating a composite key for uniqueness.
Data Loading: The transformed data is appended to an existing CSV file. Only the most recent 15 days' worth of data is kept. If data is older than 15 days, it is removed during the loading process to maintain the file size and relevance.

DAG Scheduling
The pipeline is scheduled to run every hour using Apache Airflow (@hourly), ensuring that the data is updated in near real-time. If data from a previous run is missed, the pipeline will automatically catch up on the missed data.

Start Date
The pipeline starts from 2025-01-04 and processes data from that date onward.

Example of Data Filtering
Only data from the last 15 days will be maintained in the vdm_air_quality_index.csv. Older data will be filtered out each time new data is added.
For instance, if today is 2025-01-05, the pipeline will retain data from 2025-01-05 to 2024-12-21, and any data older than that will be removed.
