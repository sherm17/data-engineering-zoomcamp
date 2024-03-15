from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook

import os
import requests
import shutil
import gzip
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

green_taxi_project_path = "/tmp/green_taxi/"

def upload_to_s3(filename: str, key: str, bucket_name: str):
    hook = S3Hook()
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG(
    "green_taxi_dag",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Green taxi dag",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["green taxi"],   
) as dag:
    
    @task
    def download_green_taxi_data():
        """Download green taxi data for final quarter of 2020
        The months are 10, 11, and 12
        """
        
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{}-{}.csv.gz"
        data_download_path = os.path.join(green_taxi_project_path, "download")
        os.makedirs(data_download_path, exist_ok=True)

        year = 2020
        months = [10, 11, 12]


        for month in months:
            curr_url = url.format(year, month)
            filename = curr_url.split("/")[-1]
            gz_file_full_path = os.path.join(data_download_path, filename)
            with open(gz_file_full_path, "wb") as f:
                r = requests.get(curr_url)
                f.write(r.content)
                with gzip.open(gz_file_full_path, "rb") as f_in:
                    csv_file_name = filename.replace(".gz", "")
                    csv_file_full_path = os.path.join(data_download_path, csv_file_name)
                    with open(csv_file_full_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

    @task
    def transform_green_taxi_data():
        """Transform the green taxi data doing the following below
        - remove rows where passenger row is equal to 0
        - remove rows where trip distance is equal to 0
        - create new column lpep_pickup_daate by converting lpep_pickup_daatetime to a date
        - rename columns in camel case to snake case
        """

        green_taxi_download_path = os.path.join(green_taxi_project_path, "download")

        green_taxi_dataframe = pd.DataFrame()
        transformed_data_path = os.path.join(green_taxi_project_path, "transformed")
        os.makedirs(transformed_data_path, exist_ok=True)

        transformed_file_full_path = os.path.join(transformed_data_path, "transformed_green_taxi.csv")


        for file in os.listdir(green_taxi_download_path):
            if file.endswith(".csv"):
                file_full_path = os.path.join(green_taxi_download_path, file)
                df = pd.read_csv(file_full_path)
                green_taxi_dataframe = pd.concat([green_taxi_dataframe, df])

        green_taxi_dataframe.rename(
            columns={
            "RatecodeID": "rate_code_id",
            "PULocationID": "pu_location_id",
            "VendorID": "vendor_id",
            "DOLocationID": "do_location_id"
        }, inplace=True)

        passenger_and_trip_filter = (green_taxi_dataframe["passenger_count"] > 0 ) & (green_taxi_dataframe["trip_distance"] > 0)

        filtered_green_taxi = green_taxi_dataframe[passenger_and_trip_filter].copy()

        filtered_green_taxi["lpep_pickup_datetime"] = pd.to_datetime(filtered_green_taxi["lpep_pickup_datetime"])
        filtered_green_taxi["lpep_dropoff_datetime"] = pd.to_datetime(filtered_green_taxi["lpep_dropoff_datetime"])
        filtered_green_taxi["lpep_pickup_date"] = (filtered_green_taxi["lpep_pickup_datetime"]).dt.date

        filtered_green_taxi.to_csv(transformed_file_full_path, index=False)

    @task
    def load_transformed_green_taxi_to_pg_sql():
        """Load transformed green taxi data into postgres database"""

        transformed_file_full_path = os.path.join(green_taxi_project_path, "transformed", "transformed_green_taxi.csv")
        pg_hook = PostgresHook()
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        with open(transformed_file_full_path, "r") as file:
            cur.copy_expert(
                "COPY public.green_taxi FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file
            )
        conn.commit()

    @task
    def write_to_parquet():
        """Write transformed data to parquet format based on lpep_pickup_date column"""
        
        transformed_data_full_path = os.path.join(green_taxi_project_path, "transformed", "transformed_green_taxi.csv")
        partition_data_path = os.path.join(green_taxi_project_path, "partitioned_green_taxi")
        df = pd.read_csv(transformed_data_full_path)

        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table, partition_data_path, partition_cols=["lpep_pickup_date"], existing_data_behavior="delete_matching")

    @task
    def load_partitioned_data_to_s3():
        """Upload the green taxi partitioned data into s3"""

        partitioned_data_path = os.path.join(green_taxi_project_path, "partitioned_green_taxi")
        s3_bucket = "de-zoomcamp-sp"
        s3_folder_name = "green_taxi_partition"
        try:
            hook = S3Hook()

            for root, _, files in os.walk(partitioned_data_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    # Remove the local_folder_path from the beginning to create the relative S3 path
                    s3_file_path = os.path.relpath(local_file_path, partitioned_data_path)
                    s3_file_path = os.path.join(s3_folder_name, s3_file_path.replace(os.path.sep, '/'))
                    hook.load_file(
                        filename = local_file_path, 
                        key = s3_file_path, 
                        bucket_name = s3_bucket,
                        replace=True
                    )
        except FileNotFoundError:
            print("One or more files were not found")


    drop_green_taxi_table = PostgresOperator(
        task_id = "drop_green_taxi_table",
        sql = "DROP TABLE IF EXISTS public.green_taxi"
    )

    create_green_taxi_table = PostgresOperator(
        task_id = "create_green_taxi_table",
        sql = "sql/create_green_taxi_table.sql"
    )

    download_green_taxi_data() >> transform_green_taxi_data() >> drop_green_taxi_table >> create_green_taxi_table >> \
    load_transformed_green_taxi_to_pg_sql() >> write_to_parquet() >> load_partitioned_data_to_s3()