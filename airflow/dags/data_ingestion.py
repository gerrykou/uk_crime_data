import requests
import functools
from typing import List, Dict, Tuple
import csv, json
import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
FORCE_ID = 'metropolitan'
DATE = "{{ execution_date.strftime('%Y-%m') }}"

print(f'date: {DATE}')

FILENAME = f'{FORCE_ID}_{DATE}'
print(f'filename: {FILENAME}')

# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'stop_and_search')

# DIR = f'{path_to_local_home}/data/'
DIR = f'{path_to_local_home}/'
PATH = f'{DIR}{FILENAME}'
INPUT_FILETYPE = "parquet"

SCHEMA = types.StructType([
    types.StructField('age_range', types.StringType(), True),
    types.StructField('outcome', types.StringType(), True),
    types.StructField('involved_person', types.BooleanType(), True),
    types.StructField('self_defined_ethnicity', types.StringType(), True),
    types.StructField('gender', types.StringType(), True),
    types.StructField('legislation', types.StringType(), True),
    types.StructField('outcome_linked_to_object_of_search', types.StringType(), True),
    types.StructField('datetime', types.TimestampType(), True),
    types.StructField('removal_of_more_than_outer_clothing', types.StringType(), True),
    types.StructField('outcome_object', types.StringType(), True),
    types.StructField('location', types.StringType(), True),
    types.StructField('operation', types.StringType(), True),
    types.StructField('officer_defined_ethnicity', types.StringType(), True),
    types.StructField('type', types.StringType(), True),
    types.StructField('operation_name', types.StringType(), True),
    types.StructField('object_of_search', types.StringType(), True),
    types.StructField('latitude', types.DoubleType(), True),
    types.StructField('longitude', types.DoubleType(), True),
    types.StructField('street_id', types.IntegerType(), True),
    types.StructField('street_name', types.StringType(), True),
])

def stop_and_searches_by_force(force_id: str, date: str) -> Dict:
    ''' https://data.police.uk/docs/method/stops-force/ '''
    print(date)
    # https://data.police.uk/api/stops-force?force=metropolitan&date=2021-01
    URL = f'https://data.police.uk/api/stops-force?force={force_id}&date={date}'
    print(f'Requesting data from {URL}')
    r = requests.get(URL, timeout=None)
    print('status code: ', r.status_code)
    stop_and_searches = r.json()
    return stop_and_searches

def json_to_file(json_data, filename: str) -> None:
    with open(f'{filename}.json', 'w') as outfile:
        json.dump(json_data, outfile)

def update_json(date: str):
    date, path, filename = _return_date_path_filename(date)
    filename = f'{path}_raw'
    new_filename = path

    file = f'{filename}.json'
    with open(file) as jf:
        data = json.load(jf)
    new_data = []
    for row in data:

        if row['location'] is not None:
            row['latitude'] = row['location']['latitude']
            row['longitude'] = row['location']['longitude']
            row['street_id'] = row['location']['street']['id']
            row['street_name'] = row['location']['street']['name']
        else:
            row['latitude'] = None
            row['longitude'] = None
            row['street_id'] = None
            row['street_name'] = None
        new_data.append(row)
    json_to_file(new_data, new_filename)

def json_to_csv(date: str) -> None:
    date, path, filename = _return_date_path_filename(date)

    with open(f'{path}.json') as jf:
        data = json.load(jf)
    stop_and_searches_data = data
    print(stop_and_searches_data[0].keys())

    with open(f'{path}.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        count = 0
        for row in stop_and_searches_data:
            if count == 0:
                header = row.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(row.values())

def create_json_file_from_api(date: str) -> None:
    date, path, filename = _return_date_path_filename(date)
    response_data = stop_and_searches_by_force(FORCE_ID, date)
    json_to_file(response_data, f'{path}_raw')

def _return_date_path_filename(date: str) -> Tuple:
    date_str = date[:7]
    filename = f'{FORCE_ID}_{date_str}'
    path = f'{DIR}{filename}'
    return date_str, path, filename # TO DO return kwargs

def delete_json_file(date: str) -> None:
    date, path, filename = _return_date_path_filename(date)
    os.remove(f'{path}_raw.json')

def format_to_parquet(date: str):
    date, path, filename = _return_date_path_filename(date)
    src_file = f'{path}.csv'
    table = pv.read_csv(src_file) 
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# def format_to_parquet_with_spark(date: str):
#     date, path, filename = _return_date_path_filename(date)
#     src_file = f'{path}.csv'

#     spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName('test') \
#     .getOrCreate()

#     df = spark.read \
#     .option("header", "true") \
#     .schema(SCHEMA) \
#     .csv(src_file)

#     df = df.repartition(1)
#     df = df.write.parquet(f'{path}')

def upload_to_gcs(bucket: str, date: str) -> None:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param date: date from ds
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    date, path, filename = _return_date_path_filename(date)
    local_file = f'{path}.parquet'
    object_name = f'{filename}.parquet'
    
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1),
    "end_date": datetime(2020, 12, 1),
    "depends_on_past": False,
    "retries": 4,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['uk-crime-data'],
) as dag:

    create_json_file_from_api_task = PythonOperator(
        task_id="create_json_file_from_api_task",
        python_callable=create_json_file_from_api,
        op_kwargs={
            'date': '{{ ds }}'
            },
    )

    update_json_task = PythonOperator(
        task_id="update_json_task",
        python_callable=update_json,
        op_kwargs={
            'date': '{{ ds }}'
            },
    )

    json_to_csv_task = PythonOperator(
    task_id="json_to_csv_task",
    python_callable=json_to_csv,
    op_kwargs={
        'date': '{{ ds }}'
        },
    )

    delete_json_file_task = PythonOperator(
    task_id="delete_json_file_task",
    python_callable=delete_json_file,
    op_kwargs={
        'date': '{{ ds }}'
        },
    )

    format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet_task",
    python_callable=format_to_parquet,
    op_kwargs={
        'date': '{{ ds }}'
        },
    )

    # format_to_parquet_with_spark_task = PythonOperator(
    # task_id="format_to_parquet_with_spark_task",
    # python_callable=format_to_parquet_with_spark,
    # op_kwargs={
    #     'date': '{{ ds }}'
    #     },
    # )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket': BUCKET,
            'date': '{{ ds }}'
        },
    )
    
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id= "bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{BIGQUERY_DATASET}_external_table",
            },
            "externalDataConfiguration": {
                # "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/*"],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET} \
        PARTITION BY DATE('datetime') \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}_external_table;")


    # Create a partitioned table from external table
    bigquery_create_partitioned_table_task = BigQueryInsertJobOperator(
        task_id=f"bigquery_create_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    # move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job
    

    create_json_file_from_api_task >> update_json_task >> \
        json_to_csv_task >> delete_json_file_task >> format_to_parquet_task >> local_to_gcs_task >> \
            bigquery_external_table_task >> bigquery_create_partitioned_table_task
    # create_json_file_from_api_task >> update_json_task >> \
    #     json_to_csv_task >> delete_json_file_task >> format_to_parquet_with_spark_task >> local_to_gcs_task

# if __name__ == '__main__':
