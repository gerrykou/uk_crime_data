import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'stop_and_search')
 
INPUT_FILETYPE = "parquet"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1),
    "end_date": datetime(2020, 12, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['stop-and-search'],
) as dag:

    # for colour, ds_col in COLOUR_RANGE.items():
    # move_files_gcs_task = GCSToGCSOperator(
    #     task_id=f'move__files_task',
    #     source_bucket=BUCKET,
    #     source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
    #     destination_bucket=BUCKET,
    #     destination_object=f'{colour}/{colour}_{DATASET}',
    #     move_object=True
    # )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id= "bq_external_table_task",
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
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    # move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job
    bigquery_external_table_task >> bq_create_partitioned_table_job