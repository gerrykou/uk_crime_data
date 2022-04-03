import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'stop_and_search')
INPUT_FILETYPE = "parquet"


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['stop-and-search'],
) as dag:
        
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id= "bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{BIGQUERY_DATASET}_external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/*"],
            },
        },
    )

    CREATE_BQ_TABLE_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{BIGQUERY_DATASET}_partitioned_table \
        PARTITION BY DATE(datetime) \
        AS \
        SELECT * FROM {PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_DATASET}_external_table;")

    # Create a partitioned table from external table
    bigquery_create_partitioned_table_task = BigQueryInsertJobOperator(
        task_id=f"bigquery_create_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TABLE_QUERY,
                "useLegacySql": False,
            }
        }
    )

    bigquery_external_table_task >> bigquery_create_partitioned_table_task