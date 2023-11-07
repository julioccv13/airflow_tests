from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from google.cloud import bigquery
import datetime
import logging

default_args = {
    "owner": "tenpo",
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( minutes= 10 ),
    "email_on_failure": False,
    "email_on_retry": False,
}

# DAG Variables used
PROJECT_NAME = Variable.get('project1')
SOURCE_BUCKET = 'mark-vii-conciliacion'
TARGET_BUCKET = 'mark-vii-conciliacion'
PREFIX = 'sql'
QUERY = 'ipm_staging_to_gold.sql'


with DAG(
    "tenpo_conciliaciones_sandbox_reprocess_test",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 

    ipm_gold = GoogleCloudStorageToBigQueryOperator(
    task_id='ipm_gold',
    bucket=SOURCE_BUCKET,
    source_objects=[f'{PREFIX}/{QUERY}'],
    destination_project_dataset_table='tenpo-datalake-sandbox.tenpo_conciliacion_gold_dev.ipm',
    schema_fields=None,  # Provide schema fields if needed
    write_disposition='WRITE_TRUNCATE',  # Choose appropriate write disposition
    bigquery_conn_id='conciliacion_gcp_conn_id',  # Connection ID to BigQuery
    google_cloud_storage_conn_id='conciliacion_gcp_conn_id',  # Connection ID to GCS
    dag=dag
)

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end')

start_task >> ipm_gold >> end_task



