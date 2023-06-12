from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
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


with DAG(
    "test_dataproc_2",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 
    

    instantiate_workflow = DataprocInstantiateWorkflowTemplateOperator(
    task_id='instantiate_workflow',
    project_id=PROJECT_NAME,
    region='us-central1',
    template_id='template_process_file',
    parameters={
        'CLUSTER':'tenpo-ipm-prod',
        'NUMWORKERS':'16',
        'JOBFILE':'gs://mark-vii-conciliacion/artifacts/dataproc/pyspark_data_process.py',
        'FILES_OPERATORS':'gs://mark-vii-conciliacion/artifacts/dataproc/operators/*',
        'INPUT':'gs://mark-vii-conciliacion/data/ipm/MCI.AR.T112.M.E0073610.D*',
        'TYPE_FILE':'ipm',
        'OUTPUT':'tenpo-datalake-sandbox.tenpo_conciliacion_staging_dev.ipm',
        'MODE_DEPLOY':'prod'
    },
    )

    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end')


start_task >> instantiate_workflow >> end_task




