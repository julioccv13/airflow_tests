from airflow.models import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from plugins.slack import get_task_success_slack_alert_callback
from plugins.conciliacion_tasks import *
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import datetime
import logging

# DAG general parameters
env = Variable.get('env')
environment = Variable.get("environment")
PROJECT_NAME = Variable.get(f'datalake_{env}')
SOURCE_BUCKET = Variable.get(f'conciliacion_ops_bucket_{env}')
TARGET_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
DATA_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
DEPLOYMENT = Variable.get(f"conciliacion_deployment_{env}")
PYSPARK_FILE = Variable.get(f'conciliacion_pyspark_{env}')
CLUSTER = Variable.get(f"conciliacion_dataproc_cluster_{env}")
DATAPROC_FILES = Variable.get(f"conciliacion_dataproc_files_{env}")
INPUT_FILES = Variable.get(f"conciliacion_inputs_{env}")
OUTPUT_DATASET = Variable.get(f"conciliacion_dataset_{env}")
SQL_FOLDER = Variable.get(f'sql_folder_{env}')
BACKUP_FOLDER = Variable.get(f"backup_folder_conciliacion_{env}")
SLACK_CONN_ID = f"slack_conn-{environment}"

# Cash in debito Parameters
REGION_CASH_IN_DEBITO = Variable.get(f"region_cash_in_debito_{env}")
DATAPROC_TEMPLATE_CASH_IN_DEBITO = Variable.get(f'conciliacion_dataproc_template_cash_in_debito_{env}')
CASH_IN_DEBITO_PREFIX = Variable.get(f"cash_in_debito_prefix_{env}")
CASH_IN_DEBITO_WORKERS = Variable.get(f"cash_in_debito_workers_{env}")
CASH_IN_DEBITO_TYPE_FILE = Variable.get(f"type_file_cash_in_debito_{env}")
CASH_IN_DEBITO_QUERY = Variable.get(f"cash_in_debito_gold_query_{env}")
MATCH_QUERY_CASH_IN_DEBITO = Variable.get(f"cash_in_debito_match_query_{env}")

# DAG args
default_args = {
    "owner": "tenpo",
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( minutes = 3 ),
    "email_on_failure": True,
    "email_on_retry": False,
    'catchup' : False,
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID)
}

#DAG
@dag(schedule_interval=None,
    default_args=default_args)
def tenpo_conciliaciones_cash_in_debito():

# Cash In debito Input conciliation process

    dataproc_cash_in_debito = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_cash_in_debito',
        project_id=PROJECT_NAME,
        region=REGION_CASH_IN_DEBITO,
        template_id=DATAPROC_TEMPLATE_CASH_IN_DEBITO,     
        parameters={
            'CLUSTER': f'{CLUSTER}-cash-in-debito-{DEPLOYMENT}',
            'NUMWORKERS':CASH_IN_DEBITO_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{CASH_IN_DEBITO_PREFIX}*',
            'TYPE_FILE':CASH_IN_DEBITO_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.cash_in_debito',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )
    
    cash_in_debito_gold = read_gcs_sql(CASH_IN_DEBITO_QUERY)

    match_cash_in_debito = read_gcs_sql(MATCH_QUERY_CASH_IN_DEBITO)
    
    move_files_cash_in_debito = GCSToGCSOperator(
        task_id='move_files_cash_in_debito',
        source_bucket=DATA_BUCKET,
        source_object=f'{CASH_IN_DEBITO_PREFIX}*',
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}cash_in_debito/LDN',
        move_object=True
        ) 
    

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end', trigger_rule='all_done'  )

# Task dependencies

    chain(
        start_task,
        dataproc_cash_in_debito,
        cash_in_debito_gold,
        match_cash_in_debito,
        move_files_cash_in_debito,
        end_task
    )

tenpo_conciliaciones_cash_in_debito = tenpo_conciliaciones_cash_in_debito()