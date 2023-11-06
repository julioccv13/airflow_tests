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
REGION_CCA = Variable.get(f"region_cca_{env}")
DATAPROC_TEMPLATE_CCA = Variable.get(f'conciliacion_dataproc_template_cca_{env}')
CCA_PREFIX = Variable.get(f"cca_prefix_{env}")
CCA_WORKERS = Variable.get(f"cca_workers_{env}")
CCA_TYPE_FILE = Variable.get(f"type_file_cca_{env}")
CCA_QUERY = Variable.get(f"cca_gold_query_{env}")
MATCH_QUERY_CCA = Variable.get(f"cca_match_query_{env}")

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
def tenpo_conciliaciones_cca():

# CCA Input conciliation process

    dataproc_cca = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_cca',
        project_id=PROJECT_NAME,
        region=REGION_CCA,
        template_id=DATAPROC_TEMPLATE_CCA,     
        parameters={
            'CLUSTER': f'{CLUSTER}-cca-{DEPLOYMENT}',
            'NUMWORKERS':CCA_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{CCA_PREFIX}*',
            'TYPE_FILE':CCA_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.cca',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )
    
    cca_gold = read_gcs_sql(CCA_QUERY)

    match_cca = read_gcs_sql(MATCH_QUERY_CCA)
    
    move_files_cca = GCSToGCSOperator(
        task_id='move_files_cca',
        source_bucket=DATA_BUCKET,
        source_object=f'{CCA_PREFIX}*',
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}cca/EX',
        move_object=True
        ) 
    

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end', trigger_rule='all_done'  )

    chain(
            start_task,
            dataproc_cca,
            cca_gold,
            match_cca,
            move_files_cca,
            end_task
        )

tenpo_conciliaciones_cca = tenpo_conciliaciones_cca()