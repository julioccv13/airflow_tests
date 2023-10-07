from airflow.models import DAG
from airflow.models import Variable
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import datetime
import logging
import json

default_args = {
    "owner": "tenpo",
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( minutes = 1 ),
    "email_on_failure": True,
    "email_on_retry": False,
    'catchup' : False
}

# DAG general parameters
env = Variable.get('env')
#conf = "{{ dag_run.conf[dag_params] }}"
PROJECT_NAME = Variable.get(f'datalake_{env}')
SOURCE_PROJECT = Variable.get(f'conciliacion_source_project_{env}')
SOURCE_BUCKET = Variable.get(f'conciliacion_ops_bucket_{env}')
TARGET_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
DATA_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
DEPLOYMENT = Variable.get(f"conciliacion_deployment_{env}")
PYSPARK_FILE = Variable.get(f'conciliacion_pyspark_{env}')
CLUSTER = Variable.get(f"conciliacion_dataproc_cluster_{env}")
DATAPROC_FILES = Variable.get(f"conciliacion_dataproc_files_{env}")
OUTPUT_DATASET = Variable.get(f"conciliacion_dataset_{env}")
SQL_FOLDER = Variable.get(f'sql_folder_{env}')
BACKUP_FOLDER = Variable.get(f"backup_folder_conciliacion_{env}")

#DAG
@dag(dag_id =  "tenpo_conciliaciones_recargas", schedule_interval='0 8,16,20 * * *', default_args=default_args)
def tenpo_conciliaciones_recargas():
    
# Recargas input conciliation process     
    dataproc_recargas = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_recargas',
        project_id=PROJECT_NAME,
        region="{{ dag_run.conf.region}}",
        template_id="{{ dag_run.conf.dataproc_template}}",     
        parameters={
            'CLUSTER': f'{CLUSTER}-recargas-{DEPLOYMENT}',
            'NUMWORKERS':"{{ dag_run.conf.workers}}",
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'"{{ dag_run.conf.prefix}}"*',
            'TYPE_FILE':"{{ dag_run.conf.type_file}}",
            'OUTPUT':f'{OUTPUT_DATASET}.recargas_app',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )
    
    @task
    def read_gcs_sql():
        try:
            hook = GCSHook() 
            object_name = f'{SQL_FOLDER}/"{{ dag_run.conf.query}}"'
            resp_byte = hook.download_as_byte_array(
                bucket_name=DATA_BUCKET,
                object_name=object_name,
            )
            resp_string = resp_byte.decode("utf-8")
            logging.info(resp_string)
            return resp_string
        except Exception as e:
            logging.error(f"Error occurred while reading SQL file from GCS: {str(e)}")

    @task
    def execute_query(resp_string):
        try:
            hook = BigQueryHook(gcp_conn_id=GoogleBaseHook.default_conn_name, delegate_to=None, use_legacy_sql=False)
            client = bigquery.Client(project=hook._get_field(PROJECT_NAME))
            consulta = client.query(resp_string) 
            if consulta.errors:
                raise Exception('Query with ERROR')
            else:
                print('Query executed successfully!')
        except Exception as e:
            logging.error(f"Error occurred while executing BigQuery query: {str(e)}")    

    move_files_recargas = GCSToGCSOperator(
        task_id='move_files_recargas',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'"{{ dag_run.conf.prefix}}"*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}recargas/',
        move_object=True
        ) 
    
# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end', trigger_rule='all_done'  )

# Task dependencies

    start_task >> dataproc_recargas >> execute_query(read_gcs_sql()) >> move_files_recargas >> end_task

tenpo_conciliaciones_recargas = tenpo_conciliaciones_recargas()