from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import datetime
import logging

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

# DAG Variables used
env = Variable.get('env')
DEPLOYMENT = Variable.get(f"conciliacion_deployment_{env}")
PROJECT_NAME = Variable.get(f'datalake_{env}')
SOURCE_BUCKET = Variable.get(f'conciliacion_ops_bucket_{env}')
TARGET_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
DATA_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
PREFIX = Variable.get(f'sql_folder_{env}')
PYSPARK_FILE = Variable.get(f'conciliacion_pyspark_{env}')
DATAPROC_TEMPLATE_CCA = Variable.get(f'conciliacion_dataproc_template_cca_{env}')
CLUSTER = Variable.get(f"conciliacion_dataproc_cluster_{env}")
DATAPROC_FILES = Variable.get(f"conciliacion_dataproc_files_{env}")
INPUT_FILES = Variable.get(f"conciliacion_inputs_{env}")
OUTPUT_DATASET = Variable.get(f"conciliacion_dataset_{env}")
BACKUP_FOLDER = Variable.get(f"backup_folder_conciliacion_{env}")
CCA_PREFIX_1 = Variable.get(f"cca_prefix_1_{env}")
CCA_PREFIX_2 = Variable.get(f"cca_prefix_2_{env}")
CCA_PREFIX_3 = Variable.get(f"cca_prefix_3_{env}")
REGION_CCA = Variable.get(f"region_cca_{env}")
cca_type_file = Variable.get(f"type_file_cca_{env}")
cca_workers = Variable.get(f"cca_workers_{env}")
match_query = Variable.get("match_query")

# Reads sql files from GCS bucket
def read_gcs_sql(query):
    try:
        hook = GCSHook() 
        object_name = f'{PREFIX}/{query}'
        resp_byte = hook.download_as_byte_array(
            bucket_name=DATA_BUCKET,
            object_name=object_name,
        )
        resp_string = resp_byte.decode("utf-8")
        logging.info(resp_string)
        return resp_string
    except Exception as e:
        logging.error(f"Error occurred while reading SQL file from GCS: {str(e)}")

# Execute sql files read from GCS bucket
def query_bq(sql):
    try:
        hook = BigQueryHook(gcp_conn_id=GoogleBaseHook.default_conn_name, delegate_to=None, use_legacy_sql=False)
        client = bigquery.Client(project=hook._get_field(PROJECT_NAME))
        consulta = client.query(sql) 
        if consulta.errors:
            raise Exception('Query with ERROR')
        else:
            print('Query executed successfully!')
    except Exception as e:
        logging.error(f"Error occurred while executing BigQuery query: {str(e)}")

# Take action depending if there is a file or not
def file_availability(**kwargs):
    try:
        file_found = kwargs['ti'].xcom_pull(task_ids=kwargs['sensor_task'])
        if file_found:
            return kwargs['dataproc_task']
        return kwargs['sql_task']
    except Exception as e:
        print(f"An error occurred: {e}")
        return kwargs['sql_task']      

#DAG
with DAG(
    "tenpo_conciliaciones_cca",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 
    
    cca_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "cca_sensor",
        bucket=SOURCE_BUCKET,
        prefix=CCA_PREFIX,
        poke_interval=30 ,
        mode='reschedule',
        timeout=60,
        soft_fail=True,
        )
    
# Action defined by file availability
    
    cca_file_availability = BranchPythonOperator(
        task_id='cca_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'cca_sensor',
            'dataproc_task' : 'dataproc_cca',
            'sql_task' : 'read_match'           
            },
        provide_context=True,   
        trigger_rule='all_done'
        )

# Instantiate a dataproc workflow template for each type of file to process 
    
    dataproc_cca = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_cca',
        project_id=PROJECT_NAME,
        region=REGION_CCA,
        template_id=DATAPROC_TEMPLATE_CCA,     
        parameters={
            'CLUSTER': f'{CLUSTER}-cca-{DEPLOYMENT}',
            'NUMWORKERS':cca_workers,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':[f'{INPUT_FILES}{CCA_PREFIX_1}*', f'{INPUT_FILES}{CCA_PREFIX_2}*', f'{INPUT_FILES}{CCA_PREFIX_3}*'],
            'TYPE_FILE':cca_type_file,
            'OUTPUT':f'{OUTPUT_DATASET}.cca',
            'MODE_DEPLOY': DEPLOYMENT
        },
        ) 


# Read the sql file with the Match query for conciliation from a GCS bucket 
    read_match = PythonOperator(
        task_id='read_match',
        provide_context=True,
        python_callable=read_gcs_sql,
        trigger_rule='none_failed',
        op_kwargs={
        "query": match_query
        }
        )

# Execute the sql file with the Match query for conciliation     
    execute_match = PythonOperator(
        task_id='execute_match',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match') }}"
        }
        )

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end')

# Task dependencies


start_task >> cca_sensor >> cca_file_availability >> [dataproc_cca, read_match]

dataproc_cca >> read_match

read_match >> execute_match >> end_task


