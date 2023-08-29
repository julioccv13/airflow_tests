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
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
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
DATAPROC_TEMPLATE_RECARGAS = Variable.get(f'conciliacion_dataproc_template_recargas_{env}')
CLUSTER = Variable.get(f"conciliacion_dataproc_cluster_{env}")
DATAPROC_FILES = Variable.get(f"conciliacion_dataproc_files_{env}")
INPUT_FILES = Variable.get(f"conciliacion_inputs_{env}")
OUTPUT_DATASET = Variable.get(f"conciliacion_dataset_{env}")
BACKUP_FOLDER = Variable.get(f"backup_folder_conciliacion_{env}")
RECARGAS_PREFIX = Variable.get(f"recargas_prefix_{env}")
REGION_RECARGAS = Variable.get(f"region_recargas_{env}")
recargas_type_file = Variable.get(f"type_file_recargas_{env}")
recargas_workers = Variable.get(f"recargas_workers_{env}")
match_query = Variable.get(f"match_query_{env}")
FUNCTION_NAME = 'function-cp-bucket'
CF_BODY = Variable.get('cf_body')


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
            return kwargs['cf_task']
        return kwargs['sql_task']
    except Exception as e:
        print(f"An error occurred: {e}")
        return kwargs['sql_task']      
    
# Invoke cloud function
def invoke_cloud_function():
    try:
        url = "https://us-central1-tenpo-datalake-sandbox.cloudfunctions.net/function-cp-bucket" #the url is also the target audience. 
        request = google.auth.transport.requests.Request()  #this is a request for obtaining the the credentials
        id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request) # If your cloud function url has query parameters, remove them before passing to the audience 
        headers = {"Content-Type": "application/json"}
        body = {
                "project_source": "tenpo-datalake-sandbox",
                "project_target": "tenpo-datalake-sandbox",
                "bucket_source": "mark-vii-conciliacion",
                "bucket_target": "mark-vii-conciliacion",
                "path_source": "data/recargas_app/",
                "path_target": "test/function/cca/",
                "input_path_files": "data/new_files/recargas_app/"
        }
        resp = AuthorizedSession(id_token_credentials).post(url=url, json=body, headers=headers) # the authorized session object is used to access the Cloud Function
        print(resp.status_code) # should return 200
        print(resp.content) # the body of the HTTP response
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False  

def file_availability(**kwargs):
    try:
        file_found = kwargs['ti'].xcom_pull(task_ids=kwargs['cf_task'])
        if file_found:
            return kwargs['test']
        return kwargs['end']
    except Exception as e:
        print(f"An error occurred: {e}")
        return kwargs['end'] 

#DAG
with DAG(
    "tenpo_conciliaciones_cf_test",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 
    
# Cloud function to copy files to local
    invoke_cf = PythonOperator(task_id="invoke_cf", python_callable=invoke_cloud_function)

    file_availability = BranchPythonOperator(
        task_id='file_availability',
        python_callable=file_availability,
        op_kwargs={
            'cf_task': 'incident_sensor',
            'test' : 'test_task',
            'end' : 'end_task'         
            },
        provide_context=True,   
        trigger_rule='all_done'
        )

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    test_task = DummyOperator( task_id = 'test_task')

    end_task = DummyOperator( task_id = 'end_task')

# Task dependencies



start_task >> invoke_cf >> file_availability >> [test_task, end_task]
test_task >> end_task


