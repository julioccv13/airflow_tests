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

# DAG general parameters
env = Variable.get('env')
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

# OPD Parameters
OPD_PREFIX = Variable.get(f"opd_prefix_{env}")
OPD_SOURCE = Variable.get(f"opd_source_{env}")
OPD_PATH_FILES = Variable.get(f"opd_path_files_{env}")

# IPM Parameters
IPM_PREFIX = Variable.get(f"ipm_prefix_{env}")
IPM_SOURCE = Variable.get(f"ipm_source_{env}")
IPM_PATH_FILES = Variable.get(f"ipm_path_files_{env}")

# Anulation Parameters
ANULATION_PREFIX = Variable.get(f"anulation_prefix_{env}")
ANULATION_SOURCE = Variable.get(f"anulation_source_{env}")
ANULATION_PATH_FILES = Variable.get(f"anulation_path_files_{env}")

# Incident parameters
INCIDENT_PREFIX = Variable.get(f"incident_prefix_{env}")
INCIDENT_SOURCE = Variable.get(f"incident_source_{env}")
INCIDENT_PATH_FILES = Variable.get(f"incident_path_files_{env}")

# CCA Parameters
CCA_PREFIX = Variable.get(f"cca_prefix_{env}")
CCA_SOURCE = Variable.get(f"cca_source_{env}")
CCA_PATH_FILES = Variable.get(f"cca_path_files_{env}")

# PDC Parameters
PDC_PREFIX = Variable.get(f"pdc_prefix_{env}")
PDC_SOURCE = Variable.get(f"pdc_source_{env}")
PDC_PATH_FILES = Variable.get(f"pdc_path_files_{env}")

# Recargas App Parameters
RECARGAS_PREFIX = Variable.get(f"recargas_prefix_{env}")
RECARGAS_SOURCE = Variable.get(f"recargas_source_{env}")
RECARGAS_PATH_FILES = Variable.get(f"recargas_path_files_{env}")

# Invoke cloud function
def invoke_cloud_function(**kwargs):
    try:
        url = "https://us-central1-tenpo-datalake-sandbox.cloudfunctions.net/function-copy-new-files" #the url is also the target audience. 
        request = google.auth.transport.requests.Request()  #this is a request for obtaining the the credentials
        id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request) # If your cloud function url has query parameters, remove them before passing to the audience 
        headers = {"Content-Type": "application/json"}
        body = {
                "project_source": kwargs['project_source'],
                "project_target":kwargs['project_target'],
                "bucket_source": kwargs['bucket_source'],
                "bucket_target": kwargs['bucket_target'],
                "path_source": kwargs['path_source'],
                "backup_path": kwargs['backup_path'],
                "input_path_files": kwargs['input_path_files']
        }
        resp = AuthorizedSession(id_token_credentials).post(url=url, json=body, headers=headers) # the authorized session object is used to access the Cloud Function
        print(resp.status_code) # should return 200
        print(resp.content) # the body of the HTTP response
    except Exception as e:
        print(f"An error occurred: {e}")
        return False  

#DAG
with DAG(
    "tenpo_conciliaciones_cf_test",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 
    
# OPD Process trigger
    opd_cf = PythonOperator(
        task_id="opd_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": OPD_SOURCE,
                "backup_path": f'{BACKUP_FOLDER}cca/',
                "input_path_files": OPD_PATH_FILES
        }
        )

# IPM Process trigger
    ipm_cf = PythonOperator(
        task_id="ipm_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": IPM_SOURCE,
                "backup_path": f'{BACKUP_FOLDER}ipm/',
                "input_path_files": IPM_PATH_FILES
        }
        )

# Anulation process trigger
    anulation_cf = PythonOperator(
        task_id="anulation_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": ANULATION_SOURCE,
                "backup_path": f'{BACKUP_FOLDER}anulation/',
                "input_path_files": ANULATION_PATH_FILES
        }
        )

# Incident process trigger
    incident_cf = PythonOperator(
        task_id="incident_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": INCIDENT_SOURCE,
                "backup_path": f'{BACKUP_FOLDER}incident/',
                "input_path_files": INCIDENT_PATH_FILES
        }
        )

# CCA Process trigger
    cca_cf = PythonOperator(
        task_id="cca_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": CCA_SOURCE,
                "backup_path": f'{BACKUP_FOLDER}cca/',
                "input_path_files": CCA_PATH_FILES
        }
        )

# PDC Process trigger
    pdc_cf = PythonOperator(
        task_id="pdc_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": PDC_SOURCE,
                "backup_path": f'{BACKUP_FOLDER}pdc/',
                "input_path_files": PDC_PATH_FILES
        }
        )

# Recargas process trigger
    recargas_cf = PythonOperator(
        task_id="recargas_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": RECARGAS_SOURCE,
                "backup_path": f'{BACKUP_FOLDER}recargas/',
                "input_path_files": RECARGAS_PATH_FILES
        }
        )

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end_task', trigger_rule = 'all_done')

# Task dependencies



start_task >> [opd_cf, ipm_cf, anulation_cf, incident_cf, cca_cf, pdc_cf, recargas_cf] >> end_task


