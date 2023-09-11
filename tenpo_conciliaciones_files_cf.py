from airflow.models import DAG
from airflow.models import Variable
from plugins.slack import get_task_success_slack_alert_callback
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from airflow.utils.dates import days_ago
import datetime

# DAG general parameters
env = Variable.get('env')
environment = Variable.get("environment")
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
CF_URL = Variable.get(f'conciliacion_cf_url_{env}')
SLACK_CONN_ID = f"slack_conn-{environment}"

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

# POS Parameters
POS_PREFIX = Variable.get(f"pos_prefix_{env}")
POS_SOURCE = Variable.get(f"pos_source_{env}")
POS_PATH_FILES = Variable.get(f"pos_path_files_{env}")

# Remesas Parameters
REMESAS_PREFIX = Variable.get(f"remesas_prefix_{env}")
REMESAS_SOURCE = Variable.get(f"remesas_source_{env}")
REMESAS_PATH_FILES = Variable.get(f"remesas_path_files_{env}")

# Invoke cloud function
def invoke_cloud_function(**kwargs):
    try:
        url = CF_URL
        request = google.auth.transport.requests.Request()  
        id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request)
        headers = {"Content-Type": "application/json"}
        body = {
                "project_source": kwargs['project_source'],
                "project_target":kwargs['project_target'],
                "bucket_source": kwargs['bucket_source'],
                "bucket_target": kwargs['bucket_target'],
                "path_source": kwargs['path_source'],
                "path_target": kwargs['path_target'],
                "input_path_files": kwargs['input_path_files']
        }
        resp = AuthorizedSession(id_token_credentials).post(url=url, json=body, headers=headers) 
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='cloud_function_response', value=resp.content.decode('utf-8'))
        print(resp.status_code) # should return 200
        print(resp.content) # the body of the HTTP response
    except Exception as e:
        print(f"An error occurred: {e}")
        return False  
    
def cf_response(**kwargs):
    try:
        ti = kwargs['ti']        
        opd_resp = ti.xcom_pull(task_ids='opd_cf', key='cloud_function_response')
        ipm_resp = ti.xcom_pull(task_ids='ipm_cf', key='cloud_function_response')
        anulation_resp = ti.xcom_pull(task_ids='anulation_cf', key='cloud_function_response')
        incident_resp = ti.xcom_pull(task_ids='incident_cf', key='cloud_function_response')
        cca_resp = ti.xcom_pull(task_ids='cca_cf', key='cloud_function_response')
        pdc_resp = ti.xcom_pull(task_ids='pdc_cf', key='cloud_function_response')
        pos_resp = ti.xcom_pull(task_ids='pos_cf', key='cloud_function_response')
        recargas_resp = ti.xcom_pull(task_ids='recargas_cf', key='cloud_function_response')
        remesas_resp = ti.xcom_pull(task_ids='remesas_cf', key='cloud_function_response')
        if any(resp == "Done!!" for resp in [opd_resp
                                                  , ipm_resp
                                                  , anulation_resp
                                                  , incident_resp
                                                  , cca_resp
                                                  , pdc_resp
                                                  , recargas_resp
                                                  , pos_resp
                                                  , remesas_resp]):
            return 'trigger_task'
        return 'end_task'
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'end_task'
    
# DAG args
default_args = {
    "owner": "tenpo",
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( minutes = 10 ),
    "email_on_failure": True,
    "email_on_retry": False,
    'catchup' : False,
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID)
}

#DAG
with DAG(
    "tenpo_conciliaciones_files_cf",
    schedule_interval='@hourly',
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
                "path_target": f'{BACKUP_FOLDER}opd/',
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
                "path_target": f'{BACKUP_FOLDER}ipm/',
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
                "path_target": f'{BACKUP_FOLDER}anulation/',
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
                "path_target": f'{BACKUP_FOLDER}incident/',
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
                "path_target": f'{BACKUP_FOLDER}cca/',
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
                "path_target": f'{BACKUP_FOLDER}pdc/',
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
                "path_target": f'{BACKUP_FOLDER}recargas/',
                "input_path_files": RECARGAS_PATH_FILES
        }
        )

# POS process trigger
    pos_cf = PythonOperator(
        task_id="pos_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": POS_SOURCE,
                "path_target": f'{BACKUP_FOLDER}pos/',
                "input_path_files": POS_PATH_FILES
        }
        )
    
# Remesas process trigger
    remesas_cf = PythonOperator(
        task_id="remesas_cf"
        , provide_context=True
        , python_callable=invoke_cloud_function
        , op_kwargs={
                "project_source": SOURCE_PROJECT,
                "project_target":PROJECT_NAME,
                "bucket_source": SOURCE_BUCKET,
                "bucket_target": TARGET_BUCKET,
                "path_source": REMESAS_SOURCE,
                "path_target": f'{BACKUP_FOLDER}remesas/',
                "input_path_files": REMESAS_PATH_FILES
        }
        )

# Dummy tasks       
    check_cf_results = BranchPythonOperator(
        task_id = 'check_cf_results',
        python_callable = cf_response,
        provide_context = True,
        trigger_rule='all_done'
    )
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task'
        , trigger_dag_id="tenpo_conciliaciones_prd"
    )

    start_task = DummyOperator( task_id = 'start_task')

    end_task = DummyOperator( task_id = 'end_task', trigger_rule = 'all_done')

# Task dependencies



start_task >> [opd_cf, ipm_cf, anulation_cf, incident_cf, cca_cf, pdc_cf, recargas_cf, pos_cf, remesas_cf] >> check_cf_results >>[trigger_task, end_task]

trigger_task >> end_task
