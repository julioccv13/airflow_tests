from airflow.models import DAG
from airflow.models import Variable
from plugins.slack import get_task_success_slack_alert_callback
from plugins.conciliacion_tasks import *
from airflow.decorators import task, dag
from plugins.slack import get_task_success_slack_alert_callback
from plugins.conciliacion import *
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from airflow.utils.dates import days_ago
import datetime

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


#DAG
@dag(
    schedule_interval='0 8,16,22 * * *',
    default_args=default_args,
    start_date=days_ago(1), 
    catchup=False
)
def tenpo_conciliaciones_process_control():
    
# OPD Process trigger
    opd_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= OPD_SOURCE,
        path_target= f'{BACKUP_FOLDER}opdV2/',
        input_path_files= OPD_PATH_FILES
        )
    
    trigger_opd = TriggerDagRunOperator(
        task_id='trigger_opd'
        , trigger_dag_id="tenpo_conciliaciones_opd_prd"
        , wait_for_completion=True
    )

# IPM Process trigger
    ipm_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= IPM_SOURCE,
        path_target= f'{BACKUP_FOLDER}ipm/',
        input_path_files= IPM_PATH_FILES
        )
    
    trigger_ipm = TriggerDagRunOperator(
        task_id='trigger_ipm'
        , trigger_dag_id="tenpo_conciliaciones_ipm_test"
        , wait_for_completion=True
    )

# Anulation process trigger
    anulation_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= ANULATION_SOURCE,
        path_target= f'{BACKUP_FOLDER}anulation/',
        input_path_files= ANULATION_PATH_FILES
        )
    
    trigger_anulation = TriggerDagRunOperator(
        task_id='trigger_anulation'
        , trigger_dag_id="tenpo_conciliaciones_anulation_prd"
        , wait_for_completion=True
    )

# Incident process trigger
    incident_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= INCIDENT_SOURCE,
        path_target= f'{BACKUP_FOLDER}incident/',
        input_path_files= INCIDENT_PATH_FILES
        )
    
    trigger_incident = TriggerDagRunOperator(
        task_id='trigger_incident'
        , trigger_dag_id="tenpo_conciliaciones_incident_prd"
        , wait_for_completion=True
    )

# CCA Process trigger
    cca_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= CCA_SOURCE,
        path_target= f'{BACKUP_FOLDER}cca/',
        input_path_files= CCA_PATH_FILES
        )
    
    trigger_cca = TriggerDagRunOperator(
        task_id='trigger_cca'
        , trigger_dag_id="tenpo_conciliaciones_cca_prd"
        , wait_for_completion=True
    )

# PDC Process trigger
    pdc_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= PDC_SOURCE,
        path_target= f'{BACKUP_FOLDER}pdc/',
        input_path_files= PDC_PATH_FILES
        )
    
    trigger_pdc = TriggerDagRunOperator(
        task_id='trigger_pdc'
        , trigger_dag_id="tenpo_conciliaciones_pdc_prd"
        , wait_for_completion=True
    )

# Recargas process trigger
    recargas_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= RECARGAS_SOURCE,
        path_target= f'{BACKUP_FOLDER}recargas/',
        input_path_files= RECARGAS_PATH_FILES
        )
    
    trigger_recargas = TriggerDagRunOperator(
        task_id='trigger_recargas'
        , trigger_dag_id="tenpo_conciliaciones_recargas_prd"
        , wait_for_completion=True
    )

# POS process trigger
    pos_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= POS_SOURCE,
        path_target= f'{BACKUP_FOLDER}pos/',
        input_path_files= POS_PATH_FILES
        )
    
    trigger_pos = TriggerDagRunOperator(
        task_id='trigger_pos'
        , trigger_dag_id="tenpo_conciliaciones_pos_prd"
        , wait_for_completion=True
    )
    
# Remesas process trigger
    remesas_cf = call_cloud_function(
        project_source= SOURCE_PROJECT,
        project_target=PROJECT_NAME,
        bucket_source= SOURCE_BUCKET,
        bucket_target= TARGET_BUCKET,
        path_source= REMESAS_SOURCE,
        path_target= f'{BACKUP_FOLDER}remesas/',
        input_path_files= REMESAS_PATH_FILES
        )
    
    trigger_remesas = TriggerDagRunOperator(
        task_id='trigger_remesas'
        , trigger_dag_id="tenpo_conciliaciones_remesas_prd"
        , wait_for_completion=True
    )

# Dummy tasks       
    start_task = DummyOperator( task_id = 'start_task')

    end_task = DummyOperator( task_id = 'end_task', trigger_rule = 'all_done')

# Task dependencies

    start_task >> opd_cf >> trigger_opd >> end_task
    start_task >> ipm_cf >> trigger_ipm >> pdc_cf >> trigger_pdc >> end_task
    start_task >> anulation_cf >> trigger_anulation >> pos_cf >> trigger_pos >> remesas_cf >> trigger_remesas >> end_task
    start_task >> incident_cf >> trigger_incident >> cca_cf >> trigger_cca >> recargas_cf >> trigger_recargas >> end_task

tenpo_conciliaciones_process_control = tenpo_conciliaciones_process_control()





