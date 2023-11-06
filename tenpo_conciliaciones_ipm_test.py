from airflow.models import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from plugins.slack import get_task_success_slack_alert_callback
from plugins.conciliacion_tasks import *
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
import datetime

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

# IPM Parameters
REGION_IPM = Variable.get(f"region_ipm_{env}")
DATAPROC_TEMPLATE_IPM = Variable.get(f'conciliacion_dataproc_template_ipm_{env}')
IPM_PREFIX = Variable.get(f"ipm_prefix_{env}")
IPM_WORKERS = Variable.get(f"ipm_workers_{env}")
IPM_TYPE_FILE = Variable.get(f"type_file_ipm_{env}")
IPM_QUERY = Variable.get(f"ipm_gold_query_{env}")
MATCH_QUERY_IPM = Variable.get(f"ipm_match_query_{env}")

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
@dag(
    schedule_interval=None,
    default_args=default_args,
    catchup = False
)
def tenpo_conciliaciones_ipm_test():

    # IPM Input conciliation process 
    dataproc_ipm = DataprocInstantiateWorkflowTemplateOperator(
        task_id="dataproc_ipm",
        project_id=PROJECT_NAME,
        region=REGION_IPM,
        template_id=DATAPROC_TEMPLATE_IPM,
        parameters={
            'CLUSTER': f'{CLUSTER}-ipm-{DEPLOYMENT}',
            'NUMWORKERS': IPM_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{IPM_PREFIX}*',
            'TYPE_FILE':IPM_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.ipm',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )
    
    ipm_gold = read_gcs_sql(query=IPM_QUERY)
    
    match_ipm = read_gcs_sql(query=MATCH_QUERY_IPM)
    
    move_files_ipm = GCSToGCSOperator(
        task_id='move_files_ipm',
        source_bucket=DATA_BUCKET,
        source_objects=[f'{IPM_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}ipm/',
        move_object=True
        )
    
    # Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end', trigger_rule='all_done'  )

    chain(
        start_task
        , dataproc_ipm
        , ipm_gold
        , match_ipm
        , move_files_ipm
        , end_task
    )

tenpo_conciliaciones_ipm_test = tenpo_conciliaciones_ipm_test()
