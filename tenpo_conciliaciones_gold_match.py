from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
GCP_REGION = Variable.get('REGION')
PROJECT_NAME = 'tenpo-mark-vii'
SOURCE_BUCKET = 'tenpo-mark-vii'
TARGET_BUCKET = 'tenpo-mark-vii-backup'
PREFIX = 'sql'

# Reads sql files from GCS bucket
def read_gcs_sql(query):
    hook = GCSHook() 
    if PREFIX:
        object_name = f'{PREFIX}/{query}'
    else:
        object_name = f'{query}'
    resp_byte = hook.download_as_byte_array(
    bucket_name = SOURCE_BUCKET,
    object_name = object_name,
            )

    resp_string = resp_byte.decode("utf-8")
    logging.info(resp_string)
    return resp_string

# Execute sql files read from GCS bucket
def query_bq(sql):
    hook = BigQueryHook(gcp_conn_id= GoogleBaseHook.default_conn_name , delegate_to=None, use_legacy_sql=False)
    client = bigquery.Client(project=hook._get_field("tenpo-mark-vii"))
    consulta = client.query(sql) 
    if consulta.errors:
        raise Exception('Query con ERROR')
    else:
        print('Query perfect!')

# Mark failed tasks as skipped
def task_failure(context):
    if context['exception']:
        task_instance = context['task_instance']
        task_instance.state = State.SKIPPED
        task_instance.log.info('Task skipped.')        

with DAG(
    "tenpo_conciliaciones_gold_match",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 

# GCS sensor for each file type    
    opd_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "opd_sensor",
        bucket=SOURCE_BUCKET,
        prefix='test/opd_v2_encrypted/PLJ61110.FINT0003',
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    ipm_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "ipm_sensor",
        bucket=SOURCE_BUCKET,
        prefix='test/query_test/ipm_test/MCI.AR.T112.M.E0073610.D',
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    anulation_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "anulation_sensor",
        bucket=SOURCE_BUCKET,
        prefix='test/anulation_files/PLJ00032.TRXS.ANULADAS',
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    incident_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "incident_sensor",
        bucket=SOURCE_BUCKET,
        prefix='test/anulation_files/PLJ62100-CONS-INC-PEND-TENPO',
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
# Bash commands to start a dataproc workflow template to process each file type
    dataproc_ipm = BashOperator(
        task_id="start_dataproc_ipm",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-ipm-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/query_test/ipm_test/MCI.AR.T112.M.E0073610.D*,TYPE_FILE=ipm,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev.ipm,MODE_DEPLOY=prod",
        )

    dataproc_opd = BashOperator(
        task_id="start_dataproc_opd",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-opd-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/opd_v2_encrypted/PLJ61110.FINT0003*,TYPE_FILE=opd,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev,MODE_DEPLOY=prod",
        )
    
    dataproc_anulation = BashOperator(
        task_id="start_dataproc_anulation",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-opd-anulation-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/anulation_files/PLJ00032.TRXS.ANULADAS*,TYPE_FILE=anulation,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev.opd_anulation,MODE_DEPLOY=prod&",
        )
    
    dataproc_incident = BashOperator(
        task_id="start_dataproc_incident",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-opd-incident-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/anulation_files/PLJ62100-CONS-INC-PEND-TENPO*,TYPE_FILE=incident,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev.opd_incident,MODE_DEPLOY=prod&",
        )

# Read a sql file for each type of file pass the tables from staging to gold    
    read_ipm_gold = PythonOperator(
        task_id='read_ipm_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": "ipm_staging_to_gold.sql"
        }
        )

    read_opd_gold = PythonOperator(
        task_id='read_opd_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": "opd_2_staging_to_gold.sql"
        }
        )      
    
    read_anulation_gold = PythonOperator(
        task_id='read_anulation_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": "opd_anulation_staging_to_gold.sql"
        }
        )     
    
    read_incident_gold = PythonOperator(
        task_id='read_incident_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": "opd_incident_staging_to_gold.sql"
        }
        )     

# Execute a sql file for each type of file pass the tables from staging to gold     
    execute_ipm_gold = PythonOperator(
        task_id='execute_ipm_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_ipm_gold') }}"
        }
        )

    execute_opd_gold = PythonOperator(
        task_id='execute_opd_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_opd_gold') }}"
        }
        )
    
    execute_incident_gold = PythonOperator(
        task_id='execute_incident_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_incident_gold') }}"
        }
        )
    
    execute_anulation_gold = PythonOperator(
        task_id='execute_anulation_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_anulation_gold') }}"
        }
        )

# Read the sql file with the Match query for conciliation from a GCS bucket 
    read_match = PythonOperator(
        task_id='read_match',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": "match.sql"
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

# Move the files of the day from the source bucket to a backup bucket to leave it empty for the next day process        
    move_files_ipm = GCSToGCSOperator(
        task_id='move_files_ipm',
        source_bucket=SOURCE_BUCKET,
        source_objects=['test/query_test/ipm_test/MCI.AR.T112.M.E0073610.D*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='ipm_backup/MCI.AR.T112.M.E0073610.D',
        move_object=True
    )

    move_files_opd = GCSToGCSOperator(
        task_id='move_files_opd',
        source_bucket=SOURCE_BUCKET,
        source_objects=['test/opd_v2_encrypted/PLJ61110.FINT0003*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='opd_backup/PLJ61110.FINT0003',
        move_object=True
    )

    move_files_anulation = GCSToGCSOperator(
        task_id='move_files_anulation',
        source_bucket=SOURCE_BUCKET,
        source_objects=['test/anulation_files/PLJ00032.TRXS.ANULADAS*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='anulation_backup/PLJ00032.TRXS.ANULADAS',
        move_object=True
    )

    move_files_incident = GCSToGCSOperator(
        task_id='move_files_incident',
        source_bucket=SOURCE_BUCKET,
        source_objects=['test/anulation_files/PLJ62100-CONS-INC-PEND-TENPO*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='incident_backup/PLJ62100-CONS-INC-PEND-TENPO',
        move_object=True
    )

# Dummy tasks        
    start_task = EmptyOperator( task_id = 'start')

    end_task = EmptyOperator( task_id = 'end')

# Task dependencies
start_task >> [opd_sensor, ipm_sensor, anulation_sensor, incident_sensor]
opd_sensor >> dataproc_opd >> read_opd_gold >> execute_opd_gold
ipm_sensor >> dataproc_ipm >> read_ipm_gold >> execute_ipm_gold
anulation_sensor >> dataproc_anulation >> read_anulation_gold >> execute_anulation_gold
incident_sensor >> dataproc_incident >> read_incident_gold >> execute_incident_gold
[execute_ipm_gold, execute_opd_gold, execute_incident_gold, execute_anulation_gold] >> read_match >> execute_match 
execute_match >> [move_files_ipm, move_files_opd, move_files_anulation, move_files_incident]>> end_task

# Failure callback
dag.on_failure_callback = task_failure



