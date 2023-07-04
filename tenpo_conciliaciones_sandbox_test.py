from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.trigger_rule import TriggerRule
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
    "email_on_failure": True,
    "email_on_retry": False,
}

# DAG Variables used
PROJECT_NAME = Variable.get('project1')
SOURCE_PROJECT = Variable.get('conciliacion_uat')
SOURCE_BUCKET = 'conciliacion-uat-test'
TARGET_BUCKET = 'mark-vii-conciliacion'
DATA_BUCKET = 'mark-vii-conciliacion'
PREFIX = 'sql'
GCS_CONN_ID = 'conciliacion_uat'

# Reads sql files from GCS bucket
def read_gcs_sql(query):
    hook = GCSHook() 
    if PREFIX:
        object_name = f'{PREFIX}/{query}'
    else:
        object_name = f'{query}'
    resp_byte = hook.download_as_byte_array(
    bucket_name = DATA_BUCKET,
    object_name = object_name,
            )

    resp_string = resp_byte.decode("utf-8")
    logging.info(resp_string)
    return resp_string

# Execute sql files read from GCS bucket
def query_bq(sql):
    hook = BigQueryHook(gcp_conn_id= GoogleBaseHook.default_conn_name , delegate_to=None, use_legacy_sql=False)
    client = bigquery.Client(project=hook._get_field("tenpo-datalake-sandbox"))
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
    "tenpo_conciliaciones_sandbox_test",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 

# GCS sensor for each file type    
    opd_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "opd_sensor",
        bucket=SOURCE_BUCKET,
        prefix='data/opd/PLJ61110.FINT0003',
        google_cloud_conn_id=GCS_CONN_ID,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    ipm_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "ipm_sensor",
        bucket=SOURCE_BUCKET,
        prefix='data/ipm/MCI.AR.T112.M.E0073610.D',
        google_cloud_conn_id=GCS_CONN_ID,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    anulation_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "anulation_sensor",
        bucket=SOURCE_BUCKET,
        prefix='data/anulation/PLJ00032.TRXS.ANULADAS',
        google_cloud_conn_id=GCS_CONN_ID,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    incident_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "incident_sensor",
        bucket=SOURCE_BUCKET,
        prefix='data/incident/PLJ62100-CONS-INC-PEND-TENPO',
        google_cloud_conn_id=GCS_CONN_ID,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
# Instantiate a dataproc workflow template for each type of file to process 
    dataproc_ipm = DataprocInstantiateWorkflowTemplateOperator(
        task_id="dataproc_ipm",
        project_id=PROJECT_NAME,
        region='us-central1',
        template_id='template_process_file',
        gcp_conn_id='conciliacion_uat',
        parameters={
            'CLUSTER':'tenpo-ipm-prod',
            'NUMWORKERS':'4',
            'JOBFILE':'gs://mark-vii-conciliacion/artifacts/dataproc/pyspark_data_process.py',
            'FILES_OPERATORS':'gs://mark-vii-conciliacion/artifacts/dataproc/operators/*',
            'INPUT':'gs://conciliacion-uat-test@tenpo-mark-vii/data/ipm/MCI.AR.T112.M.E0073610.D*',
            'TYPE_FILE':'ipm',
            'OUTPUT':'tenpo-datalake-sandbox.tenpo_conciliacion_staging_dev.ipm',
            'MODE_DEPLOY':'prod'
        },
        )

    dataproc_opd = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_opd',
        project_id=PROJECT_NAME,
        region='us-central1',
        template_id='template_process_file',
        gcp_conn_id='conciliacion_uat',
        parameters={
            'CLUSTER':'tenpo-opd-prod',
            'NUMWORKERS':'8',
            'JOBFILE':'gs://mark-vii-conciliacion/artifacts/dataproc/pyspark_data_process.py',
            'FILES_OPERATORS':'gs://mark-vii-conciliacion/artifacts/dataproc/operators/*',
            'INPUT':'gs://conciliacion-uat-test@tenpo-mark-vii/data/opd/PLJ61110.FINT0003*',
            'TYPE_FILE':'opd',
            'OUTPUT':'tenpo-datalake-sandbox.tenpo_conciliacion_staging_dev',
            'MODE_DEPLOY':'prod'
        },
        )

    dataproc_anulation = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_anulation',
        project_id=PROJECT_NAME,
        region='us-central1',
        template_id='template_process_file',
        gcp_conn_id='conciliacion_uat',
        parameters={
            'CLUSTER':'tenpo-opd-anulation-prod',
            'NUMWORKERS':'4',
            'JOBFILE':'gs://mark-vii-conciliacion/artifacts/dataproc/pyspark_data_process.py',
            'FILES_OPERATORS':'gs://mark-vii-conciliacion/artifacts/dataproc/operators/*',
            'INPUT' : 'gs://conciliacion-uat-test@tenpo-mark-vii/PLJ00032.TRXS.ANULADAS*',
            'TYPE_FILE':'anulation',
            'OUTPUT':'tenpo-datalake-sandbox.tenpo_conciliacion_staging_dev.opd_anulation',
            'MODE_DEPLOY':'prod'
        },
        )

    dataproc_incident = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_incident',
        project_id=PROJECT_NAME,
        region='us-central1',
        template_id='template_process_file',
        gcp_conn_id='conciliacion_uat',
        parameters={
            'CLUSTER':'tenpo-opd-incident-prod',
            'NUMWORKERS':'4',
            'JOBFILE':'gs://mark-vii-conciliacion/artifacts/dataproc/pyspark_data_process.py',
            'FILES_OPERATORS':'gs://mark-vii-conciliacion/artifacts/dataproc/operators/*',
            'INPUT':'gs://conciliacion-uat-test@tenpo-mark-vii/data/incident/PLJ62100-CONS-INC-PEND-TENPO*',
            'TYPE_FILE':'incident',
            'OUTPUT':'tenpo-datalake-sandbox.tenpo_conciliacion_staging_dev.opd_incident',
            'MODE_DEPLOY':'prod'
        },
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
        trigger_rule=TriggerRule.ALL_DONE,
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
        gcp_conn_id=GCS_CONN_ID,
        source_bucket=SOURCE_BUCKET,
        source_objects=['data/ipm/MCI.AR.T112.M.E0073610.D*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='conciliacion_backup/ipm/MCI.AR.T112.M.E0073610.D',
        move_object=True
        )

    move_files_opd = GCSToGCSOperator(
        task_id='move_files_opd',
        gcp_conn_id=GCS_CONN_ID,
        source_bucket=SOURCE_BUCKET,
        source_objects=['data/opd/PLJ61110.FINT0003*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='conciliacion_backup/opd/PLJ61110.FINT0003',
        move_object=True
        )

    move_files_anulation = GCSToGCSOperator(
        task_id='move_files_anulation',
        gcp_conn_id=GCS_CONN_ID,
        source_bucket=SOURCE_BUCKET,
        source_objects=['data/anulation/PLJ00032.TRXS.ANULADAS*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='conciliacion_backup/anulation/PLJ00032.TRXS.ANULADAS',
        move_object=True
        )

    move_files_incident = GCSToGCSOperator(
        task_id='move_files_incident',
        gcp_conn_id=GCS_CONN_ID,
        source_bucket=SOURCE_BUCKET,
        source_objects=['data/incident/PLJ62100-CONS-INC-PEND-TENPO*'],
        destination_bucket=TARGET_BUCKET,
        destination_object='conciliacion_backup/incident/PLJ62100-CONS-INC-PEND-TENPO',
        move_object=True
        )

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end')

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