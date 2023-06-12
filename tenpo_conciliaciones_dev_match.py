from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor, GCSObjectUpdateSensor
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from google.cloud import bigquery

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
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

GCP_REGION = Variable.get('REGION')
PROJECT_NAME = 'tenpo-mark-vii'
SOURCE_BUCKET = 'tenpo_test'
TARGET_BUCKET = 'tenpo_test1'
OBJECT_NAME = 'input_tenpo1.txt'
PREFIX = 'sql'

## Alternative commands
opd_test = 'gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-opd-test,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/opd_files_test/Encrypted/PLJ61110.FINT0003*,TYPE_FILE=opd,OUTPUT=gs://tenpo-mark-vii/pyspark_demo/output/,MODE_DEPLOY=test'
ipm_test = 'gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-ipm-test,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/query_test/ipm_test/Encriptados/MCI.AR.T112.M.E0073610.D*,TYPE_FILE=ipm,OUTPUT=gs://tenpo-mark-vii/pyspark_demo/output/,MODE_DEPLOY=test'

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

def query_bq(sql):
    hook = BigQueryHook(gcp_conn_id= GoogleBaseHook.default_conn_name , delegate_to=None, use_legacy_sql=False)
    client = bigquery.Client(project=hook._get_field("tenpo-mark-vii"))
    consulta = client.query(sql) 
    if consulta.errors:
        raise Exception('Query con ERROR')
    else:
        print('Query perfect!')

with DAG(
    "tenpo_conciliaciones_dev_gold",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args
) as dag: 
    
    gcs_sensor = GCSObjectExistenceSensor(
        task_id= "gcs_sensor",
        bucket=SOURCE_BUCKET,
        object=OBJECT_NAME,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True
        )
    
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

    read_match = PythonOperator(
        task_id='read_match',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": "ipm_opd_gold_to_match.sql"
        }
        )
    
    execute_match = PythonOperator(
        task_id='execute_match',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match') }}"
        }
        )
         
    dataproc_ipm = BashOperator(
        task_id="start_dataproc_ipm",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-ipm-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/query_test/ipm_test/Encriptados/MCI.AR.T112.M.E0073610.D*,TYPE_FILE=ipm,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev.ipm,MODE_DEPLOY=prod",
        )

    dataproc_opd = BashOperator(
        task_id="start_dataproc_opd",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-opd-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/opd_files_test/Encrypted/PLJ61110.FINT0003*,TYPE_FILE=opd,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev,MODE_DEPLOY=prod",
        )
    
    dataproc_anuladas = BashOperator(
        task_id="start_dataproc_anuladas",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-opd-anulation-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/anulation_files/PLJ00032.TRXS.ANULADAS*,TYPE_FILE=anulation,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev.opd_anulation,MODE_DEPLOY=prod&",
        )
    
    dataproc_incidencias = BashOperator(
        task_id="start_dataproc_incidencias",
        bash_command="gcloud dataproc workflow-templates instantiate template_process_file --region=us-central1 --parameters=CLUSTER=tenpo-opd-incident-prod,NUMWORKERS=16,JOBFILE=gs://tenpo-mark-vii/artifacts/dataproc/pyspark_data_process.py,FILES_OPERATORS=gs://tenpo-mark-vii/artifacts/dataproc/operators/*,INPUT=gs://tenpo-mark-vii/test/anulation_files/PLJ62100-CONS-INC-PEND-TENPO*,TYPE_FILE=incident,OUTPUT=tenpo-mark-vii.tenpo_conciliacion_staging_dev.opd_incident,MODE_DEPLOY=prod&",
        )
        
        
    move_file = GCSToGCSOperator(
        task_id="move_file",
        source_bucket=SOURCE_BUCKET,
        source_object=OBJECT_NAME,
        destination_bucket=TARGET_BUCKET,
        destination_object='backup_input_tenpo.txt',
        move_object=True,
        )
   
    start_task = EmptyOperator( task_id = 'start')

    end_task = EmptyOperator( task_id = 'end')

start_task >> gcs_sensor >> [dataproc_ipm, dataproc_opd, dataproc_anuladas, dataproc_incidencias] 
dataproc_ipm >> read_ipm_gold >> execute_ipm_gold 
dataproc_opd >> read_opd_gold >> execute_opd_gold 
dataproc_incidencias >> read_incident_gold >> execute_incident_gold 
dataproc_anuladas >> read_anulation_gold >> execute_anulation_gold 
[execute_ipm_gold, execute_opd_gold, execute_incident_gold, execute_anulation_gold] >> read_match >> execute_match >> move_file >> end_task




