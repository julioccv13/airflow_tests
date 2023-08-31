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
from airflow.providers.http.operators.http import SimpleHttpOperator
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

# Recargas App Parameters
REGION_RECARGAS = Variable.get(f"region_recargas_{env}")
DATAPROC_TEMPLATE_RECARGAS = Variable.get(f'conciliacion_dataproc_template_recargas_{env}')
RECARGAS_PREFIX = Variable.get(f"recargas_prefix_{env}")
RECARGAS_WORKERS = Variable.get(f"recargas_workers_{env}")
RECARGAS_TYPE_FILE = Variable.get(f"type_file_recargas_{env}")
RECARGAS_QUERY = Variable.get(f"recargas_gold_query_{env}")
MATCH_QUERY_RECARGAS = Variable.get(f"recargas_match_query_{env}")


# Reads sql files from GCS bucket
def read_gcs_sql(query):
    try:
        hook = GCSHook() 
        object_name = f'{SQL_FOLDER}/{query}'
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
        return kwargs['check_tables']
    except Exception as e:
        print(f"An error occurred: {e}")
        return kwargs['check_tables'] 
    
# Check status of gold tables processing
def check_gold_tables(**kwargs):
    try:
        ti = kwargs['ti']        
        ipm_status = ti.xcom_pull(task_ids='execute_ipm_gold', key='status')
        opd_status = ti.xcom_pull(task_ids='execute_opd_gold', key='status')
        anulation_status = ti.xcom_pull(task_ids='execute_anulation_gold', key='status')
        incident_status = ti.xcom_pull(task_ids='execute_incident_gold', key='status')
        cca_status = ti.xcom_pull(task_ids='execute_cca_gold', key='status')
        pdc_status = ti.xcom_pull(task_ids='execute_pdc_gold', key='status')
        recargas_status = ti.xcom_pull(task_ids='execute_cargas_gold', key='status')

        if all(status == "success" for status in [ipm_status
                                                  , opd_status
                                                  , anulation_status
                                                  , incident_status
                                                  , cca_status
                                                  , pdc_status
                                                  , recargas_status]):
            print("All inputs processed")

        skipped_tasks = [task_id for task_id, status in [('execute_opd_gold', opd_status)
                                                         , ('execute_ipm_gold', ipm_status)
                                                         , ('execute_anulation_gold', anulation_status)
                                                         , ('execute_incident_gold', incident_status)
                                                         , ('execute_cca_gold', cca_status)
                                                         , ('execute_pdc_gold', pdc_status)
                                                         , ('execute_recargas_gold', recargas_status)
                                                    ] if status == "skipped"]
        if skipped_tasks:
            print(f"Tasks {skipped_tasks} skipped")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    print("Checking gold tables")    
    

#DAG
with DAG(
    "tenpo_conciliaciones_recargas",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 
    
# Recargas input conciliation process

    recargas_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "recargas_sensor",
        bucket=SOURCE_BUCKET,
        prefix=RECARGAS_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )

    recargas_file_availability = BranchPythonOperator(
        task_id='recargas_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'recargas_sensor',
            'dataproc_task' : 'dataproc_recargas',
            'check_tables' : 'check_gold_tables_updates'         
            },
        provide_context=True,   
        trigger_rule='all_done'
        )
       
    dataproc_recargas = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_recargas',
        project_id=PROJECT_NAME,
        region=REGION_RECARGAS,
        template_id=DATAPROC_TEMPLATE_RECARGAS,     
        parameters={
            'CLUSTER': f'{CLUSTER}-recargas-{DEPLOYMENT}',
            'NUMWORKERS':RECARGAS_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{RECARGAS_PREFIX}*',
            'TYPE_FILE':RECARGAS_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.recargas_app',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )

    read_recargas_gold = PythonOperator(
        task_id='read_recargas_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": RECARGAS_QUERY
        }
        )  

    execute_recargas_gold = PythonOperator(
        task_id='execute_recargas_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_recargas_gold') }}"
        }
        )
        
    read_match_recargas = PythonOperator(
        task_id='read_match_recargas',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": MATCH_QUERY_RECARGAS
        }
        )

    execute_match_recargas = PythonOperator(
        task_id='execute_match_recargas',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match_recargas') }}"
        }
        )

    move_files_recargas = GCSToGCSOperator(
        task_id='move_files_recargas',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{RECARGAS_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}recargas/',
        move_object=True
        ) 

# Check for gold tables updates
    check_gold_tables_updates = PythonOperator(
        task_id = 'check_gold_tables_updates',
        python_callable=check_gold_tables,
        provide_context = True,
        trigger_rule='none_failed'
    )

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end', trigger_rule='all_done'  )

# Task dependencies


start_task >> recargas_sensor >> recargas_file_availability >>  [dataproc_recargas, check_gold_tables_updates]

dataproc_recargas >> read_recargas_gold >> execute_recargas_gold >> check_gold_tables_updates

check_gold_tables_updates >> read_match_recargas

read_match_recargas >> execute_match_recargas >> move_files_recargas >> end_task

