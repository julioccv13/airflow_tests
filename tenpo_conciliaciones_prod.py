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
    "retry_delay"     : datetime.timedelta( minutes = 10 ),
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
DATAPROC_TEMPLATE_IPM = Variable.get(f'conciliacion_dataproc_template_ipm_{env}')
DATAPROC_TEMPLATE_OPD = Variable.get(f'conciliacion_dataproc_template_opd_{env}')
DATAPROC_TEMPLATE_ANULATION = Variable.get(f'conciliacion_dataproc_template_anulation_{env}')
DATAPROC_TEMPLATE_INCIDENT = Variable.get(f'conciliacion_dataproc_template_incident_{env}')
DATAPROC_TEMPLATE_CCA = Variable.get(f'conciliacion_dataproc_template_cca_{env}')
DATAPROC_TEMPLATE_PDC = Variable.get(f'conciliacion_dataproc_template_pdc_{env}')
CLUSTER = Variable.get(f"conciliacion_dataproc_cluster_{env}")
DATAPROC_FILES = Variable.get(f"conciliacion_dataproc_files_{env}")
INPUT_FILES = Variable.get(f"conciliacion_inputs_{env}")
OUTPUT_DATASET = Variable.get(f"conciliacion_dataset_{env}")
BACKUP_FOLDER = Variable.get(f"backup_folder_conciliacion_{env}")
IPM_PREFIX = Variable.get(f"ipm_prefix_{env}")
OPD_PREFIX = Variable.get(f"opd_prefix_{env}")
ANULATION_PREFIX = Variable.get(f"anulation_prefix_{env}")
INCIDENT_PREFIX = Variable.get(f"incident_prefix_{env}")
CCA_PREFIX = Variable.get(f"cca_prefix_{env}")
PDC_PREFIX = Variable.get(f"pdc_prefix_{env}")
REGION_OPD = Variable.get(f"region_opd_{env}")
REGION_IPM = Variable.get(f"region_ipm_{env}")
REGION_ANULATION = Variable.get(f"region_anulation_{env}")
REGION_INCIDENT = Variable.get(f"region_incident_{env}")
REGION_CCA = Variable.get(f"region_cca_{env}")
REGION_PDC = Variable.get(f"region_pdc_{env}")
ipm_type_file = Variable.get(f"type_file_ipm_{env}")
opd_type_file = Variable.get(f"type_file_opd_{env}")
anulation_type_file = Variable.get(f"type_file_anulation_{env}")
incident_type_file = Variable.get(f"type_file_incident_{env}")
cca_type_file = Variable.get(f"type_file_cca_{env}")
pdc_type_file = Variable.get(f"type_file_pdc_{env}")
ipm_workers = Variable.get(f"ipm_workers_{env}")
opd_workers = Variable.get(f"opd_workers_{env}")
anulation_workers = Variable.get(f"anulation_workers_{env}")
incident_workers = Variable.get(f"incident_workers_{env}")
cca_workers = Variable.get(f"cca_workers_{env}")
pdc_workers = Variable.get(f"pdc_workers_{env}")
opd_query = Variable.get(f"opd_gold_query_{env}")
ipm_query = Variable.get(f"ipm_gold_query_{env}")
anulation_query = Variable.get(f"anulation_gold_query_{env}")
incident_query = Variable.get(f"incident_gold_query_{env}")
cca_query = Variable.get(f"cca_gold_query_{env}")
pdc_query = Variable.get(f"pdc_gold_query_{env}")
match_query = Variable.get(f"match_query_{env}")
match_query_cca = Variable.get(f"cca_match_query_{env}")
match_query_pdc = Variable.get(f"pdc_match_query_{env}")

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
    "tenpo_conciliaciones_prd",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 

# GCS sensor for each file type    
    opd_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "opd_sensor",
        bucket=SOURCE_BUCKET,
        prefix=OPD_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )
    
    ipm_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "ipm_sensor",
        bucket=SOURCE_BUCKET,
        prefix=IPM_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )
    
    anulation_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "anulation_sensor",
        bucket=SOURCE_BUCKET,
        prefix=ANULATION_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )
    
    incident_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "incident_sensor",
        bucket=SOURCE_BUCKET,
        prefix=INCIDENT_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )
    
    cca_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "cca_sensor",
        bucket=SOURCE_BUCKET,
        prefix=CCA_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    pdc_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "pdc_sensor",
        bucket=SOURCE_BUCKET,
        prefix=PDC_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
# Action defined by file availability
    opd_file_availability = BranchPythonOperator(
        task_id='opd_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'opd_sensor',
            'dataproc_task' : 'dataproc_opd',
            'sql_task' : 'read_match'           
            },
        provide_context=True,
        trigger_rule='all_done'
        )

    ipm_file_availability = BranchPythonOperator(
        task_id='ipm_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'ipm_sensor',
            'dataproc_task' : 'dataproc_ipm',
            'sql_task' : 'read_match'           
            },
        provide_context=True, 
        trigger_rule='all_done'  
        )

    anulation_file_availability = BranchPythonOperator(
        task_id='anulation_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'anulation_sensor',
            'dataproc_task' : 'dataproc_anulation',
            'sql_task' : 'read_match'           
            },
        provide_context=True,   
        trigger_rule='all_done'
        )

    incident_file_availability = BranchPythonOperator(
        task_id='incident_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'incident_sensor',
            'dataproc_task' : 'dataproc_incident',
            'sql_task' : 'read_match'           
            },
        provide_context=True,   
        trigger_rule='all_done'
        )
    
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
    
    pdc_file_availability = BranchPythonOperator(
        task_id='pdc_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'pdc_sensor',
            'dataproc_task' : 'dataproc_pdc',
            'sql_task' : 'read_match'           
            },
        provide_context=True,   
        trigger_rule='all_done'
        )
    
# Instantiate a dataproc workflow template for each type of file to process 
    dataproc_ipm = DataprocInstantiateWorkflowTemplateOperator(
        task_id="dataproc_ipm",
        project_id=PROJECT_NAME,
        region=REGION_IPM,
        template_id=DATAPROC_TEMPLATE_IPM,
        parameters={
            'CLUSTER': f'{CLUSTER}-ipm-{DEPLOYMENT}',
            'NUMWORKERS': ipm_workers,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{IPM_PREFIX}*',
            'TYPE_FILE':ipm_type_file,
            'OUTPUT':f'{OUTPUT_DATASET}.ipm',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )

    dataproc_opd = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_opd',
        project_id=PROJECT_NAME,
        region=REGION_OPD,
        template_id=DATAPROC_TEMPLATE_OPD,    
        parameters={
            'CLUSTER': f'{CLUSTER}-opd-{DEPLOYMENT}',
            'NUMWORKERS':opd_workers,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{OPD_PREFIX}*',
            'TYPE_FILE':opd_type_file,
            'OUTPUT': OUTPUT_DATASET,
            'MODE_DEPLOY': DEPLOYMENT
        },
        )

    dataproc_anulation = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_anulation',
        project_id=PROJECT_NAME,
        region=REGION_ANULATION,
        template_id=DATAPROC_TEMPLATE_ANULATION, 
        parameters={
            'CLUSTER': f'{CLUSTER}-opd-anulation-{DEPLOYMENT}',
            'NUMWORKERS':anulation_workers,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT' : f'{INPUT_FILES}{ANULATION_PREFIX}*',
            'TYPE_FILE':anulation_type_file,
            'OUTPUT':f'{OUTPUT_DATASET}.opd_anulation',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )

    dataproc_incident = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_incident',
        project_id=PROJECT_NAME,
        region=REGION_INCIDENT,
        template_id=DATAPROC_TEMPLATE_INCIDENT,     
        parameters={
            'CLUSTER': f'{CLUSTER}-opd-incident-{DEPLOYMENT}',
            'NUMWORKERS':incident_workers,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{INCIDENT_PREFIX}*',
            'TYPE_FILE':incident_type_file,
            'OUTPUT':f'{OUTPUT_DATASET}.opd_incident',
            'MODE_DEPLOY': DEPLOYMENT
        },
        ) 
    
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
            'INPUT':f'{INPUT_FILES}{CCA_PREFIX}*',
            'TYPE_FILE':cca_type_file,
            'OUTPUT':f'{OUTPUT_DATASET}.cca',
            'MODE_DEPLOY': DEPLOYMENT
        },
        ) 
    
    dataproc_pdc = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_pdc',
        project_id=PROJECT_NAME,
        region=REGION_PDC,
        template_id=DATAPROC_TEMPLATE_PDC,     
        parameters={
            'CLUSTER': f'{CLUSTER}-pdc-{DEPLOYMENT}',
            'NUMWORKERS':pdc_workers,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{PDC_PREFIX}*',
            'TYPE_FILE':pdc_type_file,
            'OUTPUT':f'{OUTPUT_DATASET}.pdc',
            'MODE_DEPLOY': DEPLOYMENT
        },
        ) 

# Read a sql file for each type of file pass the tables from staging to gold    
    read_ipm_gold = PythonOperator(
        task_id='read_ipm_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
                op_kwargs={
        "query": ipm_query
        }
        )

    read_opd_gold = PythonOperator(
        task_id='read_opd_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": opd_query
        }
        )      
    
    read_anulation_gold = PythonOperator(
        task_id='read_anulation_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": anulation_query
        }
        )     
    
    read_incident_gold = PythonOperator(
        task_id='read_incident_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": incident_query
        }
        )     

    read_cca_gold = PythonOperator(
        task_id='read_cca_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": cca_query
        }
        )  
    
    read_pdc_gold = PythonOperator(
        task_id='read_pdc_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": pdc_query
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

    execute_cca_gold = PythonOperator(
        task_id='execute_cca_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_cca_gold') }}"
        }
        )
    
    execute_pdc_gold = PythonOperator(
        task_id='execute_pdc_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_pdc_gold') }}"
        }
        )
# Read the sql file with the Match query for conciliation from a GCS bucket 
    read_match_ipm = PythonOperator(
        task_id='read_match_ipm',
        provide_context=True,
        python_callable=read_gcs_sql,
        trigger_rule='none_failed',
        op_kwargs={
        "query": match_query
        }
        )
    
    read_match_cca = PythonOperator(
        task_id='read_match_cca',
        provide_context=True,
        python_callable=read_gcs_sql,
        trigger_rule='none_failed',
        op_kwargs={
        "query": match_query_cca
        }
        )
    
    read_match_pdc = PythonOperator(
        task_id='read_match_pdc',
        provide_context=True,
        python_callable=read_gcs_sql,
        trigger_rule='none_failed',
        op_kwargs={
        "query": match_query_pdc
        }
        )

# Execute the sql file with the Match query for conciliation     
    execute_match_ipm = PythonOperator(
        task_id='execute_match_ipm',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match_ipm') }}"
        }
        )
    
    execute_match_cca = PythonOperator(
        task_id='execute_match_cca',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match_cca') }}"
        }
        )
    
    execute_match_pdc = PythonOperator(
        task_id='execute_match_pdc',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match_pdc') }}"
        }
        )

# Move the files of the day from the source bucket to a backup bucket to leave it empty for the next day process        
    move_files_ipm = GCSToGCSOperator(
        task_id='move_files_ipm',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{IPM_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}ipm/{IPM_PREFIX}',
        move_object=True
        )

    move_files_opd = GCSToGCSOperator(
        task_id='move_files_opd',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{OPD_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}opdV2/{OPD_PREFIX}',
        move_object=True
        )

    move_files_anulation = GCSToGCSOperator(
        task_id='move_files_anulation',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{ANULATION_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}anulation/{ANULATION_PREFIX}',
        move_object=True
        )

    move_files_incident = GCSToGCSOperator(
        task_id='move_files_incident',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{INCIDENT_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}incident/{INCIDENT_PREFIX}',
        move_object=True
        ) 
    
    move_files_cca = GCSToGCSOperator(
        task_id='move_files_cca',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{CCA_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}cca/',
        move_object=True
        ) 
    
    move_files_pdc = GCSToGCSOperator(
        task_id='move_files_pdc',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{PDC_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}pdc/',
        move_object=True
        ) 

# Dummy tasks        
    start_task = DummyOperator( task_id = 'start')

    end_task = DummyOperator( task_id = 'end')

# Task dependencies


start_task >> opd_sensor >> opd_file_availability >> [dataproc_opd, read_match_ipm]
start_task >> ipm_sensor >> ipm_file_availability >> [dataproc_ipm, read_match_ipm]
start_task >> anulation_sensor >> anulation_file_availability >> [dataproc_anulation, read_match_ipm]
start_task >> incident_sensor >> incident_file_availability >> [dataproc_incident, read_match_ipm]
start_task >> cca_sensor >> cca_file_availability >> [dataproc_cca, read_match_cca]
start_task >> pdc_sensor >> pdc_file_availability >> [dataproc_pdc, read_match_pdc]

dataproc_opd >> read_opd_gold >> execute_opd_gold >> read_match_ipm 
dataproc_ipm >> read_ipm_gold >> execute_ipm_gold >> read_match_ipm 
dataproc_anulation >> read_anulation_gold >> execute_anulation_gold >> read_match_ipm
dataproc_incident >> read_incident_gold >> execute_incident_gold >> read_match_ipm
dataproc_cca >> read_cca_gold >> execute_cca_gold >> read_match_cca
dataproc_pdc >> read_pdc_gold >> execute_pdc_gold >> read_match_pdc

read_match_ipm >> execute_match_ipm >> [move_files_opd, move_files_ipm, move_files_anulation, move_files_incident] >> end_task
read_match_cca >> execute_match_cca >> move_files_cca >> end_task
read_match_pdc >> execute_match_pdc >> move_files_pdc >> end_task


