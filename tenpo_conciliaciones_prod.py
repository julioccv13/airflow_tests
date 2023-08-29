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

# DAG general parameters
env = Variable.get('env')
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

# OPD Parameters
REGION_OPD = Variable.get(f"region_opd_{env}")
DATAPROC_TEMPLATE_OPD = Variable.get(f'conciliacion_dataproc_template_opd_{env}')
OPD_PREFIX = Variable.get(f"opd_prefix_{env}")
OPD_WORKERS = Variable.get(f"opd_workers_{env}")
OPD_TYPE_FILE = Variable.get(f"type_file_opd_{env}")
OPD_QUERY = Variable.get(f"opd_gold_query_{env}")

# IPM Parameters
REGION_IPM = Variable.get(f"region_ipm_{env}")
DATAPROC_TEMPLATE_IPM = Variable.get(f'conciliacion_dataproc_template_ipm_{env}')
IPM_PREFIX = Variable.get(f"ipm_prefix_{env}")
IPM_WORKERS = Variable.get(f"ipm_workers_{env}")
IPM_TYPE_FILE = Variable.get(f"type_file_ipm_{env}")
IPM_QUERY = Variable.get(f"ipm_gold_query_{env}")
MATCH_QUERY_IPM = Variable.get(f"match_query_{env}")

# Anulation Parameters
REGION_ANULATION = Variable.get(f"region_anulation_{env}")
DATAPROC_TEMPLATE_ANULATION = Variable.get(f'conciliacion_dataproc_template_anulation_{env}')
ANULATION_PREFIX = Variable.get(f"anulation_prefix_{env}")
ANULATION_WORKERS = Variable.get(f"anulation_workers_{env}")
ANULATION_TYPE_FILE = Variable.get(f"type_file_anulation_{env}")
ANULATION_QUERY = Variable.get(f"anulation_gold_query_{env}")

# Incident parameters
REGION_INCIDENT = Variable.get(f"region_incident_{env}")
DATAPROC_TEMPLATE_INCIDENT = Variable.get(f'conciliacion_dataproc_template_incident_{env}')
INCIDENT_PREFIX = Variable.get(f"incident_prefix_{env}")
INCIDENT_WORKERS = Variable.get(f"incident_workers_{env}")
INCIDENT_TYPE_FILE = Variable.get(f"type_file_incident_{env}")
INCIDENT_QUERY = Variable.get(f"incident_gold_query_{env}")

# CCA Parameters
REGION_CCA = Variable.get(f"region_cca_{env}")
DATAPROC_TEMPLATE_CCA = Variable.get(f'conciliacion_dataproc_template_cca_{env}')
CCA_PREFIX = Variable.get(f"cca_prefix_{env}")
CCA_WORKERS = Variable.get(f"cca_workers_{env}")
CCA_TYPE_FILE = Variable.get(f"type_file_cca_{env}")
CCA_QUERY = Variable.get(f"cca_gold_query_{env}")
MATCH_QUERY_CCA = Variable.get(f"cca_match_query_{env}")

# PDC Parameters
REGION_PDC = Variable.get(f"region_pdc_{env}")
DATAPROC_TEMPLATE_PDC = Variable.get(f'conciliacion_dataproc_template_pdc_{env}')
PDC_PREFIX = Variable.get(f"pdc_prefix_{env}")
PDC_WORKERS = Variable.get(f"pdc_workers_{env}")
PDC_TYPE_FILE = Variable.get(f"type_file_pdc_{env}")
PDC_QUERY = Variable.get(f"pdc_gold_query_{env}")
MATCH_QUERY_PDC = Variable.get(f"pdc_match_query_{env}")

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
    "tenpo_conciliaciones_prd",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 

# OPD Input conciliation process

    opd_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "opd_sensor",
        bucket=SOURCE_BUCKET,
        prefix=OPD_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )

    opd_file_availability = BranchPythonOperator(
        task_id='opd_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'opd_sensor',
            'dataproc_task' : 'dataproc_opd',
            'check_tables' : 'check_gold_tables_updates'           
            },
        provide_context=True,
        trigger_rule='all_done'
        )
    
    dataproc_opd = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_opd',
        project_id=PROJECT_NAME,
        region=REGION_OPD,
        template_id=DATAPROC_TEMPLATE_OPD,    
        parameters={
            'CLUSTER': f'{CLUSTER}-opd-{DEPLOYMENT}',
            'NUMWORKERS':OPD_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{OPD_PREFIX}*',
            'TYPE_FILE':OPD_TYPE_FILE,
            'OUTPUT': OUTPUT_DATASET,
            'MODE_DEPLOY': DEPLOYMENT
        },
        )
    
    read_opd_gold = PythonOperator(
        task_id='read_opd_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": OPD_QUERY
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
    
    move_files_opd = GCSToGCSOperator(
        task_id='move_files_opd',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{OPD_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}opdV2/{OPD_PREFIX}',
        move_object=True
        )

# IPM Input conciliation process 
    
    ipm_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "ipm_sensor",
        bucket=SOURCE_BUCKET,
        prefix=IPM_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )
    
    ipm_file_availability = BranchPythonOperator(
        task_id='ipm_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'ipm_sensor',
            'dataproc_task' : 'dataproc_ipm',
            'check_tables' : 'check_gold_tables_updates'         
            },
        provide_context=True, 
        trigger_rule='all_done'  
        )
    
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
    
    read_ipm_gold = PythonOperator(
        task_id='read_ipm_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
                op_kwargs={
        "query": IPM_QUERY
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
    
    read_match_ipm = PythonOperator(
        task_id='read_match_ipm',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": MATCH_QUERY_IPM
        }
        )
    
    execute_match_ipm = PythonOperator(
        task_id='execute_match_ipm',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match_ipm') }}"
        }
        )
    
    move_files_ipm = GCSToGCSOperator(
        task_id='move_files_ipm',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{IPM_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}ipm/{IPM_PREFIX}',
        move_object=True
        )

# Anulation input conciliation process

    anulation_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "anulation_sensor",
        bucket=SOURCE_BUCKET,
        prefix=ANULATION_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )
    
    anulation_file_availability = BranchPythonOperator(
        task_id='anulation_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'anulation_sensor',
            'dataproc_task' : 'dataproc_anulation',
            'check_tables' : 'check_gold_tables_updates'          
            },
        provide_context=True,   
        trigger_rule='all_done'
        )
    
    dataproc_anulation = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_anulation',
        project_id=PROJECT_NAME,
        region=REGION_ANULATION,
        template_id=DATAPROC_TEMPLATE_ANULATION, 
        parameters={
            'CLUSTER': f'{CLUSTER}-opd-anulation-{DEPLOYMENT}',
            'NUMWORKERS':ANULATION_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT' : f'{INPUT_FILES}{ANULATION_PREFIX}*',
            'TYPE_FILE':ANULATION_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.opd_anulation',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )
    
    read_anulation_gold = PythonOperator(
        task_id='read_anulation_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": ANULATION_QUERY
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
    
    move_files_anulation = GCSToGCSOperator(
        task_id='move_files_anulation',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{ANULATION_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}anulation/{ANULATION_PREFIX}',
        move_object=True
        )

# Incident input conciliation processs

    incident_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "incident_sensor",
        bucket=SOURCE_BUCKET,
        prefix=INCIDENT_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail = True
        )
    
    incident_file_availability = BranchPythonOperator(
        task_id='incident_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'incident_sensor',
            'dataproc_task' : 'dataproc_incident',
            'check_tables' : 'check_gold_tables_updates'         
            },
        provide_context=True,   
        trigger_rule='all_done'
        )

    dataproc_incident = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_incident',
        project_id=PROJECT_NAME,
        region=REGION_INCIDENT,
        template_id=DATAPROC_TEMPLATE_INCIDENT,     
        parameters={
            'CLUSTER': f'{CLUSTER}-opd-incident-{DEPLOYMENT}',
            'NUMWORKERS':INCIDENT_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{INCIDENT_PREFIX}*',
            'TYPE_FILE':INCIDENT_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.opd_incident',
            'MODE_DEPLOY': DEPLOYMENT
        },
        ) 
    
    read_incident_gold = PythonOperator(
        task_id='read_incident_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": INCIDENT_QUERY
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
    
    move_files_incident = GCSToGCSOperator(
        task_id='move_files_incident',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{INCIDENT_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}incident/{INCIDENT_PREFIX}',
        move_object=True
        ) 
    

# CCA Input conciliation process

    cca_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "cca_sensor",
        bucket=SOURCE_BUCKET,
        prefix=CCA_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )
    
    cca_file_availability = BranchPythonOperator(
        task_id='cca_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'cca_sensor',
            'dataproc_task' : 'dataproc_cca',
            'check_tables' : 'check_gold_tables_updates'          
            },
        provide_context=True,   
        trigger_rule='all_done'
        )

    dataproc_cca = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_cca',
        project_id=PROJECT_NAME,
        region=REGION_CCA,
        template_id=DATAPROC_TEMPLATE_CCA,     
        parameters={
            'CLUSTER': f'{CLUSTER}-cca-{DEPLOYMENT}',
            'NUMWORKERS':CCA_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{CCA_PREFIX}*',
            'TYPE_FILE':CCA_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.cca',
            'MODE_DEPLOY': DEPLOYMENT
        },
        )
    
    read_cca_gold = PythonOperator(
        task_id='read_cca_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": CCA_QUERY
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
    
    read_match_cca = PythonOperator(
        task_id='read_match_cca',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": MATCH_QUERY_CCA
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
    
    move_files_cca = GCSToGCSOperator(
        task_id='move_files_cca',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{CCA_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}cca',
        move_object=True
        ) 
    
# PDC Input conciliation process

    pdc_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "pdc_sensor",
        bucket=SOURCE_BUCKET,
        prefix=PDC_PREFIX,
        poke_interval=60*10 ,
        mode='reschedule',
        timeout=60*30,
        soft_fail=True,
        )

    pdc_file_availability = BranchPythonOperator(
        task_id='pdc_file_availability',
        python_callable=file_availability,
        op_kwargs={
            'sensor_task': 'pdc_sensor',
            'dataproc_task' : 'dataproc_pdc',
            'check_tables' : 'check_gold_tables_updates'         
            },
        provide_context=True,   
        trigger_rule='all_done'
        )
    
    dataproc_pdc = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_pdc',
        project_id=PROJECT_NAME,
        region=REGION_PDC,
        template_id=DATAPROC_TEMPLATE_PDC,     
        parameters={
            'CLUSTER': f'{CLUSTER}-pdc-{DEPLOYMENT}',
            'NUMWORKERS':PDC_WORKERS,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{PDC_PREFIX}*',
            'TYPE_FILE':PDC_TYPE_FILE,
            'OUTPUT':f'{OUTPUT_DATASET}.pdc',
            'MODE_DEPLOY': DEPLOYMENT
        },
        ) 

    read_pdc_gold = PythonOperator(
        task_id='read_pdc_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": PDC_QUERY
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
    
    read_match_pdc = PythonOperator(
        task_id='read_match_pdc',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": MATCH_QUERY_PDC
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
    
    move_files_pdc = GCSToGCSOperator(
        task_id='move_files_pdc',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{PDC_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}pdc',
        move_object=True
        ) 
    
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
            'INPUT':f'{INPUT_FILES}{RECARGAS_PREFIX}*',
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
        destination_object=f'{BACKUP_FOLDER}recargas',
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

start_task >> opd_sensor >> opd_file_availability >> [dataproc_opd, check_gold_tables_updates]
start_task >> ipm_sensor >> ipm_file_availability >> [dataproc_ipm, check_gold_tables_updates]
start_task >> anulation_sensor >> anulation_file_availability >> [dataproc_anulation, check_gold_tables_updates]
start_task >> incident_sensor >> incident_file_availability >> [dataproc_incident, check_gold_tables_updates]
start_task >> cca_sensor >> cca_file_availability >> [dataproc_cca, check_gold_tables_updates]
start_task >> pdc_sensor >> pdc_file_availability >> [dataproc_pdc, check_gold_tables_updates]
start_task >> recargas_sensor >> recargas_file_availability >>  [dataproc_recargas, check_gold_tables_updates]

dataproc_opd >> read_opd_gold >> execute_opd_gold >> check_gold_tables_updates 
dataproc_ipm >> read_ipm_gold >> execute_ipm_gold >> check_gold_tables_updates 
dataproc_anulation >> read_anulation_gold >> execute_anulation_gold >> check_gold_tables_updates
dataproc_incident >> read_incident_gold >> execute_incident_gold >> check_gold_tables_updates
dataproc_cca >> read_cca_gold >> execute_cca_gold >> check_gold_tables_updates
dataproc_pdc >> read_pdc_gold >> execute_pdc_gold >> check_gold_tables_updates
dataproc_recargas >> read_recargas_gold >> execute_recargas_gold >> check_gold_tables_updates

check_gold_tables_updates >> [read_match_ipm, read_match_cca, read_match_cca, read_match_pdc, read_match_recargas]

read_match_ipm >> execute_match_ipm >> [move_files_opd, move_files_ipm, move_files_anulation, move_files_incident] >> end_task
read_match_cca >> execute_match_cca >> move_files_cca >> end_task
read_match_pdc >> execute_match_pdc >> move_files_pdc >> end_task
read_match_recargas >> execute_match_recargas >> move_files_recargas >> end_task


