from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from conciliacion.functions import *
from conciliacion.config import *
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
    
    recargas_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id= "recargas_sensor",
        bucket=SOURCE_BUCKET,
        prefix=RECARGAS_PREFIX,
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
            'check_tables' : 'check_gold_tables_updates'           
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
            'check_tables' : 'check_gold_tables_updates'         
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
            'check_tables' : 'check_gold_tables_updates'          
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
            'check_tables' : 'check_gold_tables_updates'         
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
            'check_tables' : 'check_gold_tables_updates'          
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
            'check_tables' : 'check_gold_tables_updates'         
            },
        provide_context=True,   
        trigger_rule='all_done'
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
    
    dataproc_recargas = DataprocInstantiateWorkflowTemplateOperator(
        task_id='dataproc_recargas',
        project_id=PROJECT_NAME,
        region=REGION_RECARGAS,
        template_id=DATAPROC_TEMPLATE_RECARGAS,     
        parameters={
            'CLUSTER': f'{CLUSTER}-recargas-{DEPLOYMENT}',
            'NUMWORKERS':recargas_workers,
            'JOBFILE':f'{DATAPROC_FILES}{PYSPARK_FILE}',
            'FILES_OPERATORS':f'{DATAPROC_FILES}operators/*',
            'INPUT':f'{INPUT_FILES}{RECARGAS_PREFIX}*',
            'TYPE_FILE':recargas_type_file,
            'OUTPUT':f'{OUTPUT_DATASET}.recargas_app',
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
    
    read_recargas_gold = PythonOperator(
        task_id='read_recargas_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": recargas_query
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
    
    execute_recargas_gold = PythonOperator(
        task_id='execute_recargas_gold',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_recargas_gold') }}"
        }
        )
    
# Check if gold tables were updated
    check_gold_tables_updates = PythonOperator(
        task_id = 'check_gold_tables_updates',
        python_callable=check_gold_tables,
        provide_context = True,
        trigger_rule='none_failed'
    )
    
# Read the sql file with the Match query for conciliation from a GCS bucket 
    read_match_ipm = PythonOperator(
        task_id='read_match_ipm',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": match_query
        }
        )
    
    read_match_cca = PythonOperator(
        task_id='read_match_cca',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": match_query_cca
        }
        )
    
    read_match_pdc = PythonOperator(
        task_id='read_match_pdc',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": match_query_pdc
        }
        )

    read_match_recargas = PythonOperator(
        task_id='read_match_recargas',
        provide_context=True,
        python_callable=read_gcs_sql,
        op_kwargs={
        "query": match_query_recargas
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
    
    execute_match_recargas = PythonOperator(
        task_id='execute_match_recargas',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": "{{ task_instance.xcom_pull(task_ids='read_match_recargas') }}"
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
        destination_object=f'{BACKUP_FOLDER}cca',
        move_object=True
        ) 
    
    move_files_pdc = GCSToGCSOperator(
        task_id='move_files_pdc',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{PDC_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}pdc',
        move_object=True
        ) 
    
    move_files_recargas = GCSToGCSOperator(
        task_id='move_files_recargas',
        source_bucket=SOURCE_BUCKET,
        source_objects=[f'{RECARGAS_PREFIX}*'],
        destination_bucket=TARGET_BUCKET,
        destination_object=f'{BACKUP_FOLDER}recargas',
        move_object=True
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


