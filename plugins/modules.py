from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subprocess_operator import SubprocessOperator
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