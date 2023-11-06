from airflow.models import DAG
from airflow.models import Variable
from plugins.slack import get_task_success_slack_alert_callback
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import datetime
import logging

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
        recargas_status = ti.xcom_pull(task_ids='execute_recargas_gold', key='status')
        remesas_status = ti.xcom_pull(task_ids='execute_remesas_gold', key='status')

        if all(status == "success" for status in [ipm_status
                                                  , opd_status
                                                  , anulation_status
                                                  , incident_status
                                                  , cca_status
                                                  , pdc_status
                                                  , recargas_status
                                                  , remesas_status]):
            print("All inputs processed")
        else:
            skipped_tasks = [task_id for task_id, status in [('execute_opd_gold', opd_status)
                                                            , ('execute_ipm_gold', ipm_status)
                                                            , ('execute_anulation_gold', anulation_status)
                                                            , ('execute_incident_gold', incident_status)
                                                            , ('execute_cca_gold', cca_status)
                                                            , ('execute_pdc_gold', pdc_status)
                                                            , ('execute_recargas_gold', recargas_status)
                                                            , ('execute_remesas_gold', remesas_status)
                                                        ] if status == "skipped"]
            print(f"Tasks {skipped_tasks} skipped")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Invoke cloud function
def invoke_cloud_function(**kwargs):
    try:
        url = CF_URL
        request = google.auth.transport.requests.Request()  
        id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request)
        headers = {"Content-Type": "application/json"}
        body = {
                "project_source": kwargs['project_source'],
                "project_target":kwargs['project_target'],
                "bucket_source": kwargs['bucket_source'],
                "bucket_target": kwargs['bucket_target'],
                "path_source": kwargs['path_source'],
                "path_target": kwargs['path_target'],
                "input_path_files": kwargs['input_path_files']
        }
        resp = AuthorizedSession(id_token_credentials).post(url=url, json=body, headers=headers) 
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='cloud_function_response', value=resp.content.decode('utf-8'))
        print(resp.status_code) # should return 200
        print(resp.content) # the body of the HTTP response
    except Exception as e:
        print(f"An error occurred: {e}")
        return False  
    
# Check CF response
def cf_response(**kwargs):
    try:
        ti = kwargs['ti']        
        opd_resp = ti.xcom_pull(task_ids='opd_cf', key='cloud_function_response')
        ipm_resp = ti.xcom_pull(task_ids='ipm_cf', key='cloud_function_response')
        anulation_resp = ti.xcom_pull(task_ids='anulation_cf', key='cloud_function_response')
        incident_resp = ti.xcom_pull(task_ids='incident_cf', key='cloud_function_response')
        cca_resp = ti.xcom_pull(task_ids='cca_cf', key='cloud_function_response')
        pdc_resp = ti.xcom_pull(task_ids='pdc_cf', key='cloud_function_response')
        pos_resp = ti.xcom_pull(task_ids='pos_cf', key='cloud_function_response')
        recargas_resp = ti.xcom_pull(task_ids='recargas_cf', key='cloud_function_response')
        remesas_resp = ti.xcom_pull(task_ids='remesas_cf', key='cloud_function_response')
        if any(resp == "Done!!" for resp in [opd_resp
                                                  , ipm_resp
                                                  , anulation_resp
                                                  , incident_resp
                                                  , cca_resp
                                                  , pdc_resp
                                                  , recargas_resp
                                                  , pos_resp
                                                  , remesas_resp]):
            return 'trigger_task'
        return 'end_task'
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'end_task'
    