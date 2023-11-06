from airflow.decorators import task
from plugins.conciliacion import *
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import bigquery
import logging

# Invoke cloud function
@task
def call_cloud_function(project_source, project_target, bucket_source, bucket_target, path_source, path_target, input_path_files):
    try:
        url = CF_URL
        request = google.auth.transport.requests.Request()  
        id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request)
        headers = {"Content-Type": "application/json"}
        body = {
                "project_source": project_source,
                "project_target":project_target,
                "bucket_source": bucket_source,
                "bucket_target": bucket_target,
                "path_source": path_source,
                "path_target": path_target,
                "input_path_files": input_path_files
        }
        resp = AuthorizedSession(id_token_credentials).post(url=url, json=body, headers=headers) 
        print(resp.status_code) # should return 200
        print(resp.content) # the body of the HTTP response
    except Exception as e:
        print(f"An error occurred: {e}")
        return False  
    
# Reads sql files from GCS bucket
@task
def read_gcs_sql(query):
    try:
        hook = GCSHook() 
        object_name = f'{SQL_FOLDER}/{query}'
        resp_byte = hook.download_as_byte_array(
            bucket_name=DATA_BUCKET,
            object_name=object_name,
        )
        resp_string = resp_byte.decode("utf-8")
        hook = BigQueryHook(gcp_conn_id=GoogleBaseHook.default_conn_name, delegate_to=None, use_legacy_sql=False)
        client = bigquery.Client(project=hook._get_field(PROJECT_NAME))
        consulta = client.query(resp_string) 
        if consulta.errors:
            raise Exception('Query with ERROR')
        else:
            print('Query executed successfully!')
    except Exception as e:
        logging.error(f"Error occurred while executing BigQuery query: {str(e)}")
        return False  
    
# Skip or run file process
@task
def task_pass(file):
    try:
        hook = BigQueryHook(gcp_conn_id=GoogleBaseHook.default_conn_name, delegate_to=None, use_legacy_sql=False)
        client = bigquery.Client(project=hook._get_field(PROJECT_NAME))
        query = f"""
        SELECT *
        FROM `tenpo-datalake-sandbox.tenpo_conciliacion_staging_dev.control_process`
        WHERE name_process == {file}
    """
        consulta = client.query(query) 
        if consulta == 'Y':
            return 'trigger_ipm'
        return 'end_task'
    except Exception as e:
        logging.error(f"Error occurred while executing BigQuery query: {str(e)}")
        return 'end_task'  
        
