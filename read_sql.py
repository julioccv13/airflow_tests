from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import bigquery
from airflow.utils.dates import days_ago
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

PREFIX = 'sql'
DATA_BUCKET = 'mark-vii-conciliacion'

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


with DAG(
    "test",
    schedule_interval='0 8,16,20 * * *',
    default_args=default_args
) as dag: 
    
    read_ipm_gold = PythonOperator(
        task_id='read_ipm_gold',
        provide_context=True,
        python_callable=read_gcs_sql,
                op_kwargs={
        "query": "ipm_staging_to_gold.sql"
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
    
