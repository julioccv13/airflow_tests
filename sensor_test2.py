import os
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.dataflow import (
	CheckJobRunning,
	DataflowCreateJavaJobOperator,
)
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
    "email_on_failure": False,
    "email_on_retry": False,
}

local_tz=pendulum.timezone('UTC')
GCP_REGION = Variable.get('REGION')
GCP_MAIN_CLASS = Variable.get('MAIN_CLASS_JAVA')
GCS_JAR = Variable.get('ARTIFACT_PATH_JAVA')+Variable.get('JAR_FILE')
GCS_FILES = ['simple.sql', 'simple2.sql', 'create_op.sql']
PREFIX = 'sql'
PROJECT_NAME = 'tenpo-mark-vii'
BUCKET_NAME = 'tenpo_test'

with DAG(
    "tenpo_conciliacion_test",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args
) as dag: 

    def read_gcs_sql(**kwargs):
        hook = GCSHook()
        lista = []

        for gcs_file in GCS_FILES:

            if PREFIX:
                object_name = f'{PREFIX}/{gcs_file}'
                print("with prefix")

            else:
                object_name = f'{gcs_file}'
                print("without prefix")

            resp_byte = hook.download_as_byte_array(
                bucket_name = BUCKET_NAME,
                object_name = object_name,
            )

            resp_string = resp_byte.decode("utf-8")
            logging.info(resp_string)
            lista.append(resp_string)
        
        print("lista type: ", type(lista))
        lista = '#$%&'.join(lista)
        return lista    

    read_gcs = PythonOperator(
            task_id='read_gcs',
            provide_context=True,
            python_callable=read_gcs_sql,
            )

    sql_query = "{{ task_instance.xcom_pull(task_ids='read_gcs') }}" 

    def query_bq(sql):
        lista = sql.split('#$%&')

        print("lista type: ", type(lista))

        hook = BigQueryHook(gcp_conn_id= GoogleBaseHook.default_conn_name , delegate_to=None, use_legacy_sql=False)
        client = bigquery.Client(project=hook._get_field("tenpo-mark-vii"))
       
        for query in lista:
            print("query:")
            print(query)
            consulta = client.query(query) 
            if consulta.errors:
                raise Exception('Query con ERROR')
            else:
                print('Query perfect!')

    execute_query = PythonOperator(
        task_id='query_bq',
        provide_context=True,
        python_callable=query_bq,
        op_kwargs = {
        "sql": sql_query
        }
        )

    gcs_sensor = GCSObjectExistenceSensor(
        task_id= "gcs_sensor",
        bucket='tenpo_test',
        object='input_tenpo1.txt',
        poke_interval=60*10 ,
        mode='reschedule'
        )

    move_file = GCSToGCSOperator(
        task_id="move_file",
        source_bucket='tenpo_test',
        source_object='input_tenpo1.txt',
        destination_bucket='tenpo_test1',
        destination_object='backup_input_tenpo.txt',
        move_object=True,
        )

    load_data = DataflowCreateJavaJobOperator(
		task_id = "load_data",
		jar=GCS_JAR,
		job_name='{{task.task_id}}',
		options={
		'projectId': 'tenpo-mark-vii',
		'pathFile':'gs://tenpo-mark-vii/dataflow_test/*',
		'bigqueryDataset':'tenpo_conciliacion_staging_dev',
		'bigqueryTable':'_staging',
		'tempGCSBQBucket':'gs://tenpo-mark-vii/bigquery_temp_loads/',
		'minWorkers':5,
		'maxWorkers':15,
		},
		poll_sleep=10,
		job_class=GCP_MAIN_CLASS,
		check_if_running=CheckJobRunning.IgnoreJob,
		location=GCP_REGION
		)

gcs_sensor  >> load_data >> read_gcs >> execute_query >> move_file
