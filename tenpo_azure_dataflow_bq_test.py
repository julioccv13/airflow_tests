# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START composer_dataanalyticstutorial_azure_dag]
import datetime

from airflow import models
from airflow.providers.google.cloud.operators import dataproc
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import (
    AzureBlobStorageToGCSOperator,
)
from airflow.utils.task_group import TaskGroup


from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators import python
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.operators.empty import EmptyOperator
from google.cloud import bigquery
import logging




GCS_FILES = ['simple.sql', 'simple2.sql']
PREFIX = 'sql' # populate this if you stored your sql script in a directory in the bucket


#PROJECT_NAME = "{{var.value.gcp_project}}"
PROJECT_NAME = '{{var.value.project_name_conciliacion}}'
#REGION = "{{var.value.gce_region}}"

# BigQuery configs
# BQ_DESTINATION_DATASET_NAME = "tenpo2"
# BQ_DESTINATION_TABLE_NAME = "holidays_weather_joined"
# BQ_NORMALIZED_TABLE_NAME = "holidays_weather_normalized"

# Dataproc configs
BUCKET_NAME = "{{var.value.gcs_bucket}}"

BUCKET_NAME2 = 'tenpo_test'
print("this is BUCKET_NAME: ", BUCKET_NAME)
print("this is BUCKET_NAME2: ", BUCKET_NAME2)


#PYSPARK_JAR = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
#PROCESSING_PYTHON_FILE = f"gs://{BUCKET_NAME}/data_analytics_process.py"

# Azure configs
#AZURE_BLOB_NAME = "{{var.value.azure_blob_name}}"
#AZURE_BLOB_PATH = "{{var.value.azure_blob_path}}"
AZURE_CONTAINER_NAME = "{{var.value.azure_container_name}}"

# BATCH_ID = "data-processing-{{ ts_nodash | lower}}"  # Dataproc serverless only allows lowercase characters
# BATCH_CONFIG = {
#     "pyspark_batch": {
#         "jar_file_uris": [PYSPARK_JAR],
#         "main_python_file_uri": PROCESSING_PYTHON_FILE,
#         "args": [
#             BUCKET_NAME,
#             f"{BQ_DESTINATION_DATASET_NAME}.{BQ_DESTINATION_TABLE_NAME}",
#             f"{BQ_DESTINATION_DATASET_NAME}.{BQ_NORMALIZED_TABLE_NAME}",
#         ],
#     },
#     "environment_config": {
#         "execution_config": {
#             "service_account": "{{var.value.dataproc_service_account}}"
#         }
#     },
# }

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    "start_date": yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    "email_on_failure": False,
    "email_on_retry": False,
}

with models.DAG(
    "azure_gcs_dataflow_bq_28",
    # Continue to run DAG once per day
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:


    def read_gcs_file(**kwargs):
        hook = GCSHook()
        lista = []

        for gcs_file in GCS_FILES:

            #check if PREFIX is available and initialize the gcs file to be copied
            if PREFIX:
                object_name = f'{PREFIX}/{gcs_file}'
                print("with prefix")

            else:
                object_name = f'{gcs_file}'
                print("without prefix")

            #perform gcs hook download
            resp_byte = hook.download_as_byte_array(
                bucket_name = BUCKET_NAME2,
                object_name = object_name,
            )

            resp_string = resp_byte.decode("utf-8")
            logging.info(resp_string)
            lista.append(resp_string)
        
        print("lista type: ", type(lista))
        lista = '#$%&'.join(lista)
        return lista    

    read_gcs_op = python.PythonOperator(
            task_id='read_gcs',
            provide_context=True,
            python_callable=read_gcs_file,
            )

    sql_query = "{{ task_instance.xcom_pull(task_ids='read_gcs') }}" # store returned value from read_gcs_op

    def query_bq(sql):
        lista = sql.split('#$%&')
        #lista = ['create table if not exists `tenpo-mark-vii.tenpo_test.new_table` (\r\n x INT64 ,\r\n y STRING \r\n)', 'error', 'create table if not exists `tenpo-mark-vii.tenpo_test.new_table_2` (\r\n x INT64 ,\r\n y STRING \r\n)']

        print("lista type: ", type(lista))

        hook = BigQueryHook(gcp_conn_id= GoogleBaseHook.default_conn_name , delegate_to=None, use_legacy_sql=False)
        client = bigquery.Client(project=hook._get_field("tenpo-mark-vii"), credentials=hook._get_credentials())
        

        for query in lista:
            print("query:")
            print(query)
            consulta = client.query(query) # If you are not doing DML, you assign this to a variable and return the value
            if consulta.errors:
                raise Exception('Query con ERROR')
            else:
                print('Query perfect!')

    execute_query = python.PythonOperator(
            task_id='query_bq',
            provide_context=True,
            python_callable=query_bq,
            op_kwargs = {
                "sql": sql_query
            }
            )



    #va a buscar los archivos a Azure Blob Storage
    azure_blob_to_gcs = AzureBlobStorageToGCSOperator(
        task_id="azure_blob_to_gcs",
        # Azure args
        #blob_name=AZURE_BLOB_NAME,
        blob_name = 'head_usa_names.csv',
        #file_path=AZURE_BLOB_PATH,
        file_path = f'https://tenpostorage.blob.core.windows.net/{AZURE_CONTAINER_NAME}/head_usa_names.csv',
        container_name=AZURE_CONTAINER_NAME,
        wasb_conn_id="azure_blob_connection",
        filename=f"https://console.cloud.google.com/storage/browser/{BUCKET_NAME}/azure/",
        # GCP args
        gcp_conn_id="google_cloud_default",
        object_name="azure/head_usa_names_salida.csv",
        bucket_name=BUCKET_NAME,
        gzip=False,
        delegate_to=None,
        impersonation_chain=None,
    )

    #Ejecuta un Job de Dataflow a partir de un archivo input y deja una tabla de éste en BQ
    start_python_job = BeamRunPythonPipelineOperator(
        task_id="start-python-job",
        py_file='gs://tenpo_test/dataflow_python_examples/data_transformation_5.py',
        py_options=[],
        pipeline_options={
            'input': 'gs://tenpo_test/data_files/head_usa_names.csv',
            'output': 'tenpo-mark-vii:tenpo_test.output_dataflow_composer',
        },
        py_requirements=['apache-beam[gcp]==2.24.0', 'google-cloud-storage'],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={'location': 'us-east1'},
)






azure_blob_to_gcs >> start_python_job >> read_gcs_op >> execute_query