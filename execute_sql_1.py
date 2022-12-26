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
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


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
PROJECT_NAME = 'tenpo-mark-vii'


BUCKET_NAME2 = 'tenpo_test'

print("this is BUCKET_NAME2: ", BUCKET_NAME2)




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
    "execute_sql_1",
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
        client = bigquery.Client(project=hook._get_field("tenpo-mark-vii"))
        

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









read_gcs_op >> execute_query