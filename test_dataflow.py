
import os
import pendulum
from datetime import datetime,timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow import models
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import (
	CheckJobRunning,
	DataflowCreateJavaJobOperator,
	DataflowCreatePythonJobOperator,
	DataflowTemplatedJobStartOperator,
	)
from airflow.utils.dates import days_ago
from airflow.utils import timezone

GCP_REGION = Variable.get('REGION')
GCP_MAIN_CLASS = Variable.get('MAIN_CLASS_JAVA')
GCS_JAR = Variable.get('ARTIFACT_PATH_JAVA')+Variable.get('JAR_FILE')

local_tz=pendulum.timezone('UTC')

default_args = {
	'owner': 'tenpo',
	#'email':'',
	#'email_on_failure':True,
	#'email_on_retry':True,
	'retries': 2,
	'retry_delay': timedelta(minutes=2)
}

with models.DAG(
	dag_id = "load_opd_files",
	#schedule_interval = '0 4 * * *',
	schedule_interval = None,
	start_date = datetime(2022,12,6,1, tzinfo=local_tz),
	catchup = False,
	tags = ['tenpo_load_opd_files'],
	) as dag_java:

	start_task = EmptyOperator( task_id = 'start')

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

	end_task = EmptyOperator( task_id = 'end')

start_task >> load_data >> end_task