import datetime
import time

from airflow import models

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator




PROJECT_ID = 'tenpo-mark-vii'
CLUSTER_NAME = 'cluster-tenpo'
BUCKET_NAME = 'tenpo_test/data'
PYSPARK_FILE = 'test123.py'

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
    "dataproc_job_2",
    # Continue to run DAG once per day
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:


    PYTHON_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/{PYSPARK_FILE}"},
    }

    start_dataproc = BashOperator(
        task_id = 'dataproc_start' ,
        bash_command= 'gcloud dataproc clusters start cluster-tenpo --region=us-east1',

    )


    delay_dataproc_task: PythonOperator = PythonOperator(task_id="delay_python_task",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(210))


    dataproc_task = DataprocSubmitJobOperator(
        task_id='task_dataproc', job=PYTHON_JOB, region='us-east1', project_id=PROJECT_ID
    )

    stop_dataproc = BashOperator(
        task_id = 'dataproc_stop' ,
        bash_command= 'gcloud dataproc clusters stop cluster-tenpo --region=us-east1',

    )

start_dataproc >> delay_dataproc_task >> dataproc_task >> stop_dataproc 