from airflow.models import DAG
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator

from airflow.utils.dates import days_ago
import datetime

default_args = {
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( minutes= 10 ),
}

with DAG(
    "gcs_dataproc_dag",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args
) as dag: 

    gcs_sensor = GoogleCloudStorageObjectSensor(
        task_id= "gcs_sensor",
        bucket='tenpo_test',
        object='input_tenpo1.txt',
        poke_interval=60*10 ,      
    )

    start_dataproc = DataprocInstantiateWorkflowTemplateOperator(
        task_id="start_dataproc",
        template_id="sparkpi",
        project_id="tenpo-mark-vii",
        region="us-central1",
    )
       
gcs_sensor  >> start_dataproc