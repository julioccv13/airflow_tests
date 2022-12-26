from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from airflow.utils.dates import days_ago
import datetime

default_args = {
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( minutes= 10 ),
}

with DAG(
    "sensor_test1",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args
) as dag: 

    gcs_sensor = GCSObjectExistenceSensor(
        task_id= "gcs_sensor",
        bucket='tenpo_test',
        object='input_tenpo1.txt',
        poke_interval=60*10 ,      
    )

    move_file = GCSToGCSOperator(
        task_id="move_file",
        source_bucket='tenpo_test',
        source_object='input_tenpo.txt',
        destination_bucket='tenpo_test1',
        destination_object='backup_input_tenpo.txt',
        move_object=True,
    )

    start_dataproc = DataprocInstantiateWorkflowTemplateOperator(
        task_id="start_dataproc",
        template_id="sparkpi",
        project_id="tenpo-mark-vii",
        region="us-central1",
    )
       
gcs_sensor  >> start_dataproc >> move_file
