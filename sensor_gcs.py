from airflow.models import DAG
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator

from airflow.utils.dates import days_ago
import datetime

default_args = {
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( minutes= 10 ),
}

with DAG(
    "gcs_sensor_dag",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args
) as dag: 

    gcs_sensor = GoogleCloudStorageObjectSensor(
        task_id= "gcs_sensor",
        bucket='tenpo_test',
        object='input_tenpo1.txt',
        poke_interval=60*10 ,      
    )

    gcs_copy=GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="gcs_to_gcs",
        source_bucket='tenpo_test',
        source_object='input_tenpo.txt',
        destination_bucket='tenpo_test1',
        destination_object='backup_input_tenpo.txt'
   )
       
gcs_sensor  >> gcs_copy