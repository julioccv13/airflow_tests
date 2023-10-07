import requests
import json
import base64
import requests
import time
from google.cloud import storage
import uuid

AIRFLOW_DAG_TRIGGER_URL = 'http://airflow-web-6b9896d789-2dzb6/api/experimental/dags/tenpo_conciliaciones_test/dag_runs'
DAG_COMPLETION_FLAG_BASE = 'gs://mark-vii-conciliacion/flags/'

def trigger_airflow_dag(event, context):

    # Extract information from the GCS bucket event
    bucket = event['bucket']
    folder = event['name']
    message_data = base64.b64decode(event['data']).decode('utf-8')
    unique_flag = str(uuid.uuid4())
    response = requests.post(AIRFLOW_DAG_TRIGGER_URL, params={'flag': unique_flag, 
                                                            "region": "us-central1",
                                                            "dataproc_template": "template_process_file_usc_small",
                                                            "prefix" : "data/new_files/recargas/reporte_servicio_recargas",
                                                            "workers":"0",
                                                            "type_file" : "recargas",
                                                            "query": "recargas_staging_to_gold.sql"})

    if response.status_code == 200:
        while not check_dag_completion_flag(unique_flag):
            time.sleep(10)
        
        context.ack()
    else:
        print("Error triggering Airflow DAG")

def check_dag_completion_flag(unique_flag):
    flag_location = DAG_COMPLETION_FLAG_BASE + unique_flag
    client = storage.Client()
    bucket_name, file_name = flag_location.split('/', 3)[-2:]
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.exists()

