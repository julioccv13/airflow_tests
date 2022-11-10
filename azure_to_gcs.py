#%% Imports
import datetime
from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (GCSToBigQueryOperator,)
from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import (AzureBlobStorageToGCSOperator,)

from airflow.utils.task_group import TaskGroup

PROJECT_NAME = "{{var.value.gcp_project}}"

#%% BigQuery configs
BQ_DESTINATION_DATASET_NAME = "jcristancho_test1"
BQ_DESTINATION_TABLE_NAME = "jcristanchotest"
BUCKET_NAME = "{{var.value.gcs_bucket}}"

#%% Azure configs
AZURE_BLOB_NAME = "{{var.value.azure_blob_name}}"
AZURE_BLOB_PATH = "{{var.value.azure_blob_path}}"
AZURE_CONTAINER_NAME = "{{var.value.azure_container_name}}"

# agregar paquete apache-airflow-providers-microsoft-azure a pypi en composer  
# crear conexion: ID azure_blob_connection  
# - tipo Azure Blob Storage 
# - Login cuenta de almacenamiento 
# - clave de acceso 
# - string de acceso 
# - token SAS
# crear variables 
# - azure_container_name: Es el nombre del contenedor 
# - azure_blob_path: Es la URL del BLOB 
# - azure_blob_name: Es el nombre del BLOB 
# - gcs_bucket: el nombre del bucket 
# - gcp_project: el ID del proyecto

#%% DAG configs
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

#%% DAG
with models.DAG(
    "azure_to_gcs_dag",
    # Continue to run DAG once per day
    schedule_interval=datetime.timedelta(days=10),
    default_args=default_dag_args,
) as dag:

    azure_blob_to_gcs = AzureBlobStorageToGCSOperator(
        task_id="azure_blob_to_gcs",
        # Azure args
        blob_name=AZURE_BLOB_NAME,
        file_path=AZURE_BLOB_PATH,
        container_name=AZURE_CONTAINER_NAME,
        wasb_conn_id="azure_blob_connection",
        filename=f"https://console.cloud.google.com/storage/browser/jcristanchotest/",
        # GCP args
        gcp_conn_id="google_cloud_default",
        object_name="OPD_Tenpo.csv",
        bucket_name=BUCKET_NAME,
        gzip=False,
        delegate_to=None,
        impersonation_chain=None,
    )
    
    
    load_external_dataset = GCSToBigQueryOperator(
        task_id="run_bq_external_ingestion",
        bucket="jcristanchotest",
        source_objects=["OPD_Tenpo.csv"],
        destination_project_dataset_table=f"jcristancho_test1.test",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

   
azure_blob_to_gcs >> load_external_dataset 