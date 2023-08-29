from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subprocess_operator import SubprocessOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import datetime
import logging
import json


# Upload Airflow variables
variables = {
    "ipm_gold_query": "ipm_staging_to_gold.sql",
    "opd_gold_query": "opd_2_staging_to_gold.sql",
    "anulation_gold_query": "opd_anulation_staging_to_gold.sql",
    "incident_gold_query": "opd_incident_staging_to_gold.sql",
    "match_query": "match.sql",
    "conciliacion_datalake_bucket_sandbox": "mark-vii-conciliacion",
    "conciliacion_dataproc_cluster_sandbox": "tenpo",
    "conciliacion_dataproc_files_sandbox": "gs://mark-vii-conciliacion/artifacts/dataproc/",
    "conciliacion_pyspark_sandbox": "pyspark_data_process1.py",    
    "opd_workers_sandbox":"4",
    "ipm_workers_sandbox":"4",
    "anulation_workers_sandbox":"0",
    "incident_workers_sandbox":"0",
    "cca_workers_sandbox":"0",
    "pdc_workers_sandbox":"0",
    "conciliacion_dataproc_template_ipm_sandbox": "template_process_file_usc_small",
    "conciliacion_dataproc_template_opd_sandbox": "template_process_file_usc_medium",
    "conciliacion_dataproc_template_anulation_sandbox": "template_process_file_usc_small",
    "conciliacion_dataproc_template_incident_sandbox": "template_process_file_usc_small",
    "conciliacion_dataproc_template_cca_sandbox": "template_process_file_usc_small",
    "conciliacion_dataproc_template_pdc_sandbox": "template_process_file_usc_small",
    "conciliacion_dataset_sandbox": "tenpo-datalake-sandbox.tenpo_conciliacion_staging_dev",
    "conciliacion_inputs_sandbox": "gs://mark-vii-conciliacion/",
    "conciliacion_deployment_sandbox": "prod",
    "datalake_sandbox": "tenpo-datalake-sandbox",
    "env": "sandbox",
    "region_opd_sandbox": "us-central1",
    "region_ipm_sandbox": "us-central1",
    "region_anulation_sandbox": "us-central1",
    "region_incident_sandbox": "us-central1",
    "region_cca_sandbox": "us-central1",
    "region_pdc_sandbox": "us-central1",
    "sql_folder_sandbox": "sql",
    "backup_folder_conciliacion_sandbox" : "conciliacion_backup/",
    "ipm_prefix_sandbox": "data/ipm/MCI.AR.T112.M.E0073610.D23072",
    "opd_prefix_sandbox": "data/opd/PLJ61110.FINT0003.V2.0730.D2023072",
    "anulation_prefix_sandbox": "data/anulation/PLJ00032.TRXS.ANULADAS.2023072",
    "incident_prefix_sandbox" : "data/incident/PLJ62100-CONS-INC-PEND-TENPO.2023072",
    "cca_prefix_sandbox" : "data/cca/EXAP730",
    "pdc_prefix_sandbox" : "data/PDC",
    "type_file_ipm_sandbox" : "ipm",
    "type_file_opd_sandbox" : "opd",
    "type_file_anulation_sandbox" : "anulation",
    "type_file_incident_sandbox" : "incident",
    "type_file_cca_sandbox" : "cca",
    "type_file_pdc_sandbox" : "pdc"
}

for variable_name, variable_value in variables.items():
    try:
        Variable.get(variable_name)  
    except KeyError:
        Variable.set(variable_name, variable_value)

# Retrieve to dag
env = Variable.get('env')
DEPLOYMENT = Variable.get(f"conciliacion_deployment_{env}")
PROJECT_NAME = Variable.get(f'datalake_{env}')
SOURCE_BUCKET = Variable.get(f'conciliacion_ops_bucket_{env}')
TARGET_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
DATA_BUCKET = Variable.get(f'conciliacion_datalake_bucket_{env}')
PREFIX = Variable.get(f'sql_folder_{env}')
PYSPARK_FILE = Variable.get(f'conciliacion_pyspark_{env}')
DATAPROC_TEMPLATE_IPM = Variable.get(f'conciliacion_dataproc_template_ipm_{env}')
DATAPROC_TEMPLATE_OPD = Variable.get(f'conciliacion_dataproc_template_opd_{env}')
DATAPROC_TEMPLATE_ANULATION = Variable.get(f'conciliacion_dataproc_template_anulation_{env}')
DATAPROC_TEMPLATE_INCIDENT = Variable.get(f'conciliacion_dataproc_template_incident_{env}')
DATAPROC_TEMPLATE_CCA = Variable.get(f'conciliacion_dataproc_template_cca_{env}')
DATAPROC_TEMPLATE_PDC = Variable.get(f'conciliacion_dataproc_template_pdc_{env}')
DATAPROC_TEMPLATE_RECARGAS = Variable.get(f'conciliacion_dataproc_template_recargas_{env}')
CLUSTER = Variable.get(f"conciliacion_dataproc_cluster_{env}")
DATAPROC_FILES = Variable.get(f"conciliacion_dataproc_files_{env}")
INPUT_FILES = Variable.get(f"conciliacion_inputs_{env}")
OUTPUT_DATASET = Variable.get(f"conciliacion_dataset_{env}")
BACKUP_FOLDER = Variable.get(f"backup_folder_conciliacion_{env}")
IPM_PREFIX = Variable.get(f"ipm_prefix_{env}")
OPD_PREFIX = Variable.get(f"opd_prefix_{env}")
ANULATION_PREFIX = Variable.get(f"anulation_prefix_{env}")
INCIDENT_PREFIX = Variable.get(f"incident_prefix_{env}")
CCA_PREFIX = Variable.get(f"cca_prefix_{env}")
PDC_PREFIX = Variable.get(f"pdc_prefix_{env}")
RECARGAS_PREFIX = Variable.get(f"recargas_prefix_{env}")
REGION_OPD = Variable.get(f"region_opd_{env}")
REGION_IPM = Variable.get(f"region_ipm_{env}")
REGION_ANULATION = Variable.get(f"region_anulation_{env}")
REGION_INCIDENT = Variable.get(f"region_incident_{env}")
REGION_CCA = Variable.get(f"region_cca_{env}")
REGION_PDC = Variable.get(f"region_pdc_{env}")
REGION_RECARGAS = Variable.get(f"region_recargas_{env}")
ipm_type_file = Variable.get(f"type_file_ipm_{env}")
opd_type_file = Variable.get(f"type_file_opd_{env}")
anulation_type_file = Variable.get(f"type_file_anulation_{env}")
incident_type_file = Variable.get(f"type_file_incident_{env}")
cca_type_file = Variable.get(f"type_file_cca_{env}")
pdc_type_file = Variable.get(f"type_file_pdc_{env}")
recargas_type_file = Variable.get(f"type_file_recargas_{env}")
ipm_workers = Variable.get(f"ipm_workers_{env}")
opd_workers = Variable.get(f"opd_workers_{env}")
anulation_workers = Variable.get(f"anulation_workers_{env}")
incident_workers = Variable.get(f"incident_workers_{env}")
cca_workers = Variable.get(f"cca_workers_{env}")
pdc_workers = Variable.get(f"pdc_workers_{env}")
recargas_workers = Variable.get(f"recargas_workers_{env}")
opd_query = Variable.get(f"opd_gold_query_{env}")
ipm_query = Variable.get(f"ipm_gold_query_{env}")
anulation_query = Variable.get(f"anulation_gold_query_{env}")
incident_query = Variable.get(f"incident_gold_query_{env}")
cca_query = Variable.get(f"cca_gold_query_{env}")
pdc_query = Variable.get(f"pdc_gold_query_{env}")
recargas_query = Variable.get(f"recargas_gold_query_{env}")
match_query = Variable.get(f"match_query_{env}")
match_query_cca = Variable.get(f"cca_match_query_{env}")
match_query_pdc = Variable.get(f"pdc_match_query_{env}")
match_query_recargas = Variable.get(f"recargas_match_query_{env}")