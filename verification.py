from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
from airflow.hooks.base_hook import BaseHook
from datetime import datetime

# Define your verification logic as Python functions
def verify_and_update_first_verification(**kwargs):
    ti = kwargs['ti']

    # Get the XCom value passed from the DataProc task in the main DAG
    xcom_value = ti.xcom_pull(task_ids='data_proc_task')  # Replace 'data_proc_task' with the actual task ID

    # Connect to BigQuery
    gcs_conn_id = Variable.get("gcs_conn_id")  # The connection ID to GCS
    bigquery_conn_id = Variable.get("bigquery_conn_id")  # The connection ID to BigQuery
    gcs_hook = BaseHook.get_hook(gcs_conn_id)
    bigquery_hook = BaseHook.get_hook(bigquery_conn_id)

    # Query BigQuery table to get the actual number of rows
    project_id = 'your-project-id'
    dataset_id = 'your-dataset-id'
    table_id = 'your-first-table-id'
    
    client = bigquery.Client(project=project_id, location='US', credentials=bigquery_hook.get_connection(bigquery_conn_id).password)
    
    sql = f'SELECT COUNT(*) as row_count FROM `{project_id}.{dataset_id}.{table_id}`'
    query_job = client.query(sql)
    result = query_job.result()
    actual_row_count = result[0]['row_count']

    # Compare xcom_value with the actual number of rows
    if xcom_value == actual_row_count:
        status = "successful"
    else:
        status = f"failed process at first verification, xcom_value={xcom_value}, actual_row_count={actual_row_count}"

    # Update the verification table status column based on the comparison
    verification_table_id = 'your-verification-table-id'
    update_verification_status(verification_table_id, status, bigquery_conn_id)

def update_verification_status(table_id, status, bigquery_conn_id):
    project_id = 'your-project-id'
    dataset_id = 'your-dataset-id'
    
    client = bigquery.Client(project=project_id, location='US', credentials=bigquery_hook.get_connection(bigquery_conn_id).password)
    
    sql = f'''
        UPDATE `{project_id}.{dataset_id}.{table_id}`
        SET status = '{status}'
    '''

    query_job = client.query(sql)
    query_job.result()

def verify_and_update_second_verification(**kwargs):
    ti = kwargs['ti']

    # Connect to BigQuery
    gcs_conn_id = Variable.get("gcs_conn_id")  # The connection ID to GCS
    bigquery_conn_id = Variable.get("bigquery_conn_id")  # The connection ID to BigQuery
    gcs_hook = BaseHook.get_hook(gcs_conn_id)
    bigquery_hook = BaseHook.get_hook(bigquery_conn_id)

    # Query BigQuery to get the row count of the output table (first table)
    project_id = 'your-project-id'
    dataset_id = 'your-dataset-id'
    first_table_id = 'your-first-table-id'
    second_table_id = 'your-second-table-id'
    
    client = bigquery.Client(project=project_id, location='US', credentials=bigquery_hook.get_connection(bigquery_conn_id).password)
    
    # Query the first table
    sql_first = f'SELECT COUNT(*) as first_row_count FROM `{project_id}.{dataset_id}.{first_table_id}`'
    query_job_first = client.query(sql_first)
    first_row_count = query_job_first.result()[0]['first_row_count']

    # Query the second table
    sql_second = f'SELECT COUNT(*) as second_row_count FROM `{project_id}.{dataset_id}.{second_table_id}`'
    query_job_second = client.query(sql_second)
    second_row_count = query_job_second.result()[0]['second_row_count']

    # Compare the row counts of the two tables
    if first_row_count == second_row_count:
        status = "successful"
    else:
        status = f"failed process at second verification, first_row_count={first_row_count}, second_row_count={second_row_count}"

    # Update the verification table status column based on the comparison
    verification_table_id = 'your-verification-table-id'
    update_verification_status(verification_table_id, status, bigquery_conn_id)

# Define a SubDAG
with DAG('verification_subdag', schedule_interval=None, start_date=days_ago(1)) as verification_subdag:
    # Define tasks for the verification logic
    first_verification_task = PythonOperator(
        task_id='first_verification_task',
        python_callable=verify_and_update_first_verification,
        provide_context=True,
    )

    second_verification_task = PythonOperator(
        task_id='second_verification_task',
        python_callable=verify_and_update_second_verification,
        provide_context=True,
    )

# This is the SubDAG that contains the verification tasks
verification_subdag
