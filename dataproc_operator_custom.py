from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
import pandas as pd

class CustomDataprocInstantiateWorkflowTemplateOperator(BaseOperator):
    @apply_defaults
    def __init__(self, project_id, region, cluster_name, template_id, input_file, gcp_conn_id='google_cloud_default', delegate_to=None, *args, **kwargs):
        super(CustomDataprocInstantiateWorkflowTemplateOperator, self).__init__(task_id=kwargs.get('task_id'))
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.template_id = template_id
        self.input_file = input_file
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        # Instantiate the workflow template as in the original operator
        super(CustomDataprocInstantiateWorkflowTemplateOperator, self).execute(context)

        # After the job is completed, get the row count from the input file
        row_count = self.get_row_count(self.input_file)

        # Store the row count as an XCom value
        context['ti'].xcom_push(key='row_count', value=row_count)

    def get_row_count(self, input_file):
        try:
            # Use pandas to read the file and count the rows
            df = pd.read_csv(input_file)  # Replace with the appropriate file format if needed
            row_count = len(df)
            return row_count
        except Exception as e:
            self.log.error(f"Error while counting rows in file: {str(e)}")
            return None