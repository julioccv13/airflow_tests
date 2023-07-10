from google.cloud import bigquery
from google.cloud import storage as st
from core.google import storage
from core.ports import Process
from io import BytesIO



class Process115(Process):
    def __init__(self, **kwargs):
        self._table_path = kwargs["table_path"]
        self._bucket_name = kwargs["bucket_name"]
        self._blob_name = kwargs["blob_name"]
        self._table_path = kwargs["table_path"]
        self._exec_date=kwargs["exec_date"]
        self._output_bucket_name=kwargs["output_bucket_name"]
        self._project_name=kwargs["project_name"]
    def run(self):
        """the run method will execute instanciate the driver and call the other methods """
        self.run_query_and_save_to_gcs()

    def run_query_and_save_to_gcs(self):
        buffer=BytesIO()
        client = bigquery.Client()
        client_gcs=st.Client()
        query=storage.get_blob_as_string(f"gs://{self._bucket_name}/115/queries/reporte-cmf-c78.sql")
        parsed_query=query.replace(r"${project_name}", "tenpo-datalake-sandbox")
        query_job = client.query(parsed_query)
        query_job.result()
        dataframe = query_job.to_dataframe()
        dataframe.to_parquet(buffer,compression='snappy')
        bucket = client_gcs.bucket(self._output_bucket_name)
        blob=bucket.blob(f"{self._blob_name}.txt")
        blob.upload_from_string(dataframe.to_csv(index=False,header=False),'text/plain')
        parquet_blob=bucket.blob(f"{self._blob_name}.parquet")
        parquet_blob.upload_from_string(buffer.getvalue())