
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime 

args = {
    'owner':'Homer Simpson',
    'start_date': datetime(2022, 10, 15, 10, 0, 0)
}

def titulo():
    print("El uso de Airflow en la universidad de Springfield")

def intro():
    print("El otro día mi hija me dijo que Airflow no se utilizaba en la universidad de Springfield, y yo le dije: qué no Lisa? qué no?")

def last():
    for i in range(150):
        print("Púdrete Flanders")


with DAG ( dag_id='homer_dag',default_args = args, schedule_interval='@daily') as dag:


        start = DummyOperator(task_id = 'start')

        text1 = PythonOperator(task_id = 'text1', python_callable= titulo)

        text2 = PythonOperator(task_id = 'text2', python_callable= intro)

        text3 = PythonOperator(task_id = 'text3', python_callable= last)

start >> text1 >> text2 >> text3