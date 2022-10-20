from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


args = {
    'owner':'julio cristancho',
    'start_date' : datetime(2022, 10, 15, 11, 0)
}

def hello():
    for i in ['hello', 'world']:
        print(i)

with DAG(dag_id = 'test1', default_args= args, schedule_interval ='@daily') as dag:

    start = DummyOperator( task_id='start')

    python_test = PythonOperator(task_id = 'python_test', python_callable=hello)

    bash_test = BashOperator(task_id = 'bash_test', bash_command= 'echo prueba_bash')


start >> python_test >> bash_test

