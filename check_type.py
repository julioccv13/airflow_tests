import calendar
from datetime import datetime, date, timedelta

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 8, 21)

}


def check_if_last_day_of_month(execution_date):
    #  calendar.monthrange return a tuple (weekday of first day of the
    #  month, number
    #  of days in month)
    run_date = datetime.fromtimestamp(execution_date.timestamp())
    last_day_of_month = calendar.monthrange(run_date.year, run_date.month)[1]
    # check if date is 3 days behind the last day of the month
    if run_date == date(run_date.year, run_date.month, last_day_of_month) - timedelta(days=3):
        return True
    return False


with DAG(
    dag_id='short_example',
    schedule_interval="@once",
    default_args=default_args,
) as dag:
    first = ShortCircuitOperator(
        task_id='verify_date',
        python_callable=check_if_last_day_of_month
    )

    second = DummyOperator(task_id='task')

    first >> second