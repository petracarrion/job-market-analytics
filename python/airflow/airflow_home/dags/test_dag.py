from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

with DAG('test_dag2',
         description='Test DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 7, 29),
         catchup=False) as dag:
    @task(task_id="test_task")
    def run_test():
        run_flasky_task('do/test')


    run_test()
