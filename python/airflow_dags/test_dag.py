from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

with DAG('test_dag2',
         description='Test DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 7, 29),
         catchup=True) as dag:
    def run_flasky_task(endpoint):
        context = get_current_context()
        data = {
            'data_interval_end': context['data_interval_end'],
            'execution_date': context['execution_date'],
        }
        HttpHook().run(endpoint, data)


    @task(task_id="test_task")
    def run_test():
        run_flasky_task('do/test')


    run_test()
