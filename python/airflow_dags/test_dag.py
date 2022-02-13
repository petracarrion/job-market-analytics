from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

with DAG('test_dag',
         description='Test DAG',
         schedule_interval='*/5 * * * *',
         start_date=datetime(2022, 1, 1),
         catchup=False) as dag:
    def run_flasky_task(endpoint):
        context = get_current_context()
        data = {
            'data_interval_end': context['data_interval_end'],
        }
        HttpHook().run(endpoint, data)


    @task(task_id="test_task")
    def run_test():
        run_flasky_task('do/test')


    run_test()
