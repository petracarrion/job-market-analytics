from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

with DAG('scrape_data_source_dag',
         description='Scrape Data Source DAG',
         schedule_interval='0 * * * *',
         start_date=datetime(2022, 1, 1),
         catchup=False) as dag:
    @task(task_id="check_vpn_status_task")
    def check_vpn_status_task():
        HttpHook(method='GET').run('do/check-vpn-status')


    @task(task_id="scrape_data_source_task")
    def scrape_data_source_task():
        context = get_current_context()
        data = {
            'ts': context['ts'],
        }
        HttpHook().run('do/scrape-data-source', data)


    check_vpn_status_task() >> scrape_data_source_task()
