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
        HttpHook(method='GET').run('do/check_vpn_status')


    @task(task_id="list_downloaded_job_descriptions_task")
    def list_downloaded_job_descriptions_task():
        context = get_current_context()
        data = {
            'ts': context['ts'],
        }
        HttpHook().run('do/list_downloaded_job_descriptions', data)


    @task(task_id="download_sitemap_task")
    def download_sitemap_task():
        context = get_current_context()
        data = {
            'ts': context['ts'],
        }
        HttpHook().run('do/download_sitemap', data)


    @task(task_id="list_job_descriptions_to_download_task")
    def list_job_descriptions_to_download_task():
        context = get_current_context()
        data = {
            'ts': context['ts'],
        }
        HttpHook().run('do/list_job_descriptions_to_download', data)


    @task(task_id="download_job_descriptions_task")
    def download_job_descriptions_task():
        context = get_current_context()
        data = {
            'ts': context['ts'],
        }
        HttpHook().run('do/download_job_descriptions', data)


    check_vpn_status_task() >> [
        list_downloaded_job_descriptions_task(),
        download_sitemap_task()
    ] >> list_job_descriptions_to_download_task() >> download_job_descriptions_task()
