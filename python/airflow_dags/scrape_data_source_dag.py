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
    def run_flasky_task(endpoint):
        context = get_current_context()
        data = {
            'data_interval_end': context['data_interval_end'],
        }
        HttpHook().run(endpoint, data)


    @task(task_id="check_vpn_status")
    def check_vpn_status():
        HttpHook(method='GET').run('do/check_vpn_status')


    @task(task_id="list_downloaded_job_descriptions")
    def list_downloaded_job_descriptions():
        run_flasky_task('do/list_downloaded_job_descriptions')


    @task(task_id="download_sitemap")
    def download_sitemap():
        run_flasky_task('do/download_sitemap')


    @task(task_id="list_job_descriptions_to_download")
    def list_job_descriptions_to_download():
        run_flasky_task('do/list_job_descriptions_to_download')


    @task(task_id="download_job_descriptions")
    def download_job_descriptions():
        run_flasky_task('do/download_job_descriptions')


    check_vpn_status() >> list_downloaded_job_descriptions() >> \
    download_sitemap() >> list_job_descriptions_to_download() >> download_job_descriptions()
