from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook

from common_airflow_gag import run_flasky_task

with DAG('job_market_analytics_hourly_dag',
         description='Job Market Analytics Hourly DAG',
         schedule_interval='@hourly',
         start_date=datetime(2022, 1, 1),
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=1,
         max_active_tasks=1,
         catchup=False) as dag:
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
