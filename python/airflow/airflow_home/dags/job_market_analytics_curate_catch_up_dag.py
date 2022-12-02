import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

os.environ["no_proxy"] = "*"

YEAR = 2021
MONTH = 10

with DAG('job_market_analytics_curate_catch_up_dag',
         description='Job Market Analytics Curate Catch Up DAG',
         schedule_interval='@daily',
         start_date=datetime(YEAR, MONTH, 1),
         end_date=datetime(YEAR, MONTH, 1) + timedelta(days=30),
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=2,
         max_active_tasks=2,
         catchup=True) as dag:
    @task(task_id="curate_sitemaps")
    def curate_sitemaps():
        run_flasky_task('do/curate_sitemaps')


    @task(task_id="curate_job_descriptions")
    def curate_job_descriptions():
        run_flasky_task('do/curate_job_descriptions')


    curate_sitemaps()
    curate_job_descriptions()
