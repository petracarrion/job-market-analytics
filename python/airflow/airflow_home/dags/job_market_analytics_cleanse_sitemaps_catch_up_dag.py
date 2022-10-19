import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

os.environ["no_proxy"] = "*"

with DAG('job_market_analytics_cleanse_sitemaps_catch_up_dag',
         description='Job Market Analytics Cleanse Sitemaps Catch Up DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 1, 1),
         # end_date=datetime(2021, 12, 1),
         dagrun_timeout=timedelta(minutes=10),
         max_active_runs=4,
         max_active_tasks=4,
         catchup=True) as dag:
    @task(task_id="cleanse_sitemaps")
    def cleanse_sitemaps():
        run_flasky_task('do/cleanse_sitemaps')


    cleanse_sitemaps()
