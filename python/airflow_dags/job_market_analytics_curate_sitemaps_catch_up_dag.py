from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

with DAG('job_market_analytics_curate_sitemaps_catch_up_dag',
         description='Job Market Analytics Curate Sitemaps Catch Up DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 1, 1),
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=4,
         max_active_tasks=4,
         catchup=True) as dag:
    @task(task_id="curate_sitemaps")
    def curate_sitemaps():
        run_flasky_task('do/curate_sitemaps')


    curate_sitemaps()
