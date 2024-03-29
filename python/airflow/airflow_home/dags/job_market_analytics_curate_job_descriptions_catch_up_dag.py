import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

os.environ["no_proxy"] = "*"

with DAG('job_market_analytics_curate_job_descriptions_catch_up_dag',
         description='Job Market Analytics Curate Job Descriptions Catch Up DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 11, 1),
         end_date=datetime(2022, 11, 30),
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=4,
         max_active_tasks=4,
         catchup=True) as dag:
    @task(task_id="curate_job_descriptions")
    def curate_job_descriptions():
        run_flasky_task('do/curate_job_descriptions')


    curate_job_descriptions()
