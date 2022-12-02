import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

os.environ["no_proxy"] = "*"

with DAG('job_market_analytics_cleanse_job_descriptions_catch_up_dag',
         description='Job Market Analytics Cleanse Job Descriptions Catch Up DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 11, 1),
         end_date=datetime(2022, 12, 1),
         dagrun_timeout=timedelta(minutes=10),
         max_active_runs=1,
         max_active_tasks=1,
         catchup=True) as dag:

    @task(task_id="cleanse_job_descriptions")
    def cleanse_job_descriptions():
        run_flasky_task('do/cleanse_job_descriptions')


    cleanse_job_descriptions()
