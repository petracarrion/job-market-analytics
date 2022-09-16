from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

with DAG('job_market_analytics_load_to_dwh_dag',
         description='Job Market Analytics Load to DWH DAG',
         schedule_interval='@daily',
         start_date=datetime(2021, 10, 1),
         # end_date=datetime(2022, 6, 1),
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=1,
         max_active_tasks=1,
         catchup=True) as dag:
    @task(task_id="load_to_dwh")
    def load_to_dwh():
        run_flasky_task('do/load_to_dwh')


    load_to_dwh()
