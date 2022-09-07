from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common_airflow_gag import run_flasky_task

with DAG('cleanse_job_descriptions_dag',
         description='Cleanse Job Descriptions DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 1, 1),
         catchup=False) as dag:
    @task(task_id="cleanse_job_descriptions")
    def cleanse_job_descriptions():
        run_flasky_task('do/cleanse_job_descriptions')


    cleanse_job_descriptions()
