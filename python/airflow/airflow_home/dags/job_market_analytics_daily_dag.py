import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common_airflow_dag import run_flasky_task

os.environ["no_proxy"] = "*"

with DAG('job_market_analytics_daily_dag',
         description='Job Market Analytics Daily DAG',
         schedule_interval='@daily',
         start_date=datetime(2022, 1, 1),
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=1,
         catchup=False) as dag:
    @task(task_id="cleanse_sitemaps")
    def cleanse_sitemaps():
        run_flasky_task('do/cleanse_sitemaps')


    @task(task_id="cleanse_job_descriptions")
    def cleanse_job_descriptions():
        run_flasky_task('do/cleanse_job_descriptions')


    @task(task_id="curate_sitemaps")
    def curate_sitemaps():
        run_flasky_task('do/curate_sitemaps')


    @task(task_id="curate_job_descriptions")
    def curate_job_descriptions():
        run_flasky_task('do/curate_job_descriptions')


    @task(task_id="do_dbt_run")
    def dbt_run():
        run_flasky_task('do/do_dbt_run')


    @task(task_id="do_day_backup")
    def backup_day():
        run_flasky_task('do/do_day_backup')


    @task(task_id="verify_day_backup")
    def verify_day_backup():
        run_flasky_task('do/verify_day_backup')


    @task(task_id="prune_old_raw")
    def prune_old_raw():
        run_flasky_task('do/prune_old_raw')


    t_curate_sitemaps = curate_sitemaps()
    t_curate_job_descriptions = curate_job_descriptions()

    cleanse_sitemaps() >> t_curate_sitemaps
    cleanse_job_descriptions() >> t_curate_job_descriptions

    [t_curate_sitemaps, t_curate_job_descriptions] >> dbt_run()

    backup_day() >> verify_day_backup() >> prune_old_raw()
