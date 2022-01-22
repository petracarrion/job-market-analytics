from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator

dag = DAG('scrape_data_source_dag',
          description='Scrape Data Source DAG',
          schedule_interval='0 * * * *',
          start_date=datetime(2022, 1, 1),
          catchup=False)

task_check_vpn_status_op = SimpleHttpOperator(
    task_id='check_vpn_status_task',
    method='GET',
    endpoint='do/check-vpn-status',
    dag=dag,
)


def task_scrape_data_source_func(ts):
    http_hook = HttpHook()
    data = {
        'ts': ts,
    }
    http_hook.run('do/scrape-data-source', data)


task_scrape_data_source_op = PythonOperator(
    task_id='scrape_data_source_task',
    python_callable=task_scrape_data_source_func,
    dag=dag,
)

task_check_vpn_status_op >> task_scrape_data_source_op
