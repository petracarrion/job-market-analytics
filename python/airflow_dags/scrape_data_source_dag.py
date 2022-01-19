from datetime import datetime

from airflow import DAG
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
    data={},
    headers={},
    dag=dag,
)

task_scrape_data_source_op = SimpleHttpOperator(
    task_id='scrape_data_source_task',
    method='GET',
    endpoint='do/scrape-data-source',
    data={},
    headers={},
    dag=dag,
)

task_check_vpn_status_op >> task_scrape_data_source_op
