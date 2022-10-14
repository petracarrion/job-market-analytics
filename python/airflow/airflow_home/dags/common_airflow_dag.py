from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook


def run_flasky_task(endpoint):
    context = get_current_context()
    data = {
        'data_interval_end': context['data_interval_end'],
        'ds': context['ds'],
    }
    HttpHook().run(endpoint, data)
