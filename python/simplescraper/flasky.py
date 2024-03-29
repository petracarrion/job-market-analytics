import os
import subprocess

from flask import Flask, request, Request

from common.env_variables import SOURCE_DIR
from common.logging import logger
from common.storage import get_load_timestamp, get_load_date
from tasks.cleanse_job_descriptions import cleanse_job_descriptions
from tasks.cleanse_sitemaps import cleanse_sitemaps
from tasks.curate_job_descriptions import curate_job_descriptions
from tasks.curate_sitemaps import curate_sitemaps
from tasks.download_job_descriptions import download_job_descriptions
from tasks.download_sitemap import download_sitemap
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.list_job_descriptions_to_download import list_job_descriptions_to_download
from tasks.prune_old_raw import prune_old_raw

SUCCESS_RETURN_CODE = 0

DEFAULT_DATA_INTERVAL_END = '2022-09-08T00:00:00+00:00'
DEFAULT_DS = '2022-09-07'

SUCCESS = {'result_status': 'success', }, 200

HTML_FORM = f'''
<form method="POST">
  <label>data_interval_end:<input type="text" name="data_interval_end" value="{DEFAULT_DATA_INTERVAL_END}"></label><br>
  <label>ds:               <input type="text" name="ds"                value="{DEFAULT_DS}"></label><br>
  <input type="submit" value="Submit">
</form>
'''


def is_connected_to_vpn():
    return os.system('/usr/sbin/scutil --nc list | grep Connected | grep vpn') == 0


class RequestParams:
    def __init__(self, _request: Request):
        form = _request.form
        self.load_timestamp = get_load_timestamp(form.get('data_interval_end'))
        self.load_date = get_load_date(form.get('ds'))
        logger.info(self.__dict__)


app = Flask(__name__)


@app.route('/')
def index():
    return '<a href="/do/check_vpn_status">Check VPN Status</a><br>' \
           '<a href="/do/list_donwloaded_job_descriptions">List Downloaded Descriptions</a><br>' \
           '<a href="/do/download_sitemap">Download Sitemap</a><br>' \
           '<a href="/do/list_job_descriptions_to_download">List Job Descriptions to Download</a><br>' \
           '<a href="/do/download_job_descriptions">Download Job Descriptions</a><br>' \
           '<a href="/do/cleanse_sitemaps">Cleanse Sitemap</a><br>' \
           '<a href="/do/cleanse_job_descriptions">Cleanse Job Descriptions</a><br>' \
           '<a href="/do/do_dbt_run">Do dbt run</a><br>' \
           '<a href="/do/do_day_backup">Do Day Backup</a><br>' \
           '<a href="/do/validate_day_backup">Validate Day Backup</a><br>' \
           '<a href="/do/test">Test</a><br>'


@app.route('/do/check_vpn_status')
def do_check_vpn_status():
    logger.info('is_connected_to_vpn: start')
    is_connected = is_connected_to_vpn()
    logger.info('is_connected_to_vpn: end')
    if is_connected:
        return SUCCESS
    else:
        return {'result_status': 'failed'}, 400


@app.route('/do/list_downloaded_job_descriptions', methods=['GET', 'POST'])
def do_list_downloaded_urls():
    if request.method == 'POST':
        params = RequestParams(request)
        list_downloaded_job_descriptions(params.load_timestamp)
        return SUCCESS
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/download_sitemap', methods=['GET', 'POST'])
def do_download_sitemap():
    if request.method == 'POST':
        if is_connected_to_vpn():
            params = RequestParams(request)
            download_sitemap(params.load_timestamp)
            return {'result_status': 'success'}, 200
        else:
            return {'result_status': 'failed'}, 400
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/list_job_descriptions_to_download', methods=['GET', 'POST'])
def do_list_job_descriptions_to_download():
    if request.method == 'POST':
        if is_connected_to_vpn():
            params = RequestParams(request)
            list_job_descriptions_to_download(params.load_timestamp)
            return SUCCESS
        else:
            return {'result_status': 'failed'}, 400
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/download_job_descriptions', methods=['GET', 'POST'])
def do_download_job_descriptions():
    if request.method == 'POST':
        if is_connected_to_vpn():
            params = RequestParams(request)
            download_job_descriptions(params.load_timestamp)
            return SUCCESS
        else:
            return {'result_status': 'failed'}, 400
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/cleanse_sitemaps', methods=['GET', 'POST'])
def do_cleanse_sitemaps():
    if request.method == 'POST':
        params = RequestParams(request)
        cleanse_sitemaps(params.load_timestamp, params.load_date)
        return SUCCESS
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/cleanse_job_descriptions', methods=['GET', 'POST'])
def do_cleanse_job_descriptions():
    if request.method == 'POST':
        params = RequestParams(request)
        cleanse_job_descriptions(params.load_timestamp, params.load_date)
        return SUCCESS
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/curate_sitemaps', methods=['GET', 'POST'])
def do_curate_sitemaps():
    if request.method == 'POST':
        params = RequestParams(request)
        curate_sitemaps(params.load_timestamp, params.load_date)
        return SUCCESS
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/curate_job_descriptions', methods=['GET', 'POST'])
def do_curate_job_descriptions():
    if request.method == 'POST':
        params = RequestParams(request)
        curate_job_descriptions(params.load_timestamp, params.load_date)
        return SUCCESS
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/do_day_backup', methods=['GET', 'POST'])
def do_do_day_backup():
    if request.method == 'POST':
        params = RequestParams(request)
        year, month, day = params.load_date.split('/')
        result = subprocess.run([f'{SOURCE_DIR}/simplescraper/do_day_backup.sh', year, month, day])
        if result.returncode == SUCCESS_RETURN_CODE:
            return SUCCESS
        else:
            return {
                       'result_status': 'error',
                   }, 400
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/do_dbt_run', methods=['GET', 'POST'])
def do_dbt_run():
    if request.method == 'POST':
        _ = RequestParams(request)
        result = subprocess.run([f'{SOURCE_DIR}/simplescraper/do_dbt_run.sh'])
        if result.returncode == SUCCESS_RETURN_CODE:
            return SUCCESS
        else:
            return {
                       'result_status': 'error',
                   }, 400
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/verify_day_backup', methods=['GET', 'POST'])
def do_verify_day_backup():
    if request.method == 'POST':
        params = RequestParams(request)
        year, month, day = params.load_date.split('/')
        result = subprocess.run([f'{SOURCE_DIR}/simplescraper/verify_day_backup.sh', year, month, day])
        if result.returncode == SUCCESS_RETURN_CODE:
            return SUCCESS
        else:
            return {
                       'result_status': 'error',
                   }, 400
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/prune_old_raw', methods=['GET', 'POST'])
def do_prune_old_raw():
    if request.method == 'POST':
        params = RequestParams(request)
        prune_old_raw(params.load_timestamp, params.load_date)
        return SUCCESS
    elif request.method == 'GET':
        return HTML_FORM


@app.route('/do/test', methods=['GET', 'POST'])
def do_test():
    if request.method == 'POST':
        params = RequestParams(request)
        return {
                   'result_status': 'success',
                   'load_timestamp': params.load_timestamp,
                   'load_date': params.load_date,
               }, 200
    elif request.method == 'GET':
        return HTML_FORM
