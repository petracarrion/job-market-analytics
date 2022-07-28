import os

from flask import Flask, request

from common.logging import logger, configure_logger
from common.storage import get_run_timestamp
from tasks.download_job_descriptions import download_job_descriptions
from tasks.download_sitemap import download_sitemap
from tasks.list_downloaded_job_descriptions import list_downloaded_job_descriptions
from tasks.list_job_descriptions_to_download import list_job_descriptions_to_download


def is_connected_to_vpn():
    return os.system('/usr/sbin/scutil --nc list | grep Connected | grep vpn') == 0


app = Flask(__name__)


@app.route('/')
def index():
    return '<a href="/do/check_vpn_status">Check VPN Status</a><br>' \
           '<a href="/do/list_donwloaded_job_descriptions">List Downloaded Descriptions</a><br>' \
           '<a href="/do/download_sitemap">Download Sitemap</a><br>' \
           '<a href="/do/list_job_descriptions_to_download">List Job Descriptions to Download</a><br>' \
           '<a href="/do/download_job_descriptions">Download Job Descriptions</a><br>' \
           '<a href="/do/test">Test</a><br>'


@app.route('/do/check_vpn_status')
def do_check_vpn_status():
    logger.info('is_connected_to_vpn: start')
    is_connected = is_connected_to_vpn()
    logger.info('is_connected_to_vpn: end')
    if is_connected:
        return {'result_status': 'success'}, 200
    else:
        return {'result_status': 'failed'}, 400


@app.route('/do/list_downloaded_job_descriptions', methods=['GET', 'POST'])
def do_list_downloaded_urls():
    if request.method == 'POST':
        data_interval_end = request.form.get('data_interval_end')
        run_timestamp = get_run_timestamp(data_interval_end)
        configure_logger(run_timestamp)
        list_downloaded_job_descriptions(run_timestamp)
        return {'result_status': 'success'}, 200
    elif request.method == 'GET':
        return '<form method="POST">' \
               '  <div><label>data_interval_end: <input type="text" name="data_interval_end"></label></div>' \
               '  <input type="submit" value="Submit">' \
               '</form>'


@app.route('/do/download_sitemap', methods=['GET', 'POST'])
def do_download_sitemap():
    if request.method == 'POST':
        if is_connected_to_vpn():
            data_interval_end = request.form.get('data_interval_end')
            run_timestamp = get_run_timestamp(data_interval_end)
            configure_logger(run_timestamp)
            download_sitemap(run_timestamp)
            return {'result_status': 'success'}, 200
        else:
            return {'result_status': 'failed'}, 400
    elif request.method == 'GET':
        return '<form method="POST">' \
               '  <div><label>data_interval_end: <input type="text" name="data_interval_end"></label></div>' \
               '  <input type="submit" value="Submit">' \
               '</form>'


@app.route('/do/list_job_descriptions_to_download', methods=['GET', 'POST'])
def do_list_job_descriptions_to_download():
    if request.method == 'POST':
        if is_connected_to_vpn():
            data_interval_end = request.form.get('data_interval_end')
            run_timestamp = get_run_timestamp(data_interval_end)
            configure_logger(run_timestamp)
            list_job_descriptions_to_download(run_timestamp)
            return {'result_status': 'success'}, 200
        else:
            return {'result_status': 'failed'}, 400
    elif request.method == 'GET':
        return '<form method="POST">' \
               '  <div><label>data_interval_end: <input type="text" name="data_interval_end"></label></div>' \
               '  <input type="submit" value="Submit">' \
               '</form>'


@app.route('/do/download_job_descriptions', methods=['GET', 'POST'])
def do_download_job_descriptions():
    if request.method == 'POST':
        if is_connected_to_vpn():
            data_interval_end = request.form.get('data_interval_end')
            run_timestamp = get_run_timestamp(data_interval_end)
            configure_logger(run_timestamp)
            download_job_descriptions(run_timestamp)
            return {'result_status': 'success'}, 200
        else:
            return {'result_status': 'failed'}, 400
    elif request.method == 'GET':
        return '<form method="POST">' \
               '  <div><label>data_interval_end: <input type="text" name="data_interval_end"></label></div>' \
               '  <input type="submit" value="Submit">' \
               '</form>'


@app.route('/do/test', methods=['GET', 'POST'])
def do_test():
    if request.method == 'POST':
        logger.info(request.form)
        return {
                   'result_status': 'success',
                   'run_timestamp': 'TODO',
               }, 200
    elif request.method == 'GET':
        return '<form method="POST">' \
               '  <div><label>data_interval_end: <input type="text" name="data_interval_end"></label></div>' \
               '  <input type="submit" value="Submit">' \
               '</form>'
