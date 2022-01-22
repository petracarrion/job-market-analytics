import os

from flask import Flask, request

from common.logging import logger
from scrape_data_source import scrape_data_source


def is_connected_to_vpn():
    return os.system('/usr/sbin/scutil --nc list | grep Connected | grep vpn') == 0


app = Flask(__name__)


@app.route('/')
def index():
    return '<a href="/do/scrape-data-source">Scrape Data Source</a><br>' \
           '<a href="/do/check-vpn-status">Check VPN Status</a><br>' \
           '<a href="/do/test">Test</a><br>'


@app.route('/do/scrape-data-source', methods=['GET', 'POST'])
def do_scrape_data_source():
    if request.method == 'POST':
        ts = request.form.get('ts')
        if is_connected_to_vpn():
            scrape_data_source(ts)
            return {'result_status': 'success'}, 200
        else:
            return {'result_status': 'failed'}, 400
    elif request.method == 'GET':
        return '<form method="POST">' \
               '  <div><label>ts: <input type="text" name="ts"></label></div>' \
               '  <input type="submit" value="Submit">' \
               '</form>'


@app.route('/do/check-vpn-status')
def do_check_vpn_status():
    if is_connected_to_vpn():
        return {'result_status': 'success'}, 200
    else:
        return {'result_status': 'failed'}, 400
