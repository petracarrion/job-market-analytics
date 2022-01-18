from flask import Flask

from scrape_data_source import scrape_data_source

app = Flask(__name__)


@app.route('/')
def index():
    return '<a href="/do/scrape-data-source">Scrape Data Source</a>'


@app.route('/do/scrape-data-source')
def do_scrape_data_source():
    scrape_data_source()
    return {'result_status': 'success'}, 200
