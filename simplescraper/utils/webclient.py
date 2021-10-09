import os
from datetime import date

import requests

from utils.storage import save_raw_file

REQUEST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-language": "en-US,en;q=0.9,es;q=0.8,it-IT;q=0.7,it;q=0.6,de-DE;q=0.5,de;q=0.4",
    "cache-control": "max-age=0",
    "sec-ch-ua": "\"Chromium\";v=\"94\", \"Google Chrome\";v=\"94\", \";Not A Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"macOS\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1"
  }


def drop_url_prefix(url):
    for prefix in ['https://', 'http://']:
        if url.startswith(prefix):
            url = url[len(prefix):]
    return url


def historize_url_content(url, content):
    file_path = drop_url_prefix(url)
    save_raw_file(file_path, content)
    save_raw_file(file_path + '.' + str(date.today()), content)


def get_and_historize_url_content(url):
    response = requests.get(url)
    content = response.content
    historize_url_content(url, content)
    return content
