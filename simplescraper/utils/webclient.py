import os
from datetime import date

import requests

from utils.storage import save_raw_file

REQUEST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
              "application/signed-exchange;v=b3;q=0.9",
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


def historize_url_content(url, content):
    file_name = url.split('/')[-1]
    save_raw_file(content, 'sitemap', file_name)


def get_and_historize_url_content(url):
    response = requests.get(url)
    content = response.content
    historize_url_content(url, content)
    return content
