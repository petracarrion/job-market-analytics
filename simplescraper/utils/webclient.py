import os

import requests

USE_WEB_CACHE = "USE_WEB_CACHE"

HTTPS_PREFIX = "https://"


def get_web_content(url):
    if use_web_cache():
        return load_from_cache(url)
    else:
        response = requests.get(url)
        content = response.content
        save_to_cache(url, content)
        return content


def use_web_cache():
    return os.environ.get(USE_WEB_CACHE) == "True"


def load_from_cache(url):
    file_path = get_local_path(url)
    with open(file_path, mode='rb') as file:  # b is important -> binary
        content = file.read()
    return content


def save_to_cache(url, content):
    file_path = get_local_path(url)
    dirname = os.path.dirname(file_path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    with open(file_path, "wb") as f:
        f.write(content)


def get_local_path(url):
    if url.startswith(HTTPS_PREFIX):
        url = url[len(HTTPS_PREFIX):]
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, '../data/cache/', url)
    return file_path


result = get_web_content('https://www.stepstone.de/5/sitemaps/de/sitemapindex.xml')
print(result)