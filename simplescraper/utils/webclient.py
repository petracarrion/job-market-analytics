import os

import requests

USE_WEB_CACHE = "USE_WEB_CACHE"

HTTPS_PREFIX = "https://"

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


def save_if_not_cached(url):
    try:
        load_from_cache(url)
    except:
        response = requests.get(url, headers=REQUEST_HEADERS)
        content = response.content
        save_to_cache(url, content)


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


# result = get_web_content('https://www.stepstone.de/5/sitemaps/de/sitemapindex.xml')
# print(result)

urls = """https://www.stepstone.de/stellenangebote--Diplom-Jurist-Bachelor-of-Laws-Wirtschaftsjurist-m-w-d-Stuttgart-tekom-Deutschland-e-V--7577309-inline.html
https://www.stepstone.de/stellenangebote--Erzieherin-Heilerziehungspflegerin-Sozialpaedagogen-Heilpaedagogen-m-w-d-als-Region-und-Landeshauptstadt-Hannover-Landkreis-Hildesheim-Langenhagen-Laatzen-Burgdorf-Pro-School-Professionelle-Schulbegleitung--7577308-inline.html
https://www.stepstone.de/stellenangebote--Lagerhelfer-m-w-d-Duerrholz-Daufenbach-Gundlach-Automotive-Corporation--7577307-inline.html
https://www.stepstone.de/stellenangebote--Produktionshelfer-m-w-d-Duerrholz-Daufenbach-Gundlach-Automotive-Corporation--7577306-inline.html
https://www.stepstone.de/stellenangebote--Mitarbeiter-m-w-d-Einkauf-Muehlenbeck-Freyer-Siegel-Elektronik-GmbH-Co-KG--7577305-inline.html
https://www.stepstone.de/stellenangebote--IT-Systemadministrator-Netzwerkadministrator-m-w-d-Greifswald-HanseYachts-AG--7577304-inline.html
https://www.stepstone.de/stellenangebote--SPS-Programmierer-Automatisierungstechniker-BB3I-Ruesselsheim-Frankfurt-Mainz-VisionR-GmbH--7577303-inline.html
https://www.stepstone.de/stellenangebote--Sachbearbeiter-m-w-d-Auftragsabwicklung-Haar-bei-Muenchen-Softing-IT-Networks-GmbH--7577302-inline.html"""

urls = urls.split()

# for url in urls:
#     save_if_not_cached(url)