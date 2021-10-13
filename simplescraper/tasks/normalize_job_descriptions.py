import json

from bs4 import BeautifulSoup

METADATA_JSON_PREFIX = 'window.__PRELOADED_STATE__.HeaderStepStoneBlock = '
METADATA_JSON_SUFFIX = ';'


def normalize_job_description(html_content):
    result = {}
    print(html_content)
    soup = BeautifulSoup(html_content)
    first_script_tag = soup.find('script', id='js-section-preloaded-HeaderStepStoneBlock')
    first_script_tag_lines = first_script_tag.text.split('\n')
    for line in first_script_tag_lines:
        if line.startswith(METADATA_JSON_PREFIX) and line.endswith(METADATA_JSON_SUFFIX):
            json_line = line[len(METADATA_JSON_PREFIX):len(line) - len(METADATA_JSON_SUFFIX)]
            result['listing_data'] = json.loads(json_line)['listingData']

    return result
