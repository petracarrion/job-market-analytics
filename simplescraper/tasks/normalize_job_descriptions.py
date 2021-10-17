import json
import re

from bs4 import BeautifulSoup

METADATA_JSON_PREFIX = 'window.__PRELOADED_STATE__.HeaderStepStoneBlock = '
METADATA_JSON_SUFFIX = ';'


def flatten_metadata(job_description):
    flatten = job_description.copy()
    metadata = flatten.pop('metaData')
    flatten.update(metadata)
    return flatten


def keys_to_snake_case(job_description):
    snake_case_object = {}
    for old_key in job_description.keys():
        # https://stackoverflow.com/questions/60148175/convert-camelcase-to-snakecase
        new_key = re.sub(r'(?<!^)(?=[A-Z])', '_', old_key).lower()
        snake_case_object[new_key] = job_description[old_key]
    return snake_case_object


def normalize_job_description(html_content):
    job_description = {}
    soup = BeautifulSoup(html_content, features='lxml')
    script_tag = soup.find('script', id='js-section-preloaded-HeaderStepStoneBlock')
    script_tag_lines = script_tag.text.split('\n')
    for line in script_tag_lines:
        if line.startswith(METADATA_JSON_PREFIX) and line.endswith(METADATA_JSON_SUFFIX):
            json_line = line[len(METADATA_JSON_PREFIX):len(line) - len(METADATA_JSON_SUFFIX)]
            job_description = json.loads(json_line)['listingData']

    job_description = flatten_metadata(job_description)
    job_description = keys_to_snake_case(job_description)

    return job_description
