import json
import re

from bs4 import BeautifulSoup

SPACE_CHAR = ' '
NBSP_CHAR = u'\xa0'

METADATA_JSON_PREFIX = 'window.__PRELOADED_STATE__.HeaderStepStoneBlock = '
METADATA_JSON_SUFFIX = ';'

FIELD_SELECTORS = {
    'company_name': '.at-header-company-name',
    'description': '#job-description',
    'description_introduction': '.at-section-text-introduction',
    'description_responsabilities': '.at-section-text-description-content',
    'description_requirements': '.at-section-text-profile-content',
    'description_perks': '.at-section-text-weoffer-content',
}


def flatten_metadata(metadata):
    flatten = metadata.copy()
    temp_metadata = flatten.pop('metaData')
    flatten.update(temp_metadata)
    return flatten


def keys_to_snake_case(metadata):
    snake_case_object = {}
    for old_key in metadata.keys():
        # https://stackoverflow.com/questions/60148175/convert-camelcase-to-snakecase
        new_key = re.sub(r'(?<!^)(?=[A-Z])', '_', old_key).lower()
        snake_case_object[new_key] = metadata[old_key]
    return snake_case_object


def extract_metadata(soup):
    metadata = {}
    script_tag = soup.find('script', id='js-section-preloaded-HeaderStepStoneBlock')
    script_tag_lines = script_tag.text.split('\n')
    for line in script_tag_lines:
        if line.startswith(METADATA_JSON_PREFIX) and line.endswith(METADATA_JSON_SUFFIX):
            json_line = line[len(METADATA_JSON_PREFIX):len(line) - len(METADATA_JSON_SUFFIX)]
            metadata = json.loads(json_line)['listingData']
    metadata = flatten_metadata(metadata)
    metadata = keys_to_snake_case(metadata)
    return metadata


def parse_job_description(html_content):
    soup = BeautifulSoup(html_content, features='lxml')

    job_description = extract_metadata(soup)
    for field, selector in FIELD_SELECTORS.items():
        element = soup.select_one(selector)
        if element:
            job_description[field] = element.text.strip().replace(NBSP_CHAR, SPACE_CHAR)

    return job_description
