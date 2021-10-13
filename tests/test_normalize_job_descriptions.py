import glob
import json
import os

from tasks.normalize_job_descriptions import normalize_job_description


def load_file(file_path):
    with open('tests/data/normalize_job_description/' + file_path, 'r') as f:
        content = f.read()
    return content


def _test_normalize_job_descriptions(test_case):
    input_content = load_file('input/' + test_case + '.html')

    result_content = normalize_job_description(input_content)

    output_content = json.loads(load_file('output/' + test_case + '.json'))
    assert result_content == output_content


def test_normalize_job_descriptions():
    test_cases = [os.path.splitext(os.path.basename(f))[0] for f in glob.iglob('tests/data/normalize_job_description/input/*.html')]
    for test_case in test_cases:
        _test_normalize_job_descriptions(test_case)


