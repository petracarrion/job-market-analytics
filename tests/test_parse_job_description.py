import json

import pytest

from tasks.parse_job_description import parse_job_description


def load_file(file_path):
    with open(f'data/normalize_job_description/{file_path}', 'r') as f:
        content = f.read()
    return content


@pytest.mark.parametrize('test_case', ['test_case_7610188', 'test_case_7610222', 'test_case_7609275'])
def test_parse_job_description(test_case):
    input_content = load_file('input/' + test_case + '.html')

    result_content = parse_job_description(input_content)
    # temp = json.dumps(result_content, indent=2, ensure_ascii=False)

    output_content = json.loads(load_file('output/' + test_case + '.json'))
    assert result_content == output_content
