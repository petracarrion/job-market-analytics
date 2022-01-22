from common.storage import get_run_timestamp


def test_get_run_timestamp():
    assert get_run_timestamp('2022-01-22T12:49:39.448434+00:00') == '2022/01/22/12-49-39'
