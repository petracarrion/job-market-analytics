from common.storage import get_load_timestamp


def test_get_load_timestamp():
    assert get_load_timestamp('2022-01-22T12:49:39.448434+00:00') == '2022/01/22/12-49-39'
