from common.chunking import get_chunk_size


def test_get_chunk_size():
    assert get_chunk_size(1000, 10, 500) == 100
    assert get_chunk_size(1000, 10, 50) == 50
    assert get_chunk_size(60, 4, 10) == 8
    assert get_chunk_size(100, 4, 10) == 9
