import math


def get_chunk_size(total, slots, max_chunk_size):
    max_run_size = slots * max_chunk_size

    number_of_runs = total / max_run_size
    number_of_runs = int(math.ceil(number_of_runs))

    number_of_chunks = number_of_runs * slots

    chunk_size = total / number_of_chunks
    chunk_size = int(math.ceil(chunk_size))

    return chunk_size
