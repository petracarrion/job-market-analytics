from loguru import logger


def get_logger():
    logger.add("../temp/job-market-analysis.log", rotation="500 MB")
    return logger
