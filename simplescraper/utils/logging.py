from loguru import logger


def get_logger():
    logger.add('../temp/job-market-analysis.{time}.log')
    return logger
