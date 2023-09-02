import logging
from logging import getLogger, StreamHandler


def parse_symbols(s):
    return [f'{x}USDT' for x in s.split(',') if len(s) > 0]


def create_logger(log_level, name="binancebq"):
    level = getattr(logging, log_level.upper())
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger = getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    err = StreamHandler()
    err.setLevel(level)
    err.setFormatter(formatter)
    logger.addHandler(err)

    return logger
