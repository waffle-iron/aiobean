import os
import sys
import logging


def _setup_logger(name):
    logger = logging.getLogger(name)
    if os.environ.get('AIOBEAN_DEBUG'):
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s][%(levelname)s][%(name)s] - %(message)s'
        ))
        logger.addHandler(handler)
        os.environ['AIOBEAN_DEBUG'] = ''
    return logger


logger = _setup_logger('aiobean')
