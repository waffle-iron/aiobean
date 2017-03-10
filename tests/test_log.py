import pytest
from aiobean.log import _setup_logger


@pytest.mark.parametrize('debug,expect_handlers', [
    ['yes', True],
    ['', False]
])
def test_log(monkeypatch, debug, expect_handlers):
    monkeypatch.setenv('AIOBEAN_DEBUG', debug)
    logger = _setup_logger('test-logger-{}'.format(expect_handlers))
    assert bool(logger.handlers) == expect_handlers
