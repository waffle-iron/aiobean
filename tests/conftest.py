import pytest
from subprocess import Popen
import time

from aiobean.log import logger


class Server:

    def __init__(self, port):
        self._port = port
        self._process = None

    @property
    def address(self):
        return '127.0.0.1', self._port

    @property
    def running(self):
        return self._process is not None

    def start(self):
        self._process = Popen(['beanstalkd', '-p', str(self._port)])
        logger.debug('%s started', self)

    def terminate(self):
        if self._process:
            self._process.terminate()
            self._process = None
            logger.debug('%s terminated', self)

    def kill(self):
        if self._process:
            self._process.kill()
            self._process = None
            logger.debug('%s killed', self)

    def __str__(self):
        return 'server:{}'.format(self._port)


@pytest.fixture
def server(unused_tcp_port):
    s = Server(unused_tcp_port)
    s.start()
    time.sleep(0.01)
    yield s
    s.terminate()
