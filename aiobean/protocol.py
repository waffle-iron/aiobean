import asyncio
from .log import logger


class BeanstalkProtocol(asyncio.Protocol):

    def __init__(self):
        self._server_address = None

    @property
    def server_address(self):
        return self._server_address

    def connection_made(self, transport):
        self.transport = transport
        self._server_address = transport.get_extra_info('peername')
        logger.debug('connected to server: %s', self.server_address)

    def connection_lost(self, exc):
        logger.debug('disconnected from server: %s', exc)
