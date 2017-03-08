import asyncio
from collections import deque
from aiobean.exc import BeanstalkException
from aiobean.log import logger
from aiobean.protocol import (
    CommandsMixin, encode_command, handle_head, handle_response
)


class ConnectionLost(BeanstalkException):
    pass


class ConnectionClosed(BeanstalkException):
    pass


async def create_connection(host, port, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()
    reader, writer = await asyncio.open_connection(host, port, loop=loop)
    return Connection(reader, writer, loop)


class Connection(CommandsMixin):

    def __init__(self, reader, writer, loop):
        self._reader = reader
        self._writer = writer
        self._loop = loop
        self._queue = deque()
        self._read_task = asyncio.ensure_future(
            self._read_loop(), loop=loop)
        self._closed = False
        self._closing = False
        logger.info('connection open')

    @property
    def closed(self):
        return self._closing or self._closed

    def close(self):
        return self._close()

    def _close(self, exc=None):
        if not self.closed:
            self._closing = True
            return asyncio.ensure_future(self._do_close(exc), loop=self._loop)

    async def _do_close(self, exc):
        logger.info('closing connection..')
        self._writer.close()
        if not self._read_task.done():
            self._read_task.cancel()
        while self._queue:
            command, waiter = self._queue.popleft()
            logger.debug('cancelling waiter %r', (command, waiter))
            if exc is None:
                waiter.cancel()
            else:
                waiter.set_exception(exc)

        self._closing = False
        self._closed = True

    def execute(self, command, *args, body=None):
        if self.closed:
            raise ConnectionClosed(
                'cannot execute command because the connection is closed')
        waiter = self._loop.create_future()
        self._queue.append((command, waiter))
        for part in encode_command(command, *args, body=body):
            self._writer.write(part)
            logger.debug('scheduled to write %s', part[:30])
        return waiter

    async def _read_loop(self):
        exc = None
        while not self._reader.at_eof():
            # read response
            head = await self._reader.readline()
            logger.debug('read: head %s', head)
            if head == b'':
                logger.debug('connection closed by server')
                exc = ConnectionLost()
                break
            status, headers, body_len = handle_head(head)
            if body_len:
                try:
                    body = await self._reader.readexactly(body_len)
                    logger.debug('read: body %s', body[:30])
                    await self._reader.readexactly(2)  # crlf
                except asyncio.IncompleteReadError as e:
                    exc = ConnectionLost()
                    break
            else:
                body = None
            # handler response
            try:
                command, waiter = self._queue.popleft()
                result = handle_response(command, status, headers, body)
            except Exception as e:
                waiter.set_exception(e)
            else:
                waiter.set_result(result)
        self._close(exc)
