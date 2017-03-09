import asyncio
from collections import deque
from aiobean.exc import BeanstalkException
from aiobean.log import logger
from aiobean.protocol import (
    CommandsMixin, encode_command, handle_head, handle_response
)


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
        self._close_waiter = loop.create_future()
        self._read_task = asyncio.ensure_future(
            self._read_loop(), loop=loop)
        self._read_task.add_done_callback(self._close_waiter.set_result)

        self._closing = False  # marks _do_close is running in a coro
        self._closed = False

        logger.info('connection open')

    @property
    def closed(self):
        closed = self._closed or self._closing
        if not closed and self._reader and self._reader.at_eof():
            self._closing = True
            self._loop.call_soon(self._do_close, None)
        return self._closed

    def close(self):
        return self._do_close()

    def _do_close(self, exc=None):
        if self._closed:
            return
        logger.info('closing connection..')
        self._writer.close()
        self._read_task.cancel()
        self._reader = None
        self._writer = None
        self._closing = True
        self._closed = True
        while self._queue:
            command, waiter = self._queue.popleft()
            logger.debug('cancelling waiter %r', (command, waiter))
            if exc is None:
                waiter.cancel()
            else:
                waiter.set_exception(exc)

    async def wait_closed(self):
        return await asyncio.shield(self._close_waiter, loop=self._loop)

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
        while not self._reader.at_eof():
            try:
                head = await self._reader.readline()
                logger.debug('read: head %s', head)
            except asyncio.CancelledError:
                break
            if self._reader.at_eof():
                break
            status, headers, body_len = handle_head(head)
            if body_len:
                try:
                    body = await self._reader.readexactly(body_len)
                    await self._reader.readexactly(2)  # crlf
                    logger.debug('read: body %s', body[:30])
                except asyncio.IncompleteReadError as e:
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
        self._closing = True
        self._do_close()
