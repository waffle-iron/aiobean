import asyncio
from collections import deque
from aiobean.protocol import encode_command, handle_head, handle_response


async def create_connection(host, port, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()
    reader, writer = await asyncio.open_connection(host, port, loop=loop)
    return Connection(reader, writer, loop)


class Connection:

    def __init__(self, reader, writer, loop):
        self._reader = reader
        self._writer = writer
        self._loop = loop
        self._queue = deque()
        self._read_task = asyncio.ensure_future(
            self._read_loop(), loop=loop)

    def _execute(self, command, *args, body=None):
        waiter = self._loop.create_future()
        self._queue.append((command, waiter))
        for part in encode_command(command, *args, body=body):
            self._writer.write(part)
        return waiter

    async def _read_loop(self):
        while not self._reader.at_eof():
            head = await self._reader.readline()
            if not head:
                break  # TODO: connection lost
            status, headers, body_len = handle_head(head)
            if body_len:
                body = await self._reader.readexactly(body_len)
                await self._reader.readexactly(2)  # crlf
            else:
                body = None
            command, waiter = self._queue.popleft()
            try:
                result = handle_response(command, status, headers, body)
            except Exception as e:
                if not waiter.done():
                    waiter.set_exception(e)
                # TODO: waiter cancelled or done already
            else:
                waiter.set_result(result)
