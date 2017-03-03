import asyncio
from collections import deque, namedtuple


class BeanstalkException(Exception):
    pass


class CommandFailed(BeanstalkException):
    pass


class UnexpectedResponse(BeanstalkException):
    pass


Request = namedtuple('Request', 'command,response')


def get_int(headers, body):
    return int(headers[0])

PROTOCOL = {
    # command: (expected_ok, {expected_errors}, format)
    b'put': (
        b'INSERTED',
        {b'BURIED', b'EXPECTED_CLRF', b'JOB_TOO_BIG', b'DRAINING'},
        get_int
    ),
}


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

    def write(self, *lines):
        command = lines[0].split()[0]
        waiter = self._loop.create_future()
        self._writer.writelines(lines)
        self._queue.append((command, waiter))
        return waiter

    async def _read_loop(self):
        while not self._reader.at_eof():
            headline = await self._reader.readline()
            status, *headers = headline.split()
            if status in (b'OK',):
                body_len = int(headers[-1])
                body = await self._reader.readexactly(body_len)
                await self._reader.readexactly(2)  # crlf
            else:
                body = None
            command, waiter = self._queue.popleft()
            try:
                self._handle_resp(command, waiter, status, headers, body)
            except Exception as e:
                if not waiter.done():
                    waiter.set_exception(e)

    def _handle_resp(self, command, waiter, status, headers, body=None):
        expected_ok, expected_errors, format_resp = PROTOCOL[command]
        if status == expected_ok:
            waiter.set_result(format_resp(headers, body))
        elif status in expected_errors:
            waiter.set_exception(CommandFailed(status.decode()))
        else:
            waiter.set_exception(UnexpectedResponse(status.decode()))
