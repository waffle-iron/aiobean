"""
structure of a response::

    RESPONSE = HEAD [BODY]
    HEAD = STATUS *HEADER CRLF
    BODY = 1*OCTET CRLF

For example::

    RESERVED 10 2\r\nab\r\n

"""
from aiobean.exc import BeanstalkException
from functools import partial
import typing
try:
    from yaml import load as load_yaml
except ImportError:
    PYYAML = False
    # without yaml, we just decode it

    def load_yaml(body, **kwargs):
        return body.decode()
else:  # try to use libyaml
    PYYAML = True
    try:
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader
load_yaml = partial(load_yaml, Loader=Loader)


class ProtocolException(BeanstalkException):
    pass


class BadRequest(ProtocolException):
    pass


class InvalidCommand(ProtocolException):
    pass


class CommandFailed(ProtocolException):
    pass


class DeadlineSoon(CommandFailed):
    pass


class UnexpectedResponse(ProtocolException):
    pass


def _parse_empty(headers, body=None): pass


def _parse_int(headers, body=None):
    return int(headers[0])


def _parse_body(headers, body=None):
    return (int(headers[0]), body)


def _parse_str(headers, body=None):
    return headers[0].decode()


def _parse_yml(headers, body=None):
    return load_yaml(body)


CRLF = '\r\n'
B_CRLF = b'\r\n'
_EMPTY = set()


# return types
_a_int = typing.Awaitable[int]
_a_str = typing.Awaitable[str]
_a_job = typing.Awaitable[typing.Tuple[int, bytes]]
_a_dict = typing.Awaitable[dict]
_a_list_str = typing.Awaitable[typing.List[str]]
_a_none = typing.Awaitable[None]


class CommandsMixin:
    DEFAULT_PRI = 2**32
    DEFAULT_TTR = 300

    def put(self, body, pri: int=DEFAULT_PRI, delay: int=0,
            ttr: int=DEFAULT_TTR) -> _a_int:
        return self.execute('put', pri, delay, ttr, len(body), body=body)

    def use(self, tube: str) -> _a_str:
        return self.execute('use', tube)

    def reserve(self, timeout: int=None) -> _a_job:
        if timeout is None:
            return self.execute('reserve')
        else:
            return self.execute('reserve-with-timeout', timeout)

    def delete(self, id: int) -> None:
        return self.execute('delete', id)

    def release(self, id: int, pri: int=DEFAULT_PRI, delay: int=0) -> _a_none:
        return self.execute('release', id, pri, delay)

    def bury(self, id: int, pri: int=DEFAULT_PRI) -> _a_none:
        return self.execute('bury', id, pri)

    def touch(self, id: int) -> _a_none:
        return self.execute('touch', id)

    def watch(self, tube: str) -> _a_int:
        return self.execute('watch', tube)

    def ignore(self, tube: str) -> _a_int:
        return self.execute('ignore', tube)

    def peek(self, id: int) -> _a_job:
        return self.execute('peek', id)

    def peek_ready(self) -> _a_job:
        return self.execute('peek-ready')

    def peek_delayed(self) -> _a_job:
        return self.execute('peek-delayed')

    def peek_buried(self) -> _a_job:
        return self.execute('peek-buried')

    def kick(self, count: int=1) -> _a_int:
        return self.execute('kick', count)

    def kick_job(self, id: int) -> _a_job:
        return self.execute('kick-job', id)

    def stats_job(self, id: int) -> _a_dict:
        return self.execute('stats-job', id)

    def stats_tube(self, tube: str='default') -> _a_dict:
        return self.execute('stats-tube', tube)

    def stats(self) -> _a_dict:
        return self.execute('stats')

    def tubes(self) -> _a_list_str:
        return self.execute('list-tubes')

    def used(self) -> _a_str:
        return self.execute('list-tube-used')

    def watched(self) -> _a_list_str:
        return self.execute('list-tubes-watched')

    def pause_tube(self, tube: str, delay: int) -> _a_none:
        return self.execute('pause-tube', tube, delay)


PROTOCOL = {
    # command: (request,  expected_ok, {expected_errors}, parser)
    'put': (
        b'INSERTED',
        {b'BURIED', b'EXPECTED_CLRF', b'JOB_TOO_BIG', b'DRAINING'},
        _parse_int,
    ),
    'use': (b'USING', _EMPTY, _parse_str),
    'reserve': (b'RESERVED', {b'DEADLINE_SOON'}, _parse_body),
    'reserve-with-timeout': (
        b'RESERVED', {b'DEADLINE_SOON', b'TIMED_OUT'}, _parse_body),
    'delete': (b'DELETED', {b'NOT_FOUND'}, _parse_empty),
    'release': (b'RELEASED', {b'BURIED', b'NOT_FOUND'}, _parse_empty),
    'bury': (b'BURIED', {b'NOT_FOUND'}, _parse_empty),
    'touch': (b'TOUCHED', {b'NOT_FOUND'}, _parse_empty),
    'watch': (b'WATCHING', _EMPTY, _parse_int),
    'ignore': (b'WATCHING', {b'NOT_IGNORED'}, _parse_int),
    'peek': (b'FOUND', {b'NOT_FOUND'}, _parse_body),
    'kick': (b'KICKED', _EMPTY, _parse_int),
    'kick-job': (b'KICKED', {b'NOT_FOUND'}, _parse_empty),
    'stats-job': (b'OK', {b'NOT_FOUND'}, _parse_yml),
    'stats-tube': (b'OK', {b'NOT_FOUND'}, _parse_yml),
    'stats': (b'OK', _EMPTY, _parse_yml),
    'list-tubes': (b'OK', _EMPTY, _parse_yml),
    'list-tube-used': (b'USING', _EMPTY, _parse_str),
    'list-tubes-watched': (b'OK', _EMPTY, _parse_yml),
    'pause-tube': (b'PAUSED', {b'NOT_FOUND'}, _parse_empty),
}
for command in ('peek-ready', 'peek-delayed', 'peek-buried'):
    PROTOCOL[command] = PROTOCOL['peek']

RESP_WITH_BODY = {b'RESERVED', b'FOUND', b'OK'}


def encode_command(command, *args, body=None):
    if command not in PROTOCOL:
        raise InvalidCommand
    if args:
        command += ' ' + ' '.join(str(arg) for arg in args)
    yield (command + CRLF).encode()
    if body:
        if not isinstance(body, (bytes, bytearray, memoryview)):
            raise TypeError('job body must be a byte-like object')
        yield body
        yield B_CRLF


def handle_head(line):
    """
    returns a tuple like (status, headers, body_length).
    if there's no body to read, body_length is 0.
    """
    status, *headers = line.split()
    if status in RESP_WITH_BODY:
        body_len = int(headers[-1])
    else:
        body_len = 0
    return (status, headers, body_len)


def handle_response(command, status, headers, body):
    expected_ok, expected_errors, parse = PROTOCOL[command]
    if status == expected_ok:
        return parse(headers, body)
    elif status in expected_errors:
        # special cases
        if command.startswith('peek') and status == b'NOT_FOUND':
            return None
        if command == 'reserve-with-timeout' and status == b'TIMED_OUT':
            return None
        if command.startswith('reserve') and status == b'DEADLINE_SOON':
            raise DeadlineSoon
        raise CommandFailed(status.decode())
    else:
        raise UnexpectedResponse(status.decode())
