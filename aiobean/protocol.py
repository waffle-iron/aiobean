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


class InvalidCommand(ProtocolException):
    pass


class CommandFailed(ProtocolException):
    pass


class UnexpectedResponse(ProtocolException):
    pass


def _parse_int(headers, body=None):
    return int(headers[0])


def _parse_body(headers, body=None):
    return body


def _parse_str(headers, body=None):
    return headers[0].decode()


def _parse_yml(headers, body=None):
    return load_yaml(body)


CRLF = '\r\n'
B_CRLF = b'\r\n'
PROTOCOL = {
    # command: (expected_ok, {expected_errors}, format)
    'put': (
        b'INSERTED',
        {b'BURIED', b'EXPECTED_CLRF', b'JOB_TOO_BIG', b'DRAINING'},
        _parse_int,
    ),
    'reserve': (
        b'RESERVED',
        {b'DEADLINE_SOON'},
        _parse_body,
    ),
    'peek': (
        b'FOUND',
        {b'NOT_FOUND'},
        _parse_body,
    )
}
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
        raise CommandFailed(status.decode())
    else:
        raise UnexpectedResponse(status.decode())
