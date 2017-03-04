"""
structure of a response::

    RESPONSE = HEAD [BODY]
    HEAD = STATUS *HEADER CRLF
    BODY = 1*OCTET CRLF

For example::

    RESERVED 10 2\r\nab\r\n

"""
from .exc import BeanstalkException


class ProtocolException(BeanstalkException):
    pass


class InvalidCommand(ProtocolException):
    pass


class CommandFailed(ProtocolException):
    pass


class UnexpectedResponse(ProtocolException):
    pass


def parse_int(headers, body):
    return int(headers[0])


def parse_job(headers, body):
    return body


def parse_str(headers, body):
    return headers[0]


CRLF = '\r\n'
B_CRLF = b'\r\n'
PROTOCOL = {
    # command: (expected_ok, {expected_errors}, format)
    'put': (
        b'INSERTED',
        {b'BURIED', b'EXPECTED_CLRF', b'JOB_TOO_BIG', b'DRAINING'},
        parse_int,
    ),
    'reserve': (
        b'RESERVED',
        {b'DEADLINE_SOON'},
        parse_job,
    ),
    'peek': (
        b'FOUND',
        {b'NOT_FOUND'},
        parse_job,
    )
}
RESP_WITH_BODY = {b'RESERVED', b'FOUND', b'OK'}


def encode_command(command, *args, body=None):
    if command not in PROTOCOL:
        raise InvalidCommand('invalid command')
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
