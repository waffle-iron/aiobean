from aiobean.protocol import (
    CommandFailed, InvalidCommand, PYYAML, UnexpectedResponse,
    encode_command, handle_head, handle_response,
    _parse_body, _parse_int, _parse_str, _parse_yml,
)
import pytest


def _encode_command(*args, **kwargs):
    return list(encode_command(*args, **kwargs))


def test_encode_command():
    # simple command
    assert _encode_command('reserve') == [b'reserve\r\n']
    # command with extra headers
    assert _encode_command('peek', 10) == [b'peek 10\r\n']
    # command with body
    assert _encode_command(
        'put', 10, 0, 10, 2, body=b'ab') == [
            b'put 10 0 10 2\r\n', b'ab', b'\r\n']


def test_invalid_command():
    with pytest.raises(InvalidCommand):
        _encode_command('blah')


def test_body_type():
    with pytest.raises(TypeError) as exc_info:
        _encode_command('put', body='body')
    exc_info.match('must be a byte-like object')


def test_handle_head():
    # simple head: status only
    assert handle_head(b'NOT_FOUND\r\n') == (b'NOT_FOUND', [], 0)
    # with headers
    assert handle_head(b'WATCHING 1\r\n') == (b'WATCHING', [b'1'], 0)
    # with body
    for line in (
        b'RESERVED 1 10',
        b'FOUND 1 10',
        b'OK 10',
    ):
        assert handle_head(line)[2] == 10


def test_handle_reponse():
    # ok response
    assert handle_response('put', b'INSERTED', [b'10'], None) == 10
    # expected errors
    with pytest.raises(CommandFailed):
        handle_response('put', b'JOB_TOO_BIG', [], None)
    # unexpected errors
    with pytest.raises(UnexpectedResponse):
        handle_response('put', b'OUT_OF_MEMORY', [], None)


# test parsers
_yml_body = b'''
---
hostname: huston
uptime: 10
'''


@pytest.mark.parametrize('parser,args,expected', [
    [_parse_int, ([b'10'],), 10],
    [_parse_str, ([b'default'],), 'default'],
    [_parse_body, ([b'200', b'4'], b'body'), b'body'],
    [_parse_yml, ([], _yml_body), dict(hostname='huston', uptime=10)]
])
def test_parser(parser, args, expected):
    assert parser(*args) == expected


@pytest.mark.skipif(PYYAML, reason='only run when pyyaml is not available')
def test_parse_yml_without_pyyaml():
    assert _parse_yml([], _yml_body) == _yml_body.decode()
