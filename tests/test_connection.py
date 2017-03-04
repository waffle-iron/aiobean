from aiobean.connection import create_connection
from functools import partial
import pytest


pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


@pytest.fixture
def conn_factory(server, event_loop):
    return partial(create_connection, *server.address, loop=event_loop)


async def test_put(conn_factory):
    conn = await conn_factory()
    fut = conn._execute('put', 10, 0, 10, 2, body=b'hi')
    jid = await fut
    assert isinstance(jid, int)
