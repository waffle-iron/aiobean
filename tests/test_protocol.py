from aiobean.protocol import BeanstalkProtocol
import pytest


pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


@pytest.fixture
def protocol_factory(server, event_loop):

    async def _protocol_factory():
        transport, proto = await event_loop.create_connection(
            BeanstalkProtocol, *server.address)
        return proto

    return _protocol_factory


async def test_connect(protocol_factory, server):
    protocol = await protocol_factory()
    assert protocol.server_address == server.address
