import asyncio
from aiobean.connection import create_connection, ConnectionLost
import pytest


pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


@pytest.fixture
def conn_factory(server, event_loop):

    class ConnContext:

        def __init__(self):
            self._conn = None

        async def __aenter__(self):
            self._conn = await create_connection(
                *server.address, loop=event_loop)
            return self._conn

        async def __aexit__(self, exc_type, exc, tb):
            closing = self._conn.close()
            if closing:
                await closing

    return ConnContext


async def test_client_close(conn_factory):
    async with conn_factory() as conn:
        assert not conn.closed
        stats_task = conn.stats()
        await conn.close()
        with pytest.raises(asyncio.CancelledError):
            assert stats_task.result()
    await asyncio.sleep(0)
    assert conn.closed
    assert conn._read_task.cancelled()
    assert not conn._queue


@pytest.mark.parametrize('close_method', [
    'terminate',  # server properly closed the connection
    'kill',  # connection lost or server closed without telling the client
])
async def test_server_close(conn_factory, server, close_method):
    async with conn_factory() as conn:
        getattr(server, close_method)()
        stats_task = conn.stats()
        with pytest.raises(ConnectionLost):
            await stats_task
        assert conn.closed


async def test_execute(conn_factory):
    async with conn_factory() as conn:
        fut = conn.execute('put', 10, 0, 10, 2, body=b'hi')
        jid = await fut
        assert isinstance(jid, int)


async def test_delete(conn_factory):
    async with conn_factory() as conn:
        id = await conn.put(b'hi')
        await conn.delete(id)


async def test_producer(conn_factory):
    async with conn_factory() as conn:
        # use tube
        assert 'test-tube' == await conn.use('test-tube')
        assert 'test-tube' == await conn.used()

        jid = await conn.put(b'123')
        job_stats = await conn.stats_job(jid)
        assert job_stats['tube'] == 'test-tube'


async def test_peek(conn_factory):
    async with conn_factory() as conn:
        # nothing at first
        assert not await conn.peek_ready()
        assert not await conn.peek_buried()
        assert not await conn.peek_delayed()

        jid = await conn.put(b'test')
        assert await conn.peek(jid) == (jid, b'test')
        assert await conn.peek_ready() == (jid, b'test')

        jid, _ = await conn.reserve()
        await conn.bury(jid)
        assert await conn.peek_buried() == (jid, b'test')

        delayed_id = await conn.put(b'delayed', delay=1)
        assert await conn.peek_delayed() == (delayed_id, b'delayed')


async def test_worker(conn_factory):
    async with conn_factory() as producer:
        async with conn_factory() as worker:
            tube = 'test-tube'
            await producer.use(tube)
            jid = await producer.put(b'test')

            # nothing is ready in the default tube
            assert not await worker.reserve(0.1)

            # work in test tube
            await worker.watch(tube)
            assert await worker.watched() == ['default', tube]

            # can reserve and release
            assert await worker.reserve() == (jid, b'test')
            await worker.release(jid)

            # delete when finished
            assert await worker.reserve() == (jid, b'test')
            await worker.delete(jid)
            assert not await worker.reserve(0.1)  # no job anymore

            # bury
            jid = await producer.put(b'bury')
            assert await worker.reserve() == (jid, b'bury')
            await worker.bury(jid)
            await worker.peek_buried()
            # kick
            assert await producer.kick(1) == 1
            assert (await worker.reserve(0.1))[0] == jid
            await worker.delete(jid)
            # bury it again to test bury-job
            jid = await producer.put(b'bury2')
            await worker.reserve()
            await worker.bury(jid)
            await worker.kick_job(jid)
            assert await worker.reserve() == (jid, b'bury2')
