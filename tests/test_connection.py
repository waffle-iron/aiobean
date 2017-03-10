import asyncio
from aiobean.connection import create_connection, ConnectionClosedError
from aiobean.protocol import DeadlineSoon
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
            self._conn.close()
            await self._conn.wait_closed()

    return ConnContext


async def test_client_close(conn_factory):
    async with conn_factory() as conn:
        assert not conn.closed
        stats_task = conn.stats()
        conn.close()
        await conn.wait_closed()
        with pytest.raises(asyncio.CancelledError):
            await stats_task
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
        await conn.wait_closed()
        with pytest.raises((asyncio.CancelledError, ConnectionResetError)):
            await stats_task
        assert conn.closed


async def test_execute(conn_factory):
    async with conn_factory() as conn:
        fut = conn.execute('put', 10, 0, 10, 2, body=b'hi')
        jid = await fut
        assert isinstance(jid, int)
        # should not be able to execute if connection is closed/closing
        conn.close()
        with pytest.raises(ConnectionClosedError):
            conn.execute('stats')


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
            # can list all tubes
            assert await producer.tubes() == ['default', tube]

            # nothing is ready in the default tube
            assert not await worker.reserve(0.1)

            # work in test tube
            await worker.watch(tube)
            assert await worker.watched() == ['default', tube]

            # can reserve, touch and release
            assert await worker.reserve() == (jid, b'test')
            await worker.touch(jid)
            await worker.release(jid)

            # can get stats of a job
            job_stats = await worker.stats_job(jid)
            assert job_stats['id'] == jid

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

            # can pause tube
            await worker.pause_tube(tube, 1)
            # can ignore tube
            await worker.ignore(tube)
            assert await worker.watched() == ['default']


async def test_deadline_soon(conn_factory):
    async with conn_factory() as conn:
        await conn.put(b'test', ttr=1)
        await conn.reserve()
        # during the TTR of a reserved job, the last second is kept by the
        # server as a safety margin. If the client issues a reserve during
        # this margin, it is told that the dealine of a previous job is soon
        with pytest.raises(DeadlineSoon):
            await conn.reserve()


async def test_stats(conn_factory):
    async with conn_factory() as conn:
        # can get server stats
        server_stats = await conn.stats()
        assert 'version' in server_stats
        # can get tube status
        tube_stats = await conn.stats_tube()
        assert tube_stats['name'] == 'default'
