import asyncio
import logging
import os
from uuid import uuid4

import aioredis
from aiohttp import web
from aioredis_lock import RedisLock, LockTimeoutError

REDIS_PORT = os.environ.get('REDIS_PORT', '')
RUN_PORT = int(os.environ.get('RUN_PORT', 8000))

SERVER_ID = uuid4()
clients = dict()
logging.basicConfig(level=logging.DEBUG)

# redis clanup context conn
# async def redis conn (app)
# app['redis]' == redis.conn
# yield
# app['redis'].close()
# await app['resid']is_closed()

async def connect(request):
    """Accept HTTP connection with Upgrade to ws
    """
    client_id = request.query.get('client_id')
    assert client_id  # TODO
    if not clients.get(client_id):
        async with RedisLock(
                app['redis'],
                client_id,
                timeout=5,
                wait_timeout=10
        ) as db_session_lock:
            # getset
            # publish disconnect
            session_str = await app['redis'].get('client_id')

    # add to clients tasks
    try:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        session_id = uuid4()
        res = await ws.send_str(str(session_id))
        # try:
        async for msg in ws:
            print(msg)  # TODO msg.type PING PONG TEXT
        # except ws CLOSED exc
    except asyncio.exceptions.CancelledError:
        ws.close()
        # drop client
    return ws


async def redis_connect(app):
    """Redis connection pool context manager"""
    conn_string = 'redis://localhost'
    conn_string += f":{REDIS_PORT}" if REDIS_PORT else ''
    app['redis'] = await aioredis.create_redis_pool(conn_string)
    yield
    app['redis'].close()
    await app['redis'].wait_closed()


async def del_key(client_id):
    """Del client_id session, passing if client is connecting with another server
    """
    try:
        async with RedisLock(app['redis'], client_id, wait_timeout=1):
            await app['redis'].delete(client_id)
    except LockTimeoutError:
        pass


async def del_sessions(app):
    tasks = (asyncio.create_task(del_key(key)) for key in clients.keys())
    await asyncio.wait(tasks)


async def redis_drop_sub(app):
    """Listens to drop client commands from MQ"""
    redis_pool = app['redis']
    with await app['redis'] as conn:
        channels = await conn.subscribe(str(SERVER_ID))
        channel = channels[0]

        async for client_id in channel.iter():
            ws = clients.get(client_id)
            ws.close()


app = web.Application()
app.add_routes([web.get('/', connect)])
app.cleanup_ctx.append(redis_connect)
# app.on_startup.append(redis_drop_sub)
app.on_shutdown.append(del_sessions)
# disconnect handler

if __name__ == '__main__':
    web.run_app(app, host='localhost', port=RUN_PORT)
