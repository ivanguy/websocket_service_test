import asyncio
import logging
import os
from uuid import uuid4

import aioredis
import aiohttp
from aiohttp import web

REDIS_PORT = os.environ.get('REDIS_PORT', '')
RUN_PORT = int(os.environ.get('RUN_PORT', 8000))
DROP_CLIENT_CHANNEL_NAME = 'drop_client_channel'

if os.environ.get('PYTHONASYNCIODEBUG'):
    logging.basicConfig(level=logging.DEBUG)


async def handle_ws_connect(request):
    """Accept HTTP connection with Upgrade to ws
    """
    client_id = request.query.get('client_id')
    assert client_id  # TODO
    session_id = uuid4()
    if ws := request.app['clients'].get(client_id):
        await ws.close()
    else:
        await request.app['redis_pool'].publish(DROP_CLIENT_CHANNEL_NAME, client_id)
    await request.app['redis_pool'].set(client_id, session_id)

    try:
        ws = web.WebSocketResponse()
        app['clients'][client_id] = ws
        await ws.prepare(request)
        await ws.send_str(str(session_id))
        # try:
        ping = False
        while True:
            try:
                msg = await ws.receive(timeout=5)
                if msg.type == aiohttp.WSMsgType.TEXT:
                    print(msg.data)
                elif msg.type == aiohttp.WSMsgType.PING:
                    await ws.pong(msg.data)
                elif msg.type == aiohttp.WSMsgType.PONG:
                    ping = False
                else:
                    break
            except asyncio.TimeoutError:
                if not ping:
                    ping = True
                    await ws.ping()
                else:
                    break
    finally:
        await ws.close()
        deleted = await app['redis_pool'].eval(
            "if redis.call('get', KEYS[1])==ARGV[1] do redis.call('del', KEYS[1]) return 1 end return 0",
            [client_id],
            [session_id])
        if not deleted:
            app.logger.debug("%s session_id changed before closing session %s", client_id, session_id)
        app['clients'].pop(client_id, None)
    return ws


async def redis_connect_ctx_mngr(app):
    """Redis connection pool context manager"""
    # on_startup
    conn_string = 'redis://localhost'
    conn_string += f":{REDIS_PORT}" if REDIS_PORT else ''
    app['redis_pool'] = await aioredis.create_redis_pool(conn_string)
    app.logger.debug('Connected to redis at %s', app['redis_pool'].address)
    yield
    # on_cleanup
    app['redis_pool'].close()
    await app['redis_pool'].wait_closed()
    app.logger.debug('Redis connection closed')


async def del_sessions_from_redis(app):
    """Remove session keys from redis using lua script (transactional)
    """
    deleted = await app['redis_pool'].eval(
        script="local count=0 "
               "for i, key in ipairs(KEYS) do"
               " if redis.call('get', key)==ARGV[i]"
               " then redis.call('DEL', key) count=count+1 end "
               "end "
               "return count",
        keys=[app['clients'].keys()],
        args=[app['clients'].values()])  # TODO ws are stored here, we need session_ids
    app.logger.debug('%s sessions deleted from redis', deleted)


async def redis_drop_client_listener(app):
    """Listens to drop client commands from MQ"""
    async with app['redis_pool'] as conn:
        channels = await conn.subscribe(DROP_CLIENT_CHANNEL_NAME)
        channel = channels[0]
        async for client_id in channel.iter():
            ws = app['clients'].get(client_id)
            if ws:
                await ws.close()
                app['clients'].pop(client_id, None)


app = web.Application()
app.add_routes([web.get('/', handle_ws_connect)])
app.cleanup_ctx.append(redis_connect_ctx_mngr)
app.on_startup.append(redis_drop_client_listener)
# app.on_shutdown.append(del_sessions_from_redis)
app['clients'] = dict()
# disconnect handler

if __name__ == '__main__':
    web.run_app(app, host='localhost', port=RUN_PORT)
