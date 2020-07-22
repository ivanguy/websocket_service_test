import asyncio
import logging
import os
import sys
from uuid import uuid4

import aioredis
import aiohttp
from aiohttp import web

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT', '')
RUN_PORT = int(os.environ.get('RUN_PORT', 8000))
DROP_CLIENT_CHANNEL_NAME = 'drop_client_channel'

if os.environ.get('PYTHONASYNCIODEBUG'):
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)


async def handle_ws_connect(request):
    """Accept HTTP connection with Upgrade to ws
    """
    client_id = request.query.get('client_id')
    assert client_id  # TODO
    session_id = str(uuid4())
    old_ws = request.app['sockets'].get(client_id, None)
    if old_ws is not None:
        await old_ws.close()
    else:
        await request.app['redis_pool'].publish(DROP_CLIENT_CHANNEL_NAME, client_id)
    await request.app['redis_pool'].set(client_id, session_id)

    ws = web.WebSocketResponse()
    try:
        app['sockets'][client_id] = ws
        app['sessions'][client_id] = session_id
        await ws.prepare(request)
        await ws.send_str(str(session_id))
        app.logger.info(f"Client {client_id} connected with session {session_id}")
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
                await ws.ping()
                ping = True
    finally:
        await ws.close()
        deleted = await app['redis_pool'].eval(
            "if redis.call('get', KEYS[1])==ARGV[1] then redis.call('del', KEYS[1]) return 1 end return 0",
            [client_id],
            [session_id])
        if not deleted:
            app.logger.debug("%s session_id changed before closing session %s", client_id, session_id)
        app['sockets'].pop(client_id, None)
        app['sessions'].pop(client_id, None)
        app.logger.info(f"Client {client_id} {session_id} disconnected")
    return ws


async def redis_connect_ctx_mngr(app):
    """Redis connection pool context manager"""
    # on_startup

    conn_string = REDIS_HOST if REDIS_HOST else 'localhost'
    conn_string = 'redis://' + conn_string
    conn_string += f":{REDIS_PORT}" if REDIS_PORT else ''
    app['redis_pool'] = await aioredis.create_redis_pool(conn_string)
    app.logger.info('Connected to redis at %s', app['redis_pool'].address)
    yield
    # on_cleanup
    app['redis_pool'].close()
    await app['redis_pool'].wait_closed()
    app.logger.info('Redis connection closed')


# async def del_sessions_from_redis(app):
#     """Remove session keys from redis using lua script (transactional)
#     """
#     deleted = await app['redis_pool'].eval(
#         script="local count=0 "
#                "for i, key in ipairs(KEYS) do"
#                " if redis.call('get', key)==ARGV[i]"
#                " then redis.call('DEL', key) count=count+1 end "
#                "end "
#                "return count",
#         keys=[app['sessions'].keys()],
#         args=[app['sessions'].values()])
#     app.logger.debug('%s sessions deleted from redis', deleted)


async def create_drop_client_listener(app):
    """Listens to drop client commands from MQ"""

    async def listen_drop_channel(app):
        with await app['redis_pool'] as conn:
            channels = await conn.subscribe(DROP_CLIENT_CHANNEL_NAME)
            channel = channels[0]
            async for client_id in channel.iter():
                client_id = client_id.decode()
                app.logger.debug("Recieved drop message %s", client_id)
                ws = app['sockets'].get(client_id)
                if ws is not None:
                    await ws.close()

    asyncio.get_event_loop().create_task(listen_drop_channel(app))


async def session_list_handler(request):
    """Lists clients and sessions connected to this server"""
    return web.Response(
        text="".join("{} {}\n".format(client_id, session_id) for client_id, session_id in app['sessions'].items()))


app = web.Application()
app.add_routes([web.get('/', handle_ws_connect)])
app.add_routes([web.get('/sessions', session_list_handler)])
app.cleanup_ctx.append(redis_connect_ctx_mngr)
app.on_startup.append(create_drop_client_listener)
# app.on_shutdown.append(del_sessions_from_redis)
app['sockets'] = dict()
app['sessions'] = dict()

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=RUN_PORT)
