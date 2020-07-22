import aiohttp
import argparse
import asyncio
import uuid

class Client:

    def __init__(self, endpoint, client_id):
        self.endpoint = endpoint
        self.client_id = client_id
        self.session_id = None

    async def connect(self):
        try:
            async with aiohttp.ClientSession() as session:
                web_socket = await session.ws_connect(f'{self.endpoint}?client_id={self.client_id}', timeout=5, autoping=False)
                ping = False
                while True:
                    try:
                        msg = await web_socket.receive(timeout=5)
                        print(msg.type)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self.session_id = msg.data
                            print(msg.data)
                        elif msg.type == aiohttp.WSMsgType.PING:
                            await web_socket.pong(msg.data)
                        elif msg.type == aiohttp.WSMsgType.PONG:
                            ping = False
                        else:
                            break
                    except asyncio.TimeoutError:
                        if not ping:
                            ping = True
                            await web_socket.ping()
                        else:
                            await web_socket.close()
                            break
                    except asyncio.exceptions.CancelledError:
                        break
        finally:
            self.session_id = None

def get_args():
    parser = argparse.ArgumentParser(prog='client')
    parser.add_argument('--client-id', help='client id')
    parser.add_argument('--endpoint', help='connection endpoint')
    return parser.parse_args()

def main():
    args = get_args()
    loop = asyncio.get_event_loop()
    client = Client(args.endpoint, args.client_id)
    loop.run_until_complete(client.connect())


if __name__ == '__main__':
    main()