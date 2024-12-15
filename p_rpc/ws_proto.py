#!/usr/bin/env python3
import asyncio
from functools import partial

from websockets.asyncio.server import serve
from websockets.asyncio.client import connect

import arun

from .general_proto import gen_connected_msg_sock, gen_listened_sock, gen_proto


def _ws_path(bus_name, peer_name):
    return f'/{bus_name}/{peer_name}'


def _connected_ws(sock, fut):
    FRAME_SIZE = 8*1024

    close_ = partial(fut.set_result, 'close')

    return gen_connected_msg_sock(sock.send, sock.recv, close_, FRAME_SIZE)


def ws_l_proto(host, port):
    aqs_ = {}  # map for accept queues

    async def _ws_handler(ws):
        path = ws.request.path
        aq = aqs_.get(path)
        if aq is None: raise Exception(f'{path} not listened')
        fut = arun.future()
        sock = _connected_ws(ws, fut)
        await aq.put(sock)
        await fut  # and then framework will close the ws

    async def _ws():
        async with serve(_ws_handler, host, port) as server:
            await server.serve_forever()

    arun.append_task(_ws())

    def _ws_lsock(bus_name, self_name):
        aq = asyncio.Queue()
        path = _ws_path(bus_name, self_name)
        if aqs_.get(path) is not None: raise Exception(f'{path} listened already')
        aqs_[path] = aq
        close_ = partial(aqs_.pop, path, None)
        return gen_listened_sock(aq.get, close_)

    return gen_proto(lsock=_ws_lsock)


def ws_t_proto(base_uri):

    async def _ws_tsock(bus_name, peer_name):
        url = f'{base_uri}{_ws_path(bus_name, peer_name)}'
        ws = await connect(url)
        fut = arun.future()
        sock = _connected_ws(ws, fut)

        async def close():
            try:
                await fut
            finally:
                await ws.close()
        arun.post_in_task(close())
        return sock

    return gen_proto(tsock=_ws_tsock)


if __name__ == "__main__":
    pt1 = ws_l_proto("localhost", 8765)
    pt2 = ws_t_proto("ws://localhost:8765")

    pt1.lsock('test', 'serv1')

    async def connect_test():
        await arun.sleep(5)
        s = await pt2.tsock('test', 'serv1')
        await s.send('test')
        await arun.sleep(1)
        s.close()

    arun.append_task(connect_test())
    arun.run()
