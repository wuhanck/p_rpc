#!/usr/bin/env python3
import asyncio

import arun


def gen_connected_msg_sock(send, recv, close, FRAME_SIZE):
    slock_ = asyncio.Lock()

    async def _send(msg):
        start, end = 0, len(msg)
        if end == 0: return
        hdr = end.to_bytes(4, 'big')
        async with slock_:
            await send(hdr)
            while start < end:
                await send(msg[start : start+FRAME_SIZE])
                start += FRAME_SIZE

    async def _recv():
        hdr = await recv()
        if len(hdr) == 0: return hdr
        assert len(hdr) == 4, f'header len 4 {len(hdr)}'
        start, end = 0, int.from_bytes(hdr, 'big')
        assert end != 0, f'recv zero-length msg'
        ret = []
        while start < end:
            fm = await recv()
            start += len(fm)
            ret.append(fm)
        ret = b''.join(ret)
        assert len(ret) == end, f'content len {end} {len(ret)}'
        return ret

    def _close(): close()

    class inner:
        send = _send
        recv = _recv
        close = _close
    return inner


def gen_listened_sock(accept, close):

    async def _accept(): return await accept()

    def _close(): close()

    class inner:
        accept = _accept
        close = _close
    return inner


def _null_lsock(bus_name, self_name):

    async def _accept(): await arun.future()

    def _close(): pass

    return gen_listened_sock(_accept, _close)


async def _null_tsock(bus_name, peer_name):
    raise Exception('null tsock')


def gen_proto(lsock=_null_lsock, tsock=_null_tsock):

    def _lsock(bus_name, self_name): return lsock(bus_name, self_name)

    async def _tsock(bus_name, peer_name): return await tsock(bus_name, peer_name)

    class inner:
        lsock = _lsock
        tsock = _tsock
    return inner


if __name__ == '__main__':
    s = null_lsock('test', 'test')
    arun.append_task(s.accept())
    arun.run()
