#!/usr/bin/env python3
import asyncio

import arun


def gen_connected_msg_sock(send, recv, close, FRAME_SIZE):
    slock_ = asyncio.Lock()
    rlock_ = asyncio.Lock()

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
        async with rlock_:
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

    def _close():
        try:
            close()
        except Exception:
            pass

    class inner:
        send = _send
        recv = _recv
        close = _close
    return inner


def gen_listened_sock(accept, close):
    accept_ = accept

    def _close():
        try:
            close()
        except Exception:
            pass

    class inner:
        accept = accept_
        close = _close
    return inner


def _null_lsock(bus_name, self_name):
    fut_ = None

    async def _accept():
        nonlocal fut_
        fut_ = arun.future()
        await fut_

    def _close():
        if fut_ is not None: fut_.set_result('close')

    return gen_listened_sock(_accept, _close)


async def _null_tsock(bus_name, peer_name):
    raise Exception('null tsock')


def gen_proto(lsock=_null_lsock, tsock=_null_tsock):
    lsock_ = lsock
    tsock_ = tsock

    class inner:
        lsock = lsock_
        tsock = tsock_
    return inner


if __name__ == '__main__':
    s = _null_lsock('test', 'test')

    async def test_close():
        await arun.sleep(5)
        s.close()

    arun.append_task(s.accept(), test_close())
    arun.run()
