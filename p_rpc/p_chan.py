#!/usr/bin/env python3
import socket
import asyncio
from functools import partial

import arun


def _abs_uds_name(bus_name, peer_name): # abstract name (need to used by abs uds)
    return '\0'+bus_name+'\0'+peer_name


def _connected_sock(sock):
    FRAME_SIZE = 128*1024
    slock_ = asyncio.Lock()

    async def _send(msg):
        loop = arun.loop()
        start, end = 0, len(msg)
        if end == 0: return
        hdr = end.to_bytes(4, 'big')
        async with slock_:
            await loop.sock_sendall(sock, hdr)
            while start < end:
                await loop.sock_sendall(sock, msg[start : start+FRAME_SIZE])
                start += FRAME_SIZE

    async def _recv():
        loop = arun.loop()
        hdr = await loop.sock_recv(sock, FRAME_SIZE)
        if len(hdr) == 0: return hdr
        assert len(hdr) == 4, f'header len 4 {len(hdr)}'
        start, end = 0, int.from_bytes(hdr, 'big')
        assert end != 0, f'recv zero-length msg'
        ret = []
        while start < end:
            fm = await loop.sock_recv(sock, FRAME_SIZE)
            start += len(fm)
            ret.append(fm)
        ret = b''.join(ret)
        assert len(ret) == end, f'content len {end} {len(ret)}'
        return ret

    def _close():
        sock.close()

    class inner:
        send = _send
        recv = _recv
        close = _close
    return inner


def _listened_sock(sock):
    async def _accept():
        loop = arun.loop()
        ret, _ = await loop.sock_accept(sock)
        return _connected_sock(ret)

    def _close():
        sock.close()

    class inner:
        accept = _accept
        close = _close
    return inner


async def tsock(bus_name, peer_name):
    cname = _abs_uds_name(bus_name, peer_name)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET
            |socket.SOCK_CLOEXEC
            |socket.SOCK_NONBLOCK)

    try:
        loop = arun.loop()
        await loop.sock_connect(sock, cname)
    except Exception:
        sock.close()
        raise

    return _connected_sock(sock)


def lsock(bus_name, self_name):
    cname = _abs_uds_name(bus_name, self_name)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET
            |socket.SOCK_CLOEXEC
            |socket.SOCK_NONBLOCK)

    try:
        sock.bind(cname)
        sock.listen()
    except Exception:
        sock.close()
        raise

    return _listened_sock(sock)


def chan(bus_name, self_name):
    sock_ = lsock(bus_name, self_name)
    ichans_ = {}
    tchans_ = {}
    msg_cb_ = None

    def _cb(msg_cb):
        nonlocal msg_cb_
        assert asyncio.iscoroutinefunction(msg_cb)
        msg_cb_ = msg_cb

    async def _serv_recv(sock, chans, peer_name):
        try:
            while True:
                msg = await sock.recv()
                if len(msg) == 0:
                    break
                if msg_cb_ is not None:
                    arun.post_in_task(msg_cb_(peer_name, msg))
        finally:
            chans.pop(peer_name)
            sock.close()

    async def _target_handshake(sock):
        try:
            peer_name = await sock.recv()
            if len(peer_name) == 0:
                raise Exception('sock shutdown')
            peer_name = peer_name.decode()
            if tchans_.get(peer_name, None) is not None:
                raise Exception('chan already exit')
            tchans_[peer_name] = sock
            arun.post_in_task(_serv_recv(sock, tchans_, peer_name))
        except Exception:
            sock.close()

    async def _initiator_handshake(peer_name):
        try:
            sock = await tsock(bus_name, peer_name)
            await sock.send(self_name.encode())
            other_sock = ichans_.get(peer_name, None)
            if other_sock is None:
                ichans_[peer_name] = sock
                arun.post_in_task(_serv_recv(sock, ichans_, peer_name))
                return sock
            else:
                sock.close()
                return other_sock
        except Exception:
            if sock: sock.close()

    async def _serv_accept():
        while True:
            sock = await sock_.accept()
            arun.post_in_task(_target_handshake(sock))

    arun.append_task(_serv_accept())

    async def _enqueue(peer_name, msg):
        sock = tchans_.get(peer_name, None)
        if sock is None:
            sock = ichans_.get(peer_name, None)
        if sock is None:
            sock = await _initiator_handshake(peer_name)
        if sock is None:
            raise Exception(f'unreachable peer: {peer_name}')
        await sock.send(msg)

    class inner:
        cb = _cb
        enqueue = _enqueue

    return inner


if __name__ == '__main__':
    chan1 = chan('test', 'ty')
    chan2 = chan('test', 'test1')

    async def print_msg(peer_name, msg):
        print(f'{peer_name} msg-len: {len(msg)}')

    chan1.cb(print_msg)

    msg = bytearray(1240*1023*78)
    bs = bytearray()
    bs.decode()

    async def _test_self(idx):
        print(idx)
        await chan1.enqueue('ty', msg)
        pass

    async def _test_connect(idx):
        print(idx)
        await chan2.enqueue('ty', msg)
        pass

    arun.append_task(_test_self(1))
    arun.append_task(_test_self(2))
    arun.append_task(_test_connect(3))
    arun.append_task(_test_connect(4))
    arun.run(forever=False)
