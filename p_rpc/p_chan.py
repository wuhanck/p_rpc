#!/usr/bin/env python3
import socket
import asyncio
from functools import partial

import arun


MAX_MSG = 130*1024
LOCKS_NUM = 64


def _cname(prefix, bus_name, peer_name):
    return '\0'+prefix+'\0'+bus_name+'\0'+peer_name


def _csock(cname):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    if (cname is not None):
        sock.bind(cname)
    sock.setblocking(False)
    return sock


async def _caccept(sock):
    loop = arun.loop()
    ret, _ = await loop.sock_accept(sock)
    ret.setblocking(False)
    return ret


def chan(bus_name, self_name):
    _lname = partial(_cname, 'local', bus_name)
    _lsock = partial(_csock, _lname(self_name))
    _tsock = partial(_csock, None)

    sock_ = _lsock()
    sock_.listen()
    ichans_ = {}
    ilocks_ = {}
    tchans_ = {}
    msg_cb_ = None

    def _cb(msg_cb):
        nonlocal msg_cb_
        assert(asyncio.iscoroutinefunction(msg_cb))
        msg_cb_ = msg_cb

    async def _serv_recv(sock, chans, peer_name):
        loop = arun.loop()
        try:
            while (True):
                msg = await loop.sock_recv(sock, MAX_MSG)
                if len(msg) == 0:
                    break
                if (msg_cb_ is not None):
                    arun.post_in_task(msg_cb_(peer_name, msg))
        finally:
            chans.pop(peer_name, None)
            sock.close()

    async def _target_handshake(sock):
        loop = arun.loop()
        try:
            peer_name = await loop.sock_recv(sock, MAX_MSG)
            if (len(peer_name) == 0):
                raise Exception('sock shutdown')
            peer_name = peer_name.decode()
            if (tchans_.get(peer_name, None) is not None):
                raise Exception('chan already exit')
            tchans_[peer_name] = sock
            arun.post_in_task(_serv_recv(sock, tchans_, peer_name))
        except Exception:
            sock.close()

    async def _initiator_handshake(peer_name):
        idx = hash(peer_name) % LOCKS_NUM
        lock = ilocks_.get(idx, None)
        if (lock is None):
            lock = asyncio.Lock()
            ilocks_[idx] = lock
        loop = arun.loop()
        async with lock:
            sock = ichans_.get(peer_name, None)
            if (sock is not None):
                return sock
            sock = _tsock()
            try:
                await loop.sock_connect(sock, _lname(peer_name))
                await loop.sock_sendall(sock, self_name.encode())
                ichans_[peer_name] = sock
                arun.post_in_task(_serv_recv(sock, ichans_, peer_name))
                return sock
            except Exception:
                sock.close()

    async def _serv_accept():
        _laccept = partial(_caccept, sock_)
        while (True):
            sock = await _laccept()
            arun.post_in_task(_target_handshake(sock))

    arun.append_task(_serv_accept())

    async def _enqueue(peer_name, msg):
        loop = arun.loop()
        sock = tchans_.get(peer_name, None)
        if (sock is None):
            sock = ichans_.get(peer_name, None)
        if (sock is None):
            sock = await _initiator_handshake(peer_name)
        if (sock is None):
            raise Exception(f'unreachable peer: {peer_name}')
        await loop.sock_sendall(sock, msg)

    class inner:
        cb = _cb
        enqueue = _enqueue
        max_msg = MAX_MSG

    return inner


if __name__ == '__main__':
    chan1 = chan('test', 'ty')
    chan2 = chan('test', 'test1')

    async def print_msg(peer_name, msg):
        print(f'{peer_name} msg-len: {len(msg)}')

    chan1.cb(print_msg)

    msg = bytearray(MAX_MSG)
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
