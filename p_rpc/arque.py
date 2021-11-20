#!/usr/bin/env python3
import socket
import asyncio
from functools import partial
from contextlib import suppress

import arun


MAX_MSG = 130*1024


def _csock(cname):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    if (cname is not None):
        sock.bind(cname)
    sock.setblocking(False)
    return sock


def _cname(prefix, bus_name, peer_name):
    return '\0'+prefix+'\0'+bus_name+'\0'+peer_name


async def _caccept(sock):
    loop = arun.loop()
    ret, _ = await loop.sock_accept(sock)
    ret.setblocking(False)
    return ret


def arque(bus_name, self_name, msg_cb):
    _lname = partial(_cname, 'local', bus_name)
    _lsock = partial(_csock, _lname(self_name))
    _tsock = partial(_csock, None)

    lock_ = asyncio.Lock()
    sock_ = _lsock()
    sock_.listen()
    ichans_ = {}
    tchans_ = {}

    async def _serv_recv(sock, chans, peer_name):
        loop = arun.loop()
        try:
            while (True):
                msg = await loop.sock_recv(sock, MAX_MSG)
                if len(msg) == 0:
                    break
                print(f'{bus_name}, {self_name}, {peer_name}, {len(msg)}')
                with suppress(Exception):
                    await msg_cb(peer_name, msg)
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

    async def _iniator_handshake(peer_name):
        loop = arun.loop()
        async with lock_:
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
            sock = await _iniator_handshake(peer_name)
        await loop.sock_sendall(sock, msg)

    class inner:
        enqueue = _enqueue
        max_msg = MAX_MSG

    return inner


if __name__ == '__main__':
    chan1 = arque('test', 'ty', None)
    chan2 = arque('test', 'test1', None)

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
