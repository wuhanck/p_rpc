#!/usr/bin/env python3
import socket
import sys
import os
import typing

import arun

MAX_MSG = 130*1024


def _cname(bus_name, name):
    return '\0'+bus_name+'\0'+name

def _csock(cname):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        sock.bind(cname)
        sock.setblocking(False)
        return sock

def arque(bus_name, self_name, dequeue_cb):
    cname_ = _cname(bus_name, self_name)
    sock_ = _csock(cname_)
    sock_.listen()
    chans_ = {}

    def _add_chan(sock, cname):
        if (cname != '' and chans_.get(cname, None) is None):
            sock.setblocking(False)
            chans_[cname] = sock

            async def _serv_recv():
                loop = arun.loop()
                try:
                    while (True):
                        msg = await loop.sock_recv(sock, MAX_MSG)
                        if len(msg) == 0:
                            break
                        print(len(msg))
                        # dequeue_cb(msg)
                finally:
                    chans_.pop(cname, None)
                    sock.close()

            arun.post_in_task(_serv_recv())
            return sock
        sock.close()
        return None

    async def _serv_accept():
        loop = arun.loop()
        while (True):
            sock, cname = await loop.sock_accept(sock_)
            _add_chan(sock, cname)

    arun.append_task(_serv_accept())

    async def _connect(peer_name):
        loop = arun.loop()
        cname = _cname(bus_name, peer_name)
        sock = _csock(cname)
        await loop.socket_connect(sock, cname)
        return _add_chan(sock, cname)

    async def _enqueue(peer_name, msg):
        loop = arun.loop()
        cname = _cname(bus_name, peer_name)
        sock = chans_.get(cname, None)
        if (sock is None):
            sock = await _connect(peer_name)
        await loop.send(sock, msg)

    class inner:
        enqueue = _enqueue
        max_msg = MAX_MSG

    return inner


if __name__ == '__main__':
    arque('test', 'ty', None)

    msg = bytearray(MAX_MSG)

    async def _test_connect(idx):
        loop = arun.loop()
        cname = _cname('test', 'client')
        sock = _csock(cname)
        print(idx)
        await loop.sock_connect(sock, '\0test\0ty')
        await loop.sock_sendall(sock, msg)
        await loop.sock_sendall(sock, msg)
        await loop.sock_sendall(sock, msg)
        await loop.sock_sendall(sock, msg)

    arun.append_task(_test_connect(1))
    arun.append_task(_test_connect(2))
    arun.append_task(_test_connect(3))
    arun.append_task(_test_connect(4))
    arun.run(forever=False)
