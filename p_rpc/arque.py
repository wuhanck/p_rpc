#!/usr/bin/env python3
import socket

import asyncio

import arun

MAX_MSG = 130*1024


def _csock(cname):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    if (cname is not None):
        sock.bind(cname)
    sock.setblocking(False)
    return sock

def arque(bus_name, self_name, dequeue_cb):
    def _cname(peer_name):
        return '\0'+bus_name+'\0'+peer_name
    lock_ = asyncio.Lock()
    cname_ = _cname(self_name)
    sock_ = _csock(cname_)
    sock_.listen()
    chans_ = {}

    async def _serv_msg(msg):
        print(f'msg len: {len(msg)}')
        if (dequeue_cb is not None):
            await dequeue_cb(msg)

    async def _serv_recv(sock, peer_name):
        loop = arun.loop()
        try:
            while (True):
                msg = await loop.sock_recv(sock, MAX_MSG)
                if len(msg) == 0:
                    break
                await _serv_msg(msg)
        finally:
            chans_.pop(peer_name, None)
            sock.close()

    async def _handshake(sock, peer_name):
        loop = arun.loop()
        popchan = False
        closesock = True
        try:
            if (peer_name is None): # as target
                peer_name = await loop.sock_recv(sock, MAX_MSG)
                peer_name = peer_name.decode()
                print(f'target peer_name: {peer_name}')
                if (len(peer_name) == 0 or chans_.get(peer_name, None) is not None):
                    return
                chans_[peer_name] = sock
                popchan = True
                print(f'target self_name: {self_name}')
                await loop.sock_sendall(sock, self_name.encode())
                popchan = False
                closesock = False

            else: # as iniator
                await loop.sock_sendall(sock, self_name.encode())
                print(f'iniator self: {self_name}')
                check_name = await loop.sock_recv(sock, MAX_MSG)
                check_name = check_name.decode()
                print(f'iniator check_name: {check_name}')
                print(f'iniator peer_name: {peer_name}')
                if (check_name != peer_name):
                    return
                assert(chans_.get(peer_name, None) is None)
                chans_[peer_name] = sock
                closesock = False

            arun.post_in_task(_serv_recv(sock, peer_name))

        finally:
            if (popchan):
                chans_.pop(peer_name, None)
            if (closesock):
                sock.close()


    async def _serv_accept():
        loop = arun.loop()
        while (True):
            sock, _ = await loop.sock_accept(sock_)
            sock.setblocking(False)
            arun.post_in_task(_handshake(sock, None))

    arun.append_task(_serv_accept())

    async def _enqueue(peer_name, msg):
        if (peer_name == self_name):
            await _serv_msg(msg)
            return

        loop = arun.loop()
        async with lock_:
            sock = chans_.get(peer_name, None)
            if (sock is None):
                sock = _csock(None)
                await loop.sock_connect(sock, _cname(peer_name))
                await _handshake(sock, peer_name)
        await loop.sock_sendall(sock, msg)

    class inner:
        enqueue = _enqueue
        max_msg = MAX_MSG

    return inner


if __name__ == '__main__':
    chan1 = arque('test', 'ty', None)
    chan2 = arque('test', 'test1', None)

    msg = bytearray(MAX_MSG)

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
