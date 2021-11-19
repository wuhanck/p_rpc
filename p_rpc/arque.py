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
        return '\0local\0'+bus_name+'\0'+peer_name

    lock_ = asyncio.Lock()
    cname_ = _cname(self_name)
    sock_ = _csock(cname_)
    sock_.listen()
    ichans_ = {}
    tchans_ = {}

    async def _serv_msg(msg):
        try:
            print(f'len:{len(msg)}')
            await dequeue_cb(msg)
        except:
            pass

    async def _serv_recv(sock, chans, peer_name):
        loop = arun.loop()
        try:
            while (True):
                msg = await loop.sock_recv(sock, MAX_MSG)
                if len(msg) == 0:
                    break
                await _serv_msg(msg)
        finally:
            chans.pop(peer_name, None)
            sock.close()

    async def _handshake(sock, peer_name):
        loop = arun.loop()
        popchan = False
        closesock = True
        chans = tchans_
        try:
            if (peer_name is None): # as target
                peer_name = await loop.sock_recv(sock, MAX_MSG)
                if (len(peer_name) == 0):
                    return
                peer_name = peer_name.decode()
                if (chans.get(peer_name, None) is not None):
                    return
                chans[peer_name] = sock
                popchan = True
                await loop.sock_sendall(sock, self_name.encode())
                popchan = False
                closesock = False

            else: # as iniator
                chans = ichans_
                await loop.sock_connect(sock, _cname(peer_name))
                await loop.sock_sendall(sock, self_name.encode())
                check_name = await loop.sock_recv(sock, MAX_MSG)
                if (len(check_name) == 0):
                    return
                check_name = check_name.decode()
                if (check_name != peer_name):
                    return
                assert(chans.get(peer_name, None) is None)
                chans[peer_name] = sock
                closesock = False

            arun.post_in_task(_serv_recv(sock, chans, peer_name))

        finally:
            if (popchan):
                chans.pop(peer_name, None)
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
        loop = arun.loop()
        async with lock_:
            sock = tchans_.get(peer_name, None)
            if (sock is None):
                sock = ichans_.get(peer_name, None)
            if (sock is None):
                sock = _csock(None)
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
