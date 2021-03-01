#!/usr/bin/env python3

import os
import sys
import logging
import asyncio
import p_rpc
import arun

import redis_conf


kiwi6_bus = p_rpc.init('kiwi-test6', redis_conf)


@p_rpc.call_remote
async def orange_dec(bus, call_to, num):
    pass


@p_rpc.serve_remote
async def kiwi_dec(bus, call_from, num):
    print(f'call_from:{call_from} {num}')
    if num > 0:
        num = num-1
        return await orange_dec(bus, call_from, num)
    return num


async def test_remote_recur_call():
    while True:
        await asyncio.sleep(6)
        await orange_dec(kiwi6_bus, 'orange-test', 6)


arun.append_task(test_remote_recur_call())

arun.run(logging.WARNING)
#arun.run()
