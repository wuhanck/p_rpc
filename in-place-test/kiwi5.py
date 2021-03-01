#!/usr/bin/env python3

import os
import sys
import logging
import asyncio
import arun

sys.path.insert(0, os.path.abspath(__file__ + "/../../"))
import p_rpc
import redis_conf


kiwi5_bus = p_rpc.init('in-place-kiwi-test5', redis_conf)


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
        await asyncio.sleep(5)
        await orange_dec(kiwi5_bus, 'in-place-orange-test', 5)


arun.append_task(test_remote_recur_call())

arun.run(logging.WARNING)
#arun.run()
