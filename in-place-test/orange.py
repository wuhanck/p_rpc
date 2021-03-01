#!/usr/bin/env python3

import os
import sys
import logging
import arun

sys.path.insert(0, os.path.abspath(__file__ + "/../../"))
import p_rpc
import redis_conf


orange_bus = p_rpc.init('in-place-orange-test', redis_conf)


@p_rpc.call_remote
async def kiwi_dec(bus, call_to, num):
    pass


@p_rpc.serve_remote
async def orange_dec(bus, call_from, num):
    print(f'call_from:{call_from} {num}')
    if num > 0:
        num = num-1
        return await kiwi_dec(bus, call_from, num)
    return num


@p_rpc.serve_remote
async def orange_get_file(bus, call_from, name, offset, length):
    print(f'call_from:{call_from}')
    with open(name, 'rb') as f:
        total = os.stat(name).st_size
        offset = f.seek(offset)
        content = f.read(length)

    return total, offset, content


#arun.run()
arun.run(logging.INFO)
