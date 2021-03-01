#!/usr/bin/env python3

import os
import sys
import logging
import p_rpc
import arun

import redis_conf


orange_bus = p_rpc.init('orange-test', redis_conf)


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


#arun.run()
arun.run(logging.INFO)
