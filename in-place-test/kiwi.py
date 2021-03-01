#!/usr/bin/env python3

import os
import sys
import logging
import asyncio
import arun

sys.path.insert(0, os.path.abspath(__file__ + "/../../"))
import p_rpc
import redis_conf


kiwi_bus = p_rpc.init('in-place-kiwi-test', redis_conf)


@p_rpc.call_remote
async def orange_get_file(bus, remote_name, name , offset, length):
    pass


async def test_remote_call():
    total = offset = content = None
    await asyncio.sleep(10)
    try:
        total, offset, content = await orange_get_file(kiwi_bus, 'in-place-orange-test', 'orange-test', 5, 100)
    except Exception as e:
        print(f'{e}')
    finally:
        pass
    print(f'{total}, {offset}, {content}')


arun.append_task(test_remote_call())

arun.run(logging.WARNING)
#arun.run()
