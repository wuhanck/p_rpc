#!/usr/bin/env python3
import arun

import p_rpc as rpc

print(dir(rpc))

async def serv1(chan, peer_name):
    print(f'chan:{chan} from: {peer_name}. serv1 called.')
    pass

async def serv2(chan, peer_name, err_str):
    print(f'chan:{chan} from: {peer_name}. serv2 called.')
    raise Exception(err_str)

peer1 = rpc.init('test-bus', 'test1')
peer1.reg_serv(serv1)
peer1.reg_serv(serv2)

peer2 = rpc.init('test-bus', 'test2')

async def _test2_call_test1_serv1():
    ret = await peer2.call('test1', 'serv1')
    print(f'_test2_call_test1_serv1 ret: {ret}')
    ret = await peer2.call('test1', 'serv2', 'my fault')
    print(f'exception happens. no print this {ret}')

arun.append_task(_test2_call_test1_serv1())

arun.run()
