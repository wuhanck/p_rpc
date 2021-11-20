#!/usr/bin/env python3
import logging
from asyncio import CancelledError, iscoroutinefunction
import traceback

from msgspec.core import decode, encode

from arun import future

from .arque import arque

MAX_SERV_ID = (0x1 << 48)

TASK_TYPE_CALL = 0
TASK_TYPE_RET = 1


def init(bus_name, self_name):
    logger_ = logging.getLogger(f'{__name__}.{bus_name}.{self_name}')
    call_ = {}
    serv_ = {}
    serv_id_ = 0
    chan_ = arque(bus_name, self_name)

    def _gen_id():
        nonlocal serv_id_
        serv_id_ += 1
        if (serv_id_ == MAX_SERV_ID):
            serv_id_ = 0
        return serv_id_

    def _reg_serv(func):
        assert(iscoroutinefunction(func))
        fname = func.__name__
        if (serv_.get(fname, None) is not None):
            raise Exception(f'serv {fname} registered')
        serv_[fname] = func

    async def _call(peer_name, fname, *args, **kwargs):
        wait_ret = future()
        task_id = _gen_id()
        try:
            task = [TASK_TYPE_CALL, task_id, fname, args, kwargs]
            logger_.debug(f'submit task {task}')
            task = encode(task)
            call_[task_id] = wait_ret
            await chan_.enqueue(peer_name, task)
            ok, ret = await wait_ret
            if (not ok):
                logger_.info(f'call remote {fname} error {ret}')
                raise Exception(ret)
            return ret
        finally:
            call_.pop(task_id, None)

    async def _process(peer_name, task_data):
        try:
            job = decode(task_data)
            task_type, task_id, *val = job
        except Exception as e:
            logger_.warning(f'drop task-data from {peer_name}. error {repr(e)}')
            return

        logger_.debug(f'Starting process {job}')
        if (task_type == TASK_TYPE_CALL):
            try:
                fname, args, kwargs = val
                ret = await serv_[fname](chan_, peer_name, *args, **kwargs)
                ret = [TASK_TYPE_RET, task_id, False, ret]
                ret = encode(ret)
            except CancelledError as e:
                logger_.warning(f'_process task {task_id} call-from {peer_name} cancelled')
                raise
            except Exception as e:
                logger_.debug(f'procss call-id {task_id} error. trace {traceback.format_exc()}')
                ret = [TASK_TYPE_RET, task_id, False, repr(e)]
                ret = encode(ret)
            await chan_.enqueue(peer_name, ret)
        elif (task_type == TASK_TYPE_RET):
            try:
                ok, ret = val
                wait_ret = call_.pop(task_id, None)
                if wait_ret is not None:
                    wait_ret.set_result(ok, ret)
            except CancelledError as e:
                logger_.warning(f'_process {task_id} (maybe returned) cancelled')
                raise
            except Exception as e:
                logger_.info(f'drop. job error {repr(e)}')
        else:
            logger_.warning(f'_process {task_id} unknown type {task_type}')

        chan_.cb(_process)

        class inner:
            reg_serv = _reg_serv
            call = _call

        return inner


if __name__ == '__main__':
    pass
