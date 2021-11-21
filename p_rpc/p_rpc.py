#!/usr/bin/env python3
import logging
from asyncio import CancelledError, iscoroutinefunction
import traceback

from msgspec.core import decode, encode

from arun import future

from .arque import arque


_MAX_SERV_ID = (0x1 << 48)
_REQT_CALL = 0
_REQT_RET_DONE = 1
_REQT_RET_ERR = 2
_REQT_NOTIFY = -1


def init(bus_name, self_name):
    logger_ = logging.getLogger(f'{__name__}.{bus_name}.{self_name}')
    call_ = {}
    serv_ = {}
    serv_tag_ = 0
    chan_ = arque(bus_name, self_name)

    def _gen_tag():
        nonlocal serv_tag_
        serv_tag_ += 1
        if (serv_tag_ == _MAX_SERV_ID):
            serv_tag_ = 0
        return serv_tag_

    def _reg_serv(serv_func, serv_name=None):
        assert(iscoroutinefunction(serv_func))
        if (serv_name is None):
            serv_name = serv_func.__name__
        if (serv_.get(serv_name, None) is not None):
            raise Exception(f'serv {serv_name} registered')
        serv_[serv_name] = serv_func

    async def _call(peer_name, serv_name, *args, **kwargs):
        wait_ret = future()
        tag = _gen_tag()
        try:
            req = [_REQT_CALL, tag, serv_name, args, kwargs]
            logger_.debug(f'submit req {req}')
            req = encode(req)
            call_[tag] = wait_ret
            await chan_.enqueue(peer_name, req)
            ok, ret = await wait_ret
            if (not ok):
                logger_.info(f'call remote {serv_name} error {ret}')
                raise Exception(ret)
            return ret
        finally:
            call_.pop(tag, None)

    class inner_:
        reg_serv = _reg_serv
        call = _call

    async def _process(peer_name, req):
        try:
            req = decode(req)
            reqt, tag, *rest = req
        except Exception as e:
            logger_.warning(f'drop req from {peer_name}. error {repr(e)}')
            return

        logger_.debug(f'Starting _process {req}')
        if (reqt == _REQT_CALL):
            try:
                serv_name, args, kwargs = rest
                ret = await serv_[serv_name](inner_, peer_name, *args, **kwargs)
                ret = [_REQT_RET_DONE, tag, ret]
                ret = encode(ret)
            except CancelledError as e:
                logger_.warning(f'_process req {tag} call-from {peer_name} cancelled')
                raise
            except Exception as e:
                logger_.debug(f'_procss req {tag} error. trace {traceback.format_exc()}')
                ret = [_REQT_RET_ERR, tag, repr(e)]
                ret = encode(ret)
            await chan_.enqueue(peer_name, ret)

        elif (reqt == _REQT_RET_DONE):
            try:
                res, = rest
                wait_ret = call_.pop(tag, None)
                if wait_ret is not None:
                    wait_ret.set_result(True, res)
            except CancelledError as e:
                logger_.warning(f'_process {tag} (maybe returned) cancelled')
                raise
            except Exception as e:
                logger_.info(f'drop. req error {repr(e)}')

        elif (reqt == _REQT_RET_ERR):
            try:
                err, = rest
                wait_ret = call_.pop(tag, None)
                if wait_ret is not None:
                    wait_ret.set_result(False, err)
            except CancelledError as e:
                logger_.warning(f'_process {tag} (maybe returned) cancelled')
                raise
            except Exception as e:
                logger_.info(f'drop. req error {repr(e)}')

        elif (reqt == _REQT_NOTIFY):
            pass
        else:
            logger_.warning(f'_process {tag} unknown type {reqt}')

    chan_.cb(_process)

    return inner_


if __name__ == '__main__':
    pass
