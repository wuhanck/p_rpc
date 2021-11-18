#!/usr/bin/env python3
import logging
from asyncio import CancelledError, TimeoutError
from functools import wraps
import traceback

import mspspec

from arun import append_task, append_cleanup, run, future, sleep, post_in_task, timeout

from .arque import arque


_logger = logging.getLogger(__name__)
_call_remote = {}
_serve_remote = {}


async def _cleanup(bus):
    pass


def _get_future():
    return future()


def call_remote(func):
    @wraps(func)
    async def wrapped(bus, remote_name, *args, **kwargs):
        await func(bus, remote_name, *args, **kwargs)  # Just check param or you can inspect in func
        wait_ret = _get_future()
        task_id = None
        ret = None
        try:
            task = {'call': func.__name__,
                    'from': bus._local_name,
                    'args': args,
                    'kwargs': kwargs}
            _logger.debug(f'submit task {task}')
            task = bson.dumps(task)
            task_id = Arque.gen_task_id()
            _call_remote[task_id] = wait_ret
            await _call_enqueue(bus, remote_name, task, task_id)
            ret = await wait_ret
            if 'e' in ret:
                e = ret['e']
                _logger.info(f'call remote {func.__name__} error {e}')
                raise Exception(e)
            return (*ret['t'], ) if 't' in ret else ret['d']
        finally:
            _call_remote.pop(task_id, None)

    return wrapped


async def _process(bus, task_id, task_data):
    ret = None
    job = None
    try:
        job = bson.loads(task_data)
    except Exception as e:
        _logger.warning(f'drop. task-data error {repr(e)}')
        return

    _logger.debug(f'Starting process {job}')
    if 'call' in job:
        call_from = None
        try:
            call_from = job['from']
            call_name = job['call']
            args = job['args']
            kwargs = job['kwargs']
            ret = await _serve_remote[call_name](bus, call_from, *args, **kwargs)
            ret = {'t': ret, 'call-id': task_id} if type(ret) is tuple else {'d': ret, 'call-id': task_id}
            ret = bson.dumps(ret)
        except CancelledError as e:
            _logger.warning(f'_process task {task_id} call-from {call_from} cancelled')
            call_from = None
            raise e
        except Exception as e:
            _logger.debug(f'procss call-id {task_id} error. trace {traceback.format_exc()}')
            ret = {'e': repr(e), 'call-id': task_id}
            ret = bson.dumps(ret)
        finally:
            if call_from is not None:
                await _call_enqueue(bus, call_from, ret, Arque.gen_task_id())
    else:
        try:
            task_id = job['call-id']
            wait_ret = _call_remote.pop(task_id, None)
            if wait_ret is not None:
                wait_ret.set_result(job)
        except CancelledError as e:
            _logger.warning(f'_process {task_id} (maybe returned) cancelled')
            raise e
        except Exception as e:
            _logger.info(f'drop. job error {repr(e)}')


def serve_remote(func):
    _serve_remote[func.__name__] = func
    @wraps(func)
    def wrapped():
        assert('@serve_remote func' == 'not callable directly')

    return wrapped


async def _consume_task(bus, redis):
    _logger.info('Starting consuming...')
    queue = Arque(redis, bus._local_name)
    while True:
        task_id = None
        task_data = None
        async with timeout(_dequeue_timeout):
            task_id, task_data = await queue.dequeue(_dequeue_timeout//3)

        if task_id is None:
            continue

        if task_id == '__not_found__':
            _logger.debug(f'TASK ID: {task_id}')
            continue

        if task_id == '__overloaded__':
            _logger.info(f'TASK ID: {task_id}')
            await sleep(1)
            continue

        if task_id == '__marked_as_failed___':
            _logger.info(f'TASK  ID: {task_id}')
            continue

        async def _do_consume_task(bus, task_id, task_data):
            try:
                await _process(bus, task_id, task_data)
            finally:
                await _release_queue_task(bus, bus._local_name, task_id)

        post_in_task(_do_consume_task(bus, task_id, task_data))


def init(bus, name):
    class _bus:
        _local_name = name
        _redis_conf = redis_conf

    append_task(_consume_task(_bus))
    append_cleanup(_cleanup(_bus))
    return _bus


if __name__ == '__main__':
    @serve_remote
    async def test():
        pass
    # test()

    @call_remote
    async def test_call(remote_name):
        pass

    bus = init('kiwi', {'host': '127.0.0.1',
                  'port': 6379,
                  'password': '123456',
                  'db': 1})

    append_task(test_call(bus, 'orange'))
    print(_serve_remote)
    print(_call_remote)
    run()
