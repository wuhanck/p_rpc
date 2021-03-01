#!/usr/bin/env python3

import lz4.frame
import json
import bson
import os
import sys

def f_test(*args, **kwargs):
    print(args)
    print(kwargs)

def f_test_json(*args, **kwargs):
    print(args)
    print(kwargs)
    jargs = json.dumps(args)
    jkwargs = json.dumps(kwargs)
    #print(jargs)
    #print(jkwargs)
    f_test(*json.loads(jargs), **json.loads(jkwargs))

def f_ret(*args, **kwargs):
    #return (*args, kwargs)
    #return '123'
    #return [1, 2, 3]
    #return 1, 4, 7, 0
    pass

def f_ret_json(*args, **kwargs):
    ret = f_ret(*args, **kwargs)
    if type(ret) is tuple:
        jret = json.dumps({"t": ret})
    else:
        jret = json.dumps({"d": ret})
    print(ret)
    #print(jret)

    ret_j = json.loads(jret)
    #print(ret_j)
    if 't' in ret_j:
        return (*ret_j['t'], )
    else:
        return ret_j['d']

f_test_json(1, 2, 3, a=100)
ret = f_ret_json(4, 5, 6, b=1, c=2)
print(ret)

def greeting(name: str) -> str:
    return 'Hello ' + name

def ftest(someid: str)->str:
    pass

greeting('1')
ftest(1)


in_data = 'abcdddddddddddd'*80
#c_data = lz4.frame.compress(in_data)
c_data = lz4.frame.compress(in_data.encode())
print(c_data)
d_data = lz4.frame.decompress(c_data).decode()
print(d_data)

kv = {}
##print(kv['not exist'])
kv.pop(None, None)
if 1 == 0:
    xxx = 1000
else:
    xxx = 10
print(xxx)

with open('orange-test', 'rb') as f:
    ret = f.read(1000)
    print(f'{ret}')


py_content = {'k': (1, 3, 4, [5, 6])} #bson need key
bson_thing = bson.dumps(py_content)
print(f'{bson_thing}')
py_thing = bson.loads(bson_thing)
print(f'{py_thing}')

# this is our descriptor object
class Bar(object):
    def __init__(self):
        self.static_value = ''
        self.value = ''
    def __get__(self, instance, owner):
        print(f"returned from descriptor object{self, instance, owner}")
        if instance is None:
            return self.static_value
        return self.value
    def __set__(self, instance, value):
        print(f"set in descriptor object{self, instance}")
        if instance is None:
            self.static_value = value
        else:
            self.value = value
    def __delete__(self, instance):
        print("deleted in descriptor object")
        del self.value

class Foo(object):
    bar = Bar()

f = Foo()
f2 = Foo()
f.bar = 10
print(f.bar)
print(f2.bar)
print(Foo.bar)
del f.bar

def f_many_args(x,  y, *, z, a=0, b=1, **kwargs):
    print(f'a:{a}, b:{b}')

f_many_args(7, 1, b=1, a=2, c=3, z=7)

sys.path.insert(0, os.path.abspath(__file__ + "/../../"))
print(f'paths:{sys.path}')