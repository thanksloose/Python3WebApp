# !/usr/bin/env python3
# coding = utf-8

__author__ = 'hmm'

import logging;

logging.basicConfig(level=logging.INFO)
import asyncio, os, json, time
from datetime import datetime
from aiohttp import web


# 处理url
def index(request):
    return web.Response(body=b'<h1>python3 webapp</h1>', content_type='text/html', charset='utf-8')


# @asyncio.coroutine把一个generator标记为coroutine类型，然后把这个coroutine扔到EventLoop中执行
@asyncio.coroutine
def init(loop):
    # 创建web服务器实例
    app = web.Application(loop=loop)
    # 将处理函数index注册到创建的app.router中
    app.router.add_route('GET', '/', index)
    # 用协程创建监听服务 loop为传入函数的协程 yield from 返回一个创建好的，绑定IP、端口、HTTP协议簇的监听服务的协程
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000')
    return srv


# 创建协程
loop = asyncio.get_event_loop()
# 运行协程
loop.run_until_complete(init(loop))
loop.run_forever()
