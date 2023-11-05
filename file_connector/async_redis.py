# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import json
import aioredis


class RedisClient(object):
    def __init__(self, host, port, pwd, page):
        self.host = host
        self.port = port
        self.pwd = pwd
        self.page = page

        self.connected = False

    async def connect(self):
        self.__r = await aioredis.create_redis_pool(
            address="redis://{}:{}".format(self.host, self.port),
            password=self.pwd,
            db=self.page,
            timeout=1,
        )

        self.connected = True

    async def getDepth(self, exchange, vendor, ticker):
        key = 'depth|||{}|||{}|||{}'.format(exchange, vendor, ticker)
        ret = await self.__quote_r.get(key)
        if ret:
            return json.loads(ret)

    async def getTradeServerSnap(self, hostname):
        key = 'ModuleSnap|||TradeServer|||{}'.format(hostname)
        ret = await self.__trade_r.get(key)
        if ret:
            return json.loads(ret)

    async def publish(self, channel: str, msg: dict):
        await self.__quote_r.publish_json(channel, msg)

    async def setSnapshot(self, module, id_, value, timeout=300):
        key = 'ModuleSnap|||{}|||{}'.format(module, id_)
        await self.__trade_r.set(key, json.dumps(value))
        await self.__trade_r.expire(key, timeout)