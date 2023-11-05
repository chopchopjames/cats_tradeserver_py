# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import json
import aioredis
from datetime import datetime

from xtrade_essential.xlib import xdb
from xtrade_essential.utils.quote import BasicTick, TradingPhase


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()


class RedisClient(object):
    def __init__(self):
        self.__quote_r = None
        self.__trade_r = None

        self.connected = False

    def getTradeConn(self):
        return self.__trade_r

    def getQuoteConn(self):
        return self.__quote_r

    async def getTick(self, host, ticker) -> BasicTick:
        key = "tick|||{}|||{}".format(host, ticker)
        value = await self.__quote_r.get(key)
        if value:
            value = json.loads(value)
            timestamp = value.get("datetime")
            if timestamp is None:
                timestamp = value.get("dt")
            tick = BasicTick(
                trading_session=value.get("trading_session", TradingPhase.TRADING),
                dateTime=datetime.fromtimestamp(timestamp),
                open_=value.get('open', 0),
                high=value.get('high', 0),
                low=value.get('low', 0),
                close=value.get('close', 0),
                volume=value.get('volume', 0),
                amount=value.get('amount', 0),
                bp=list(value.get('bps', list())),
                bv=list(value.get('bvs', list())),
                ap=list(value.get('aps', list())),
                av=list(value.get('avs', list())),
                preclose=value.get("preclose", 0),
                bought_amount=value.get('bought_amount', None),
                sold_amount=value.get("sold_amount"),
                bought_volume=value.get("bought_qty"),
                sold_volume=value.get("sold_qty"),
                upper_limit=value.get("upper_limit"),
                lower_limit=value.get("lower_limit"),
                extra=value,
            )
            return tick

    async def getSpotPrice(self, host, ticker):
        key = "tick|||{}|||{}".format(host, ticker)
        value = await self.__quote_r.get(key)
        if value is None:
            key = "depth|||{}|||{}".format(host, ticker)
            value = await self.__quote_r.get(key)
            if value is None:
                return
            else:
                value = json.loads(value)
                ret = (value["aps"][-1] + value["bps"][0]) / 2

        else:
            value = json.loads(value)
            ret = value["close"]

        return ret

    async def connect(self):
        self.__quote_r = await aioredis.create_redis_pool(
            address="redis://{}:{}".format(xdb.REDIS_HOST, xdb.REDIS_PORT),
            password=xdb.REDIS_PWD,
            db=xdb.QUOTE_PAGE,
            timeout=5,
        )

        self.__trade_r = await aioredis.create_redis_pool(
            address="redis://{}:{}".format(xdb.REDIS_HOST, xdb.REDIS_PORT),
            password=xdb.REDIS_PWD,
            db=xdb.TRADE_PAGE,
            timeout=5,
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
        await self.__trade_r.set(key, json.dumps(value, default=myconverter))
        await self.__trade_r.expire(key, timeout)