# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import json

from datetime import datetime

from xtrade_essential.xlib import xdb
from xtrade_essential.utils import quote


class QuoteRedisClient:
    def __init__(self):
        self.__quote_r = xdb.getQuoteRedis()

    def getSpotPrice(self, host, ticker):
        key = "tick|||{}|||{}".format(host, ticker)
        value = self.__quote_r.get(key)
        if value is None:
            key = "depth|||{}|||{}".format(host, ticker)
            value = self.__quote_r.get(key)
            if value is None:
                return
            else:
                value = json.loads(value)
                ret = (value["aps"][-1] + value["bps"][0]) / 2

        else:
            value = json.loads(value)
            ret = value["close"]

        return ret

    def getTick(self, host, ticker) -> quote.BasicTick:
        key = "tick|||{}|||{}".format(host, ticker)
        value = self.__quote_r.get(key)
        if value:
            value = json.loads(value)
            if "dt" in value:
                dt = datetime.fromtimestamp(value.get("dt"))
            elif "datetime" in value:
                dt = datetime.fromtimestamp(value.get("datetime"))
            else:
                return

            tick = quote.BasicTick(
                trading_session=value.get("trading_session", quote.TradingPhase.TRADING),
                dateTime=dt,
                open_=value.get('open', 0),
                high=value.get('high', 0),
                low=value.get('low', 0),
                close=value.get('new_price', 0),
                volume=value.get('volume', 0),
                amount=value.get('amount', 0),
                bp=list(value.get('bps', list())),
                bv=list(value.get('bvs', list())),
                ap=list(value.get('aps', list())),
                av=list(value.get('avs', list())),
                preclose=value.get("preclose", 0),
                bought_amount=value.get('bought_amount', None),
                sold_amount=value.get("sold_amount"),
                upper_limit=value.get("upper_limit"),
                lower_limit=value.get("lower_limit"),
                extra=value,
                bought_volume=value.get("bought_qty"),
                sold_volume=value.get("sold_qty")
            )
            return tick


def setLogicSuper(trader_id, logic_id, value):
    r = xdb.getTradeRedis()
    key = 'LogicSuper|||{}|||{}'.format(trader_id, logic_id)
    r.set(key, json.dumps(value))
    r.expire(key, 60 * 60 * 24 * 365)


def getLogicSuper(trader_id, logic_id):
    r = xdb.getTradeRedis()
    key = 'LogicSuper|||{}|||{}'.format(trader_id, logic_id)
    ret = r.get(key)
    if ret:
        return json.loads(ret)


def setLogicPara(trader_id, logic_id, value):
    r = xdb.getTradeRedis()
    key = 'LogicPara|||{}|||{}'.format(trader_id, logic_id)
    r.set(key, json.dumps(value))
    r.expire(key, 60 * 60 * 24 * 30)


def getLogicPara(trader_id, logic_id):
    r = xdb.getTradeRedis()
    key = 'LogicPara|||{}|||{}'.format(trader_id, logic_id)
    ret = r.get(key)
    if ret:
        return json.loads(ret)


def getSpreadLogicByTrader(trader_id):
    ret = list()

    r = xdb.getTradeRedis()
    for key in r.scan_iter('LogicPara|||{}*'.format(trader_id)):
        ret.append(json.loads(r.get(key)))

    return ret


def getSnapshot(module, id_):
    r = xdb.getTradeRedis()
    key = 'ModuleSnap|||{}|||{}'.format(module, id_)
    ret = r.get(key)
    if ret:
        return json.loads(ret)


def setSnapshot(module, id_, value, timeout=300):
    r = xdb.getTradeRedis()
    key = 'ModuleSnap|||{}|||{}'.format(module, id_)
    r.set(key, json.dumps(value))
    r.expire(key, timeout)


def getAllSpreadTraderSnap():
    ret = list()

    r = xdb.getTradeRedis()
    for key in r.scan_iter('ModuleSnap|||SpreadTrader*'):
        ret.append(json.loads(r.get(key)))

    return ret


def getAllTradeServerSnap():
    ret = dict()

    r = xdb.getTradeRedis()
    for key in r.scan_iter('ModuleSnap|||TradeServer|||*'):
        ret[key] = json.loads(r.get(key))

    return ret


def setTick(hostname, ticker, quote, timeout=60*48):
    r = xdb.getQuoteRedis()
    key = 'Tick|||{}|||{}'.format(hostname, ticker)
    r.set(key, json.dumps(quote))
    r.expire(key, timeout)


def getTick(hostname, ticker):
    r = xdb.getQuoteRedis()
    key = 'tick|||{}|||{}'.format(hostname, ticker)
    ret = r.get(key)
    if ret:
        return json.loads(ret)


def setDepth(hostname, ticker, quote, timeout=60*48):
    r = xdb.getQuoteRedis()
    key = 'depth|||{}|||{}'.format(hostname, ticker)
    r.set(key, json.dumps(quote))
    r.expire(key, timeout)


def getDepth(hostname, ticker):
    r = xdb.getQuoteRedis()
    key = 'depth|||{}|||{}'.format(hostname, ticker)
    ret = r.get(key)
    if ret:
        return json.loads(ret)


def setTradeDetail(hostname, ticker, quote, timeout=60*48):
    r = xdb.getQuoteRedis()
    key = 'TradeDetail|||{}|||{}'.format(hostname, ticker)
    r.set(key, json.dumps(quote))
    r.expire(key, timeout)


def getTradeDetail(hostname, ticker):
    r = xdb.getQuoteRedis()
    key = 'TradeDetail|||{}|||{}'.format(hostname, ticker)
    ret = r.get(key)
    if ret:
        return json.loads(ret)


def _getSpotPrice(market, broker, base_symbol, quote_symbol):
    r = xdb.getQuoteRedis()
    key = 'SpotPrice|||{}|||{}|||{}/{}'.format(market, broker, base_symbol, quote_symbol)
    ret = r.get(key)

    if ret:
        return json.loads(ret)


def getSpotPrice(market, broker, base_symbol, quote_symbol):
    spot_price = _getSpotPrice(market, broker, base_symbol, quote_symbol)
    if spot_price:
        return spot_price['price']

    spot_price = _getSpotPrice(market, broker, quote_symbol, base_symbol)
    if spot_price:
        return 1 / spot_price['price']


def setSpotPrice(market, broker, base_symbol, quote_symbol, quote, timeout=60*48):
    r = xdb.getQuoteRedis()
    key = 'SpotPrice|||{}|||{}|||{}/{}'.format(market, broker, base_symbol, quote_symbol)
    r.set(key, json.dumps(quote))
    r.expire(key, timeout)


def setMinuteBar(hostname, ticker, quote, timeout=60*48):
    r = xdb.getQuoteRedis()
    key = 'MinuteBar|||{}|||{}'.format(hostname, ticker)
    r.set(key, json.dumps(quote))
    r.expire(key, timeout)


def getMinuteBar(hostname, ticker):
    r = xdb.getQuoteRedis()
    key = 'MinuteBar|||{}|||{}'.format(hostname, ticker)
    ret = r.get(key)
    if ret:
        return json.loads(ret)