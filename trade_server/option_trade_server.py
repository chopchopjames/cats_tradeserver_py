# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>

"""

import os
import pytz
import time
import typing
import orjson as json
import pandas as pd

from datetime import datetime
from xtrade_essential.utils.clients.async_support.redis import RedisClient
from xtrade_essential.utils import errors as trade_errors

from common.tradeHandler.async_server import AsyncBaseTradeServer, LimitOrder, EtfConvertRequest
from common.tradeHandler.data_models import AccountBalance, AccountHolding
from .utils import OPTION_TRAITS, STOCK_TRAITS, parseDatetimeStr
from .stock_trade_server import TraderServer as StockTraderServer


CN_TZ = pytz.timezone('Asia/Shanghai')

ACCT_TYPE = "0B"


class MsgAction:
    INSERT_ORDER = "io"
    CANCEL_ORDER = 'del'


class TraderServer(StockTraderServer):
    def __init__(self, hostname):
        StockTraderServer.__init__(self, hostname)

    def getOrderReq(self, order: LimitOrder):
        if order.isBuy():
            account = self.getAccountBalance()
            if account['CNY'].getMargin() < 0.7:
                action = "FA"  # 买入开仓

            else:
                holding = self.getAccountHolding(order.getTicker())
                if holding is None or holding.getShortAvailable() >= order.getQuantity():
                    action = "FD"  # 买入平仓
                else:
                    action = "FA"

        else:
            account = self.getAccountBalance()
            if account['CNY'].getMargin() < 0.7:
                action = "FB"  # 卖出开仓

            else:
                holding = self.getAccountHolding(order.getTicker())
                if holding is None or holding.getLongAvailable() >= order.getQuantity():
                    action = "FD"  # 卖出平仓
                else:
                    action = "FB"

        req = ('O',order.getCustId(),'0B',self.getAccountId(),'',order.getTicker(),action,order.getQuantity(),order.getLimitPrice(),'0')

        return req

    async def cancelOrderReq(self, order_ids: list):
        req = list()
        for order_id in order_ids:
            req.append(('C','','0B',self.getAccountId(),order_id))

        await self.sendMsgToRedis(
            action=MsgAction.CANCEL_ORDER,
            data=req,
        )



if __name__ == '__main__':
    from .stock_trade_server import parse_args

    demo = TraderServer(*parse_args())
    demo.run_forever()
    time.sleep(60*60*24)

