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

    async def handleAccountAndPositionResp(self, msg_dict: dict):
        asset_data = msg_dict['asset']

        # 更新资金
        asset_df = pd.DataFrame(asset_data)

        asset_df['S2'] = asset_df['S2'].astype(float)
        asset_df['S3'] = asset_df['S3'].astype(float)
        asset_df['S4'] = asset_df['S4'].astype(float)
        asset_df['S5'] = asset_df['S5'].astype(str)
        asset_df['S8'] = asset_df['S8'].replace('', 0.0).astype(float)

        bal = AccountBalance(
            balance=asset_df['S3'].sum(),
            cash_balance=asset_df.iloc[0]['S3'],
            cash_available=asset_df.iloc[0]['S4'],
            margin=0,
            unrealized_pnl=0,
            realized_pnl=0,
        )
        self.updateAccountBalance({'CNY': bal})

        # 更新持仓
        def _holding(group):
            ret = pd.Series()
            long = group[group['S5'] == "0"]
            if len(long) > 0:
                long = long.iloc[0]
                ret['long_holding'] = int(long['S2'])
                ret['long_available'] = int(long['S3'])
                ret['long_avg_cost'] = float(long['S4'])
                ret['long_market_value'] = float(long['S8'])
            else:
                ret['long_holding'] = 0
                ret['long_available'] = 0
                ret['long_avg_cost'] = 0
                ret['long_market_value'] = 0

            short = group[group['S5'] == "1"]
            if len(short) > 0:
                short = short.iloc[0]
                ret['short_holding'] = int(short['S2'])
                ret['short_available'] = int(short['S3'])
                ret['short_avg_cost'] = float(short['S4'])
                ret['short_market_value'] = float(short['S8'])
            else:
                ret['short_holding'] = 0
                ret['short_available'] = 0
                ret['short_avg_cost'] = 0
                ret['short_market_value'] = 0
            return ret

        holding_df = asset_df.iloc[1:].groupby(by='S1').apply(_holding)
        holdings = dict()
        for ticker, row in holding_df.iterrows():
            holding = AccountHolding(
                long_avg_cost=row['long_avg_cost'],
                long_holding=row['long_holding'],
                long_available=row['long_available'],
                long_profit=0,
                long_margin=0,
                long_market_value=row['long_market_value'],

                short_avg_cost=0,
                short_holding=row['short_holding'],
                short_available=row['short_available'],
                short_profit=0,
                short_margin=0,
                short_market_value=row['short_market_value'],
            )
            holdings[ticker] = holding

        self.updateAccountHoldings(holdings)

    def getOrderReq(self, order: LimitOrder):
        """
        FA	开多仓（开仓买入），适用于期货和期权
        FB	开空仓（开仓卖出），适用于期货和期权
        FC	平空仓（平仓买入），适用于期货和期权
        FD	平多仓（平仓卖出），适用于期货和期权
        FG	平今空（平今买入），适用于期货
        FH	平今多（平今卖出），适用于期货
        OA	备兑开仓，适用于期权
        OB	备兑平仓，适用于期权

        :param order:
        :return:
        """

        if order.isBuy():
            account = self.getAccountBalance()
            if account['CNY'].getMargin() < 0.5:
                action = "FA"  # 买入开仓

            else:
                holding = self.getAccountHolding(order.getTicker())
                if holding is None or holding.getShortAvailable() < order.getQuantity():
                    action = "FA"  # 买入开仓
                else:
                    action = "FC"

        else:
            account = self.getAccountBalance()
            if account['CNY'].getMargin() < 0.5:
                action = "FB"  # 卖出开仓

            else:
                holding = self.getAccountHolding(order.getTicker())
                if holding is None or holding.getLongAvailable() < order.getQuantity():
                    action = "FB"  # 卖出平仓
                else:
                    action = "FD"

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

