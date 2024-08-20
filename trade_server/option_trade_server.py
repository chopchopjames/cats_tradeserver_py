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
        if 'fund' not in msg_dict:
            return

        fund_df = pd.DataFrame(msg_dict['fund'])
        fund_df['TOTEQUITY'] = fund_df['TOTEQUITY'].astype(float)
        fund_df['FETBALANCE'] = fund_df['FETBALANCE'].astype(float)

        # 更新资金
        bal = AccountBalance(
            balance=fund_df.iloc[0]["TOTEQUITY"],
            cash_balance=fund_df.iloc[0]["FETBALANCE"],
            cash_available=fund_df.iloc[0]["FETBALANCE"],
            margin=float(fund_df.iloc[0]["CURMARGIN"].replace('%', '')),
            unrealized_pnl=0,
            realized_pnl=0,
        )
        self.updateAccountBalance({'CNY': bal})

        # 更新持仓
        posi_df = pd.DataFrame(msg_dict['posi'])
        posi_df['CURRENTQTY'] = posi_df['CURRENTQTY'].astype(float)
        posi_df['ENABLEQTY'] = posi_df['ENABLEQTY'].astype(float)
        posi_df['COSTPRICE'] = posi_df['COSTPRICE'].astype(float)
        posi_df['MKTVALUE'] = posi_df['MKTVALUE'].astype(float)

        holdings = dict()
        for ticker, group in posi_df.groupby(by="SYMBOL"):
            long_ = group[group["DIRECTION"] == "0"]
            short_ = group[group["DIRECTION"] == "1"]

            holding = AccountHolding(
                long_avg_cost=long_['MKTVALUE'].sum() / max(long_['CURRENTQTY'].sum(), 1),
                long_holding=long_['CURRENTQTY'].sum(),
                long_available=long_['ENABLEQTY'].sum(),
                long_profit=0,
                long_margin=0,
                long_market_value=long_['MKTVALUE'].sum(),

                short_avg_cost=short_['MKTVALUE'].sum() / max(short_['CURRENTQTY'].sum(), 1),
                short_holding=short_['CURRENTQTY'].sum(),
                short_available=short_['ENABLEQTY'].sum(),
                short_profit=0,
                short_margin=0,
                short_market_value=short_['MKTVALUE'].sum(),
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

