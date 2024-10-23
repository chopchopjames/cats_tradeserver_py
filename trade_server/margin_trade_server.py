# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>

"""

import time
import pandas as pd

from common.tradeHandler.async_server import LimitOrder, EtfConvertRequest, AccountHolding, AccountBalance

from .stock_trade_server import MsgAction, CN_TZ, TraderServer, parse_args


class CreditTraderServer(TraderServer):
    def __init__(self, hostname: str):
        TraderServer.__init__(self, hostname)

    async def handleAccountAndPositionResp(self, msg_dict: dict):
        if 'fund' not in msg_dict:
            return

        # 更新资金
        fund_df = pd.DataFrame(msg_dict['fund'])
        bal = AccountBalance(
            balance=float(fund_df['NETASSET'].iloc[0]),
            cash_balance=float(fund_df['CASHASSET'].iloc[0]),
            cash_available=float(fund_df['CASHASSET'].iloc[0]),
            margin=0,
            unrealized_pnl=0,
            realized_pnl=0,
            cash_borrowable=float(fund_df['RZENABLEQT'].iloc[0]),
        )
        self.updateAccountBalance({'CNY': bal})

        # 多头持仓
        posi_df = pd.DataFrame(msg_dict['posi'])
        posi_df['long_holding'] = posi_df['CURQTY'].astype(float)
        posi_df['long_available'] = posi_df['ENABLEQTY'].astype(float)
        posi_df['long_market_value'] = posi_df['MKTVALUE'].astype(float)
        posi_df = posi_df.set_index('SYMBOL')[['long_holding', 'long_available', 'long_market_value']]

        # 融券可用
        rq_data = msg_dict['rq']
        if len(rq_data) > 0:
            rq_df = pd.DataFrame(rq_data).set_index("SYMBOL")
        else:
            rq_df = pd.DataFrame([], columns=['SYMBOL', 'QTY'])
            rq_df = rq_df.set_index('SYMBOL')

        rq_df['margin_sell_available'] = rq_df['QTY'].astype(int)

        # 空头持仓
        compact_data = msg_dict['compact']
        if len(compact_data) > 0:
            compact_df = pd.DataFrame(compact_data).set_index('STOCKCODE')
            compact_df['CMPTYPE'] = compact_df['CMPTYPE'].astype(str)
            compact_df = compact_df[compact_df['CMPTYPE'] == '1']  # 融券
            compact_df["RCMAMOUNT"] = compact_df["RCMAMOUNT"].astype(int)
            grouped = compact_df.groupby('STOCKCODE')
            compact_summary = pd.DataFrame()
            compact_summary['short_holding'] = grouped['RCMAMOUNT'].sum()  # 未还数量

        else:
            compact_summary = pd.DataFrame([], columns=['STOCKCODE', 'short_holding'])
            compact_summary = compact_summary.set_index('STOCKCODE')

        rqall_df = pd.merge(compact_summary, rq_df, left_index=True, right_index=True, how='outer')
        rqall_df = rqall_df[['short_holding', 'margin_sell_available']]

        holding_df = pd.merge(posi_df, rqall_df, left_index=True, right_index=True, how='outer')
        holding_df.fillna(0, inplace=True)

        holdings = dict()
        for ticker, row in holding_df.iterrows():
            holding = AccountHolding(
                long_avg_cost=row['long_market_value']/max(row['long_holding'], 1),
                long_holding=row['long_holding'],
                long_available=row['long_available'],
                long_market_value=row['long_market_value'],
                long_profit=0,
                long_margin=0,

                short_avg_cost=0,
                short_holding=int(getattr(row, "short_holding")),
                short_available=int(getattr(row, "short_holding")),
                margin_sell_available=int(getattr(row, "margin_sell_available")),
                short_profit=0,
                short_margin=0,
            )

            holdings[ticker] = holding

        self.updateAccountHoldings(holdings)

    def getOrderReq(self, order: LimitOrder):
        """
        A	融资买入
        B	融券卖出
        C	买券还券
        D	卖券还款
        E	先买券还券，再担保品买入

        :param order:
        :return:
        """

        if order.isBuy():
            position = self.getAccountHolding(order.getTicker())
            balance = self.getAccountBalance()['CNY']
            if position is not None and position.getShortAvailable() >= order.getQuantity():
                action = 'C'  # 买券还券
            elif order.getQuantity() * order.getLimitPrice() < balance.getCashAvailable():
                action = "1"  # 买担保品
            else:
                action = 'A'

        else:
            holding = self.getAccountHolding(order.getTicker())
            if holding is not None and holding.getLongAvailable() >= order.getQuantity():
                action = "2"  # 担保品卖出
            else:
                action = "B"

        req = ('O',order.getCustId(),'C',self.getAccountId(),'',order.getTicker(),action,order.getQuantity(),order.getLimitPrice(),'0')

        return req

    async def cancelOrderReq(self, order_ids: list):
        req = list()
        for order_id in order_ids:
            req.append(('C','','C',self.getAccountId(),order_id))

        await self.sendMsgToRedis(
            action=MsgAction.CANCEL_ORDER,
            data=req,
        )

    async def sendEtfConvert(self, etf_convert: EtfConvertRequest):
        raise NotImplemented()


if __name__ == '__main__':
    demo = CreditTraderServer(*parse_args())
    demo.run_forever()
    time.sleep(60*60*24)



