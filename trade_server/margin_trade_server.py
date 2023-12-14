# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>

"""

import time

from common.tradeHandler.async_server import AsyncBaseTradeServer, LimitOrder, EtfConvertRequest

from .stock_trade_server import MsgAction, CN_TZ, TraderServer, parse_args



class CreditTraderServer(TraderServer):
    def __init__(self, hostname: str):
        TraderServer.__init__(self, hostname)

    def getOrderReq(self, order: LimitOrder):
        if order.isBuy():
            action = 'E'  # 先买券还券，再买担保品

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



