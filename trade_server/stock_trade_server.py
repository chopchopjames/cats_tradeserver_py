# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>

"""

import os
import pytz
import time
import typing
import asyncio
import orjson as json
import pandas as pd

from datetime import datetime
from xtrade_essential.utils.clients.async_support.redis import RedisClient
from xtrade_essential.utils import errors as trade_errors

from common.tradeHandler.async_server import AsyncBaseTradeServer, LimitOrder, EtfConvertRequest
from common.tradeHandler.data_models import AccountBalance, AccountHolding
from .utils import OPTION_TRAITS, STOCK_TRAITS, parseDatetimeStr


CN_TZ = pytz.timezone('Asia/Shanghai')


class MsgAction:
    INSERT_ORDER = "io"
    CANCEL_ORDER = 'del'


class TraderServer(AsyncBaseTradeServer):
    def __init__(self, hostname):
        AsyncBaseTradeServer.__init__(self, hostname=os.environ["TRADE_HOSTNAME"])
        self.getLogger().setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())

        self.__hostname = hostname
        self.__date_str = datetime.now().strftime('%Y%m%d')
        self.__req_id = 0
        self.__start_time = datetime.now()
        self.__start_cancel_count = 0
        self.__holdings = dict()
        self.__qry_order_result = list()

        self.__connector_sub_ch = os.environ["TRADE_REDIS_CONNECTOR_SUB_CH"]
        self.__connector_pub_ch = os.environ["TRADE_REDIS_CONNECTOR_PUB_CH"]
        self.__accountid = str(os.environ["TRADE_STOCK_ACCOUNTID"]) # 证券资金账号

        self.getLogger().info(f"connector_sub_ch: {self.__connector_sub_ch}, "
                              f"connector_pub_ch: {self.__connector_pub_ch}")

        self.__redis_client = RedisClient()

    def getReqId(self):
        self.__req_id += 1
        return self.__req_id

    def genCustOrderId(self):
        return f"{self.__start_time.strftime('%d%H%M%S')}-{self.getReqId()}"

    def handleAccountAndPositionResp(self, data):
        # 更新资金
        asset_df = pd.DataFrame(data)
        asset_df['S3'] = asset_df['S3'].astype(float)
        asset_df['S2'] = asset_df['S2'].astype(float)
        asset_df['S4'] = asset_df['S4'].astype(float)
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
        holdings = dict()
        for i, row in asset_df.iloc[1:].iterrows():
            ticker = row['S1']
            holding = AccountHolding(
                long_avg_cost=row['S4'],
                long_holding=row['S2'],
                long_available=row['S3'],
                long_profit=0,
                long_margin=0,
                long_market_value=row['S8'],

                short_avg_cost=0,
                short_holding=0,
                short_available=0,
                short_profit=0,
                short_margin=0,
            )
            holdings[ticker] = holding
        self.updateAccountHoldings(holdings)

    async def handleOrderUpdates(self, data):
        order_update_df = pd.DataFrame(data)
        order_update_df['ORD_STATUS'] = order_update_df['ORD_STATUS'].astype(str)
        order_update_df['ORD_NO'] = order_update_df['ORD_NO'].astype(str)

        for doc in order_update_df.itertuples():
            self.getLogger().info(doc)
            # 先看是不是ETF申赎
            etf_convert_req = self.getActEtfConvertByCustId(getattr(doc, 'CLIENT_ID'))
            if etf_convert_req is not None:
                if getattr(doc, "ORD_STATUS") == '2':
                    self.onEtfConvertResp(etf_convert_req)
                    continue

            # 再看是不是普通订单
            order = self.getOrderByCustId(getattr(doc, 'CLIENT_ID'))
            if order is None:
                self.getLogger().info(f"order not found: {doc}")
                continue

            if getattr(doc, "ORD_STATUS") == "0":
                if order.getState() == LimitOrder.State.SUBMITTED:
                    self.onOrderAcceptedResp(
                        exchange_order_ref=getattr(doc, 'ORD_NO'),
                        strategy_order_ref=order.getId(),
                        accepted_time=parseDatetimeStr(getattr(doc, 'ORD_TIME')),
                    )
                    continue

            elif getattr(doc, "ORD_STATUS") == "4":
                # 全部撤单
                self.onOrderCanceledResp(
                    order=order,
                    canceled_time=parseDatetimeStr(getattr(doc, 'ORD_TIME')),
                )
                continue

            elif getattr(doc, "ORD_STATUS") == "1":
                # 部分成交
                self.onExecInfo(
                    avg_fill_price=float(getattr(doc, 'AVG_PX')),
                    fill_quantity=int(getattr(doc, 'FILLED_QTY')),
                    dateTime=parseDatetimeStr(f"{getattr(doc, 'ORD_TIME')}"),
                    exchange_order_ref=order.getExchangeId(),
                    cost=0,
                )
                continue

            elif getattr(doc, "ORD_STATUS") == "2":
                # 全部成交
                self.onTrade(
                    ticker=order.getTicker(),
                    price=float(getattr(doc, 'AVG_PX')),
                    quantity=int(order.getRemaining()),
                    commission=0,
                    dateTime=parseDatetimeStr(f"{getattr(doc, 'ORD_TIME')}"),
                    exchange_trade_ref=order.getReqId() + f'{len(order.getAllExcutionInfo())}',
                    exchange_order_ref=order.getExchangeId(),
                    strategy_order_ref=order.getId(),
                )
                continue

            elif getattr(doc, "ORD_STATUS") == "3":
                # 部分撤单
                self.onOrderCanceledResp(
                    order=order,
                    canceled_time=parseDatetimeStr(getattr(doc, 'ORD_TIME')),
                )
                continue

            elif getattr(doc, "ORD_STATUS") == "5" or getattr(doc, "ORD_STATUS") == "6":
                self.onError(
                    type_=trade_errors.InvalidOrder.PROTO_CODE,
                    strategy_order_ref=order.getId(),
                    msg=getattr(doc, 'ERR_MSG'),
                )
                continue

    async def handleActOrderRes(self, data):
        actorder_df = pd.DataFrame(data)
        print(actorder_df)

        if self.__start_cancel_count <= 3:
            # start cancel
            to_cancel = list()
            for order_id, row in actorder_df.iterrows():
                order_time = parseDatetimeStr(getattr(row, 'ORD_TIME'))
                if order_time < self.__start_time:
                    self.getLogger().info("cancel order from last session")
                    to_cancel.append(order_id)

            if len(to_cancel) > 0:
                await self._cancelOrderReq(to_cancel)
                self.__start_cancel_count += 1

    async def monitorConnectorResp(self):
        await self.__redis_client.connect()

        sub_chs = await self.__redis_client.getTradeConn().subscribe(self.__connector_pub_ch)
        sub_ch = sub_chs[0]
        while not self.getTaskHandler().eof():
            try:
                msg_str = await sub_ch.get()
                if msg_str is None:
                    continue

                msg = json.loads(msg_str.decode())
                # print(msg)
                if msg['prefix'] == 'Asset':  # 持仓 & 资金
                    self.handleAccountAndPositionResp(msg['data'])

                elif msg['prefix'] == 'OrderUpdate':  # 委托回报
                    await self.handleOrderUpdates(msg['data'])

                elif msg['prefix'] == 'ActOrd':
                    await self.handleActOrderRes(msg['data'])

                else:
                    self.getLogger().info(f"unknown msg: {msg_str.decode()}")
                    continue

            except Exception as e:
                self.getLogger().info(f"failed msg: {msg_str.decode()}")

                # # TODO
                # self.stop()
                # raise e

    # TradeServer func start
    def getInstrumentTrait(self, ticker):
        ins_id, ex = ticker.split(".")
        if len(ins_id) == "6":
            return STOCK_TRAITS
        else:
            return OPTION_TRAITS

    async def sendMsgToRedis(self, action: str, data: typing.Union[dict, list]):
        if not self.__redis_client.connected:
            await self.__redis_client.connected()

        msg = {
            "action": action,
            "data": data
        }
        self.getLogger().info(f'sending: {msg}')
        await self.__redis_client.getTradeConn().publish_json(self.__connector_sub_ch, msg)

    async def sendLimitOrder(self, order: LimitOrder):
        """
        data = [
               [O,fads1,0,20800018203,不填,512010.SH,1,100,0.42,0],
               [O,<cust_id>,<柜台类型>,<账号>,<order_id>,<代码>,<买卖方向>,<qty>,<价格>,<委托类型>],
               ]

        :param order:
        :return:
        """
        if order.isBuy():
            action = '1'
        else:
            action = '2'

        order.setCustId(self.genCustOrderId())
        req = ('O',order.getCustId(),'0',self.__accountid,'',order.getTicker(),action,order.getQuantity(),order.getLimitPrice(),'0')

        await self.sendMsgToRedis(
            action=MsgAction.INSERT_ORDER,
            data=[req],
        )

    async def _cancelOrderReq(self, order_ids: list):
        req = list()
        for order_id in order_ids:
            req.append(('C','','0',self.__accountid,order_id))

        await self.sendMsgToRedis(
            action=MsgAction.CANCEL_ORDER,
            data=req,
        )

    async def cancelOrder(self, order: LimitOrder):
        """
        data = [
            [C,0,20800018203,0_20800018203_100100970806]
            [C,<柜台类型>,<client_id>,<order_no>]
        ]

        :param order:
        :return:
        """
        if order.getExchangeId() is not None:
            await self._cancelOrderReq([order.getExchangeId()])

    async def sendEtfConvert(self, etf_convert: EtfConvertRequest):
        """
        data = [
               [O,fads1,0,20800018203,不填,512010.SH,1,100,0.42,0],
               [O,<cust_id>,<柜台类型>,<账号>,<order_id>,<代码>,<买卖方向>,<qty>,<价格>,<委托类型>],
               ]
        :param etf_convert:
        :return:
        """
        if etf_convert.getAction() == EtfConvertRequest.Action.CREATE:
            action = "F"
        else:
            action = 'G'

        etf_convert.setCustId(self.genCustOrderId())
        req = ('O', etf_convert.getCustId(),'0',self.__accountid,'',etf_convert.getTicker(),action,int(etf_convert.getQuantity() * etf_convert.getMinExchangeUnit()),0,'0')
        self.registerActEtfConvert(etf_convert)

        await self.sendMsgToRedis(
            action=MsgAction.INSERT_ORDER,
            data=[req],
        )

    async def sendOrdersInBatch(self, batch_id, order_list: typing.List[LimitOrder]):
        batch_req = list()
        for order in order_list:
            if order.isBuy():
                action = '1'
            else:
                action = '2'

            order.setCustId(self.genCustOrderId())
            req = ('O', order.getCustId(), '0', self.__accountid, '', order.getTicker(), action, order.getQuantity(),
                   order.getLimitPrice(), '0')

            batch_req.append(req)

        await self.sendMsgToRedis(
            action=MsgAction.INSERT_ORDER,
            data=batch_req,
        )

    async def cancelOrdersInBatch(self, order_list: typing.List[LimitOrder]):
        await self._cancelOrderReq([order.getExchangeId() for order in order_list])

    async def qryActiveOrder(self):
        """定时更新"""
        pass

    async def qryAccountBalance(self):
        """定时更新"""
        pass

    async def qryAccountHolding(self):
        """定时更新"""
        pass

    async def login(self, *args, **kwargs):
        return True

    def run_forever(self):
        from common.async_http import HttpClient
        client = HttpClient()
        config = asyncio.run(client.getTradeAccount(hostname=self.__hostname))
        login_info = json.loads(config['login_info'])

        self.__accountid = str(login_info["account_id"])
        self.__connector_pub_ch = f"{login_info['connector_pub_ch']}|{self.__accountid}"
        self.__connector_sub_ch = login_info["connector_sub_ch"]

        self.getLogger().info(f"pub: {self.__connector_pub_ch}, sub: {self.__connector_sub_ch}")

        self.addCoroutineTask(self.monitorConnectorResp())

        super().run_forever()


if __name__ == '__main__':
    from .margin_trade_server import parse_args
    demo = TraderServer(*parse_args())

    demo.run_forever()

    time.sleep(60*60*24)

