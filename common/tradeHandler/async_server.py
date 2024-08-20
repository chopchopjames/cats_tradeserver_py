## coding: utf8

"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
1. 接受req，pub回报
2. 通过zmq转发req到recorder
"""

import abc
import json
import uuid
import typing
import asyncio

import numpy as np

from datetime import datetime

from . import zmqClient
from .data_models import LimitOrder, QryOrderResult, AccountBalance, AccountHolding

from xtrade_essential.utils.order import EtfConvertRequest, Order, OrderExecutionInfo
from xtrade_essential.utils.clients import trade_client, http
from xtrade_essential.utils.errors import *
from xtrade_essential.utils.taskhandler import async_task
from xtrade_essential.proto import trade_pb2
from xtrade_essential.xlib import protobuf_to_dict
from xtrade_essential.xlib import logger


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.int64):
            return int(obj)
        return super(CustomEncoder, self).default(obj)


class AsyncBaseTradeServer(object):
    def __init__(self, hostname):
        self.__hostname = hostname

        self.__traits = dict()

        self.__last_order_time = datetime.now().timestamp()
        self.__last_cancel_time = datetime.now()

        self.__active_orders = dict()
        self.__filled_order_ids = set()
        self.__batch_mapping = dict() # {<strategy_ref>: <batch_id>}
        self.__order_id_mapping = dict() # {<exchange_ref>: <strategy_ref>}, 这个一定要是全量
        self.__cust_order_id_mapping = dict()

        self.__account_holdings = dict()
        self.__account_balance = None
        self.__account_balance_update_time = datetime(1992, 7, 23)
        self.__account_holding_update_time = datetime(1992, 7, 23)

        self.__hostname = hostname
        self.__logger = logger.getFileLogger(hostname,
                                             log_dir='./logs',
                                             log_file=f"{hostname}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log")

        self.__async_task_h = async_task.TaskHandler()

        self.__logic_order_counts = dict()

        # risk related
        self.__order_volume_cumsum_ticker = dict()
        self.__order_volume_limit_ticker = dict()

        # etf convert
        self.__act_etf_convert = dict()
        self.__act_etf_convert_cust_id = dict()

        # self trading
        self.__max_buy_order_prices = dict()  # {<ticker>: <price>}
        self.__min_sell_order_prices = dict()  # {<ticker>: <price>}

        # pending orders from other session
        self.__qry_order_res = list()
        self.__order_cancel_time = dict()

        # trade server start time
        self.__start_time = datetime.now()

        # 统计
        self.__order_num = 0
        self.__trade_num = 0
        self.__error_num = 0

    def getAccount(self):
        return self.__account_name

    def getQryOrderResults(self) -> typing.List[QryOrderResult]:
        return self.__qry_order_res

    def getActEtfConvert(self, strategy_order_ref: str):
        return self.__act_etf_convert.get(strategy_order_ref)

    def getActEtfConvertByCustId(self, cust_id: str):
        return self.__act_etf_convert_cust_id.get(cust_id)

    def getAllActEtcConverts(self):
        return self.__act_etf_convert_cust_id

    def registerActEtfConvert(self, etf_convert: EtfConvertRequest):
        if etf_convert.getReqId():
            self.__act_etf_convert[etf_convert.getReqId()] = etf_convert

        if etf_convert.getCustId():
            self.__act_etf_convert_cust_id[etf_convert.getCustId()] = etf_convert

        self.getLogger().info("etf convert request registered, req_id: {}, cust_id: {}".format(etf_convert.getReqId(),
                                                                                               etf_convert.getCustId()))

    def unRegisterActEtfConvert(self, etf_convert: EtfConvertRequest):
        if etf_convert.getReqId() in self.__act_etf_convert:
            del self.__act_etf_convert[etf_convert.getReqId()]

        if etf_convert.getCustId() in self.__act_etf_convert_cust_id:
            del self.__act_etf_convert_cust_id[etf_convert.getCustId()]

    def getAccountHoldings(self) -> dict:
        return self.__account_holdings

    def getAccountHolding(self, ticker) -> AccountHolding:
        return self.__account_holdings.get(ticker)

    def getAccountBalance(self) -> typing.Dict[str, AccountBalance]:
        return self.__account_balance

    def getActOrder(self, order_id: str) -> LimitOrder:
        return self.__active_orders.get(order_id, None)

    def getActOrders(self) -> typing.Dict[str, LimitOrder]:
        return self.__active_orders

    def getStratOrderRef(self, exchange_ref):
        return self.__order_id_mapping.get(exchange_ref, None)

    def getOrderExIds(self):
        return set(self.__order_id_mapping.keys())

    def getOrderByCustId(self, cust_id) -> LimitOrder:
        for order in self.getActOrders().values():
            if order.getCustId() == cust_id:
                return order

    def getOrderByExchangeId(self, exchange_id) -> LimitOrder:
        for order in self.getActOrders().values():
            self.getLogger().info(f"{exchange_id}, {order.getExchangeId()}")
            if order.getExchangeId() == exchange_id:
                return order

    def getInstrumentTrait(self, ticker):
        return self.__traits.get(ticker, None)

    def getTaskHandler(self):
        return self.__async_task_h

    def getTotalVolumeSentByTicker(self, ticker: str):
        return self.__order_volume_cumsum_ticker.get(ticker, 0)

    def getVolumeLimitByTicker(self, ticker: str):
        return self.__order_volume_limit_ticker.get(ticker, 10e10)

    def getLoop(self):
        return self.__async_task_h.getLoop()

    def getLogger(self):
        return self.__logger

    def getAccountName(self) -> str:
        return self.__account_name

    def getHostname(self):
        return self.__hostname

    def addCoroutineTask(self, task):
        self.__async_task_h.addCoroutineTask(task)

    # orders related start
    def registerOrder(self, order):
        if order.getReqId() not in self.__active_orders:
            self.__order_volume_cumsum_ticker[order.getTicker()] = order.getQuantity() + self.getTotalVolumeSentByTicker(order.getTicker())

        self.__active_orders[order.getReqId()] = order

        if order.getExchangeId():
            self.__order_id_mapping[str(order.getExchangeId())] = order.getReqId()

        # print(type(order.getExchangeId()))
        self.__logger.debug('order registered, strat_ref: {}, ex_ref: {}, cust_id: {}'.format(
            order.getReqId(),
            order.getExchangeId(),
            order.getCustId()))

    def unRegisterOrderById(self, order_id: str):
        """无需判断订单是否存在"""
        order = self.getActOrder(order_id)
        if order:
            self.unRegisterOrder(order)

    def unRegisterOrder(self, order):
        del self.__active_orders[order.getReqId()]
        self.__logger.debug(f'order unregistered, strat_ref: {order.getReqId()}, '
                            f'ex_ref: { order.getExchangeId()}')

    # orders related end
    async def _monitorReq(self):
        # 等待一段时间，同步订单，资金，持仓
        self.getLogger().info("waiting 5 seconds before receiving req")
        await asyncio.sleep(5) ##

        self.getLogger().info("receiving req")

        while not self.__async_task_h.eof():
            msg = await self.__zmq_client.recv()
            self.handleReq(msg)

            tsk = self.__zmq_client.pushLog(
                hostname=self.__hostname,
                type_='req',
                msg_str=msg
            )
            self.addCoroutineTask(tsk)

    def handleReq(self, msg_str):
        """

        :return:
        :rtype: str.
        """

        ## order
        req = trade_pb2.ReqMessage()
        req.ParseFromString(msg_str)
        # self.getLogger().info(req)

        if req.head == trade_pb2.ReqMessage.Head.Value('INSERT_ORDER'):
            self.__logger.debug(f'insert order from strategy {req.logic_id}, '
                               f'req_id: {req.req_id}, ticker: {req.insert_order.ticker}')
            try:
                if req.insert_order.action in (EtfConvertRequest.Action.CREATE, EtfConvertRequest.Action.REDEEM):
                    etf_convert_req = EtfConvertRequest.constructFromReq(req)
                    self.onEtfConvertReq(etf_convert_req)

                else:
                    self.onInsertOrder(req)

            except Exception as e:
                self.getLogger().error(f"invalid order: {protobuf_to_dict.protobuf_to_dict(req)}")
                self.onError(
                    strategy_order_ref=req.req_id,
                    type_=InvalidOrder.PROTO_CODE,
                    msg=str(e)
                )

        elif req.head == trade_pb2.ReqMessage.Head.Value('INSERT_BATCH_ORDER'):
            self.__logger.info(f'batch order {req.batch_order.batch_id} from strategy {req.logic_id}')
            try:
                self.onBatchOrder(req=req)

            except Exception as e:
                self.getLogger().error(f"invalid order: {protobuf_to_dict.protobuf_to_dict(req)}")
                self.onError(
                    strategy_order_ref=req.req_id,
                    type_=InvalidOrder.PROTO_CODE,
                    msg=str(e)
                )

        elif req.head == trade_pb2.ReqMessage.Head.Value('ALTER_ORDER'):
            self.__logger.info(f'cancel {req.cancel_order.order_req_id} order from {req.logic_id}')
            self.onCancelOrderReq(req.cancel_order.order_req_id)

        elif req.head == trade_pb2.ReqMessage.Head.Value("BATCH_CANCEL"):
            self.__logger.info(f'batch cancel from {req.logic_id}')
            self.onBatchCancelOrderReq(req)

        elif req.head == trade_pb2.ReqMessage.Head.Value('QUERY_POSITION'):
            # 同时返回持仓和资金，本身现金也是一种持仓
            asyncio.get_event_loop().create_task(self.onQryPosition())
            asyncio.get_event_loop().create_task(self.onQryAccountBalance())

        elif req.head == trade_pb2.ReqMessage.Head.Value('QUERY_ORDER'):
            asyncio.get_event_loop().create_task(self._pubActOrders())

        elif req.head == trade_pb2.ReqMessage.Head.Value('QUERY_TRADE'):
            self.__logger.warning('query trade method not supported')

        elif req.head == trade_pb2.ReqMessage.Head.Value('QUERY_INSTRUMENT'):
            asyncio.get_event_loop().create_task(self.onQueryInstrumentResp())

        elif req.head == trade_pb2.ReqMessage.Head.Value('QUERY_ACCOUNT'):
            # 同时返回持仓和资金，本身现金也是一种持仓
            asyncio.get_event_loop().create_task(self.onQryAccountBalance())
            asyncio.get_event_loop().create_task(self.onQryPosition())

        else:
            self.__logger.error('invalid request header: {}'.format(req.head))

    # self trading check
    def getMaxBuy(self, ticker):
        order_price = list()
        for order in self.getActOrders().values():
            if order.getTicker() == ticker and order.isBuy():
                order_price.append(order.getLimitPrice())

        if len(order_price):
            return max(order_price)
        else:
            return -10e10

    def getMinSell(self, ticker):
        order_price = list()
        for order in self.getActOrders().values():
            if order.getTicker() == ticker and not order.isBuy():
                order_price.append(order.getLimitPrice())

        if len(order_price):
            return min(order_price)
        else:
            return 10e10

    def isSelfTrading(self, order: LimitOrder):
        # self trading
        ticker = order.getTicker()
        limit_price = order.getLimitPrice()

        if order.isBuy():
            min_sell_price = self.getMinSell(order.getTicker())
            if limit_price >= min_sell_price:
                self.getLogger().warning("{} buy order price {} > active sell order price {}, self-trading risk".format(ticker, limit_price, min_sell_price))
                return True

        else:
            max_buy_price = self.getMaxBuy(order.getTicker())
            if limit_price <= max_buy_price:
                self.getLogger().warning("{} sell order price {} < active buy order price {}, self-trading risk".format(ticker, limit_price, max_buy_price))
                return True
    # self-trading end

    def onInsertOrder(self, req: trade_pb2.ReqMessage):

        """
        order is sent and registered if last order time is no late than <now> - <self.__order_interval>,
        otherwise order is cached.

        :return:
        """

        ticker = req.insert_order.ticker
        strategy_order_ref = req.req_id
        logic_id = req.logic_id

        if self.getTotalVolumeSentByTicker(ticker) >= self.getVolumeLimitByTicker(ticker):
            self.getLogger().warning("quantity excess limit {}, {} sent".format(self.getVolumeLimitByTicker(ticker),
                                                                                self.getTotalVolumeSentByTicker(ticker)))
            self.onError(type_=QuantityExcessLimit.PROTO_CODE)

        if datetime.now().timestamp() - self.__last_order_time < self.__order_interval:
            self.onError(type_=trade_pb2.Error.ErrorType.Value('insert_order_too_fast'),
                         strategy_order_ref=strategy_order_ref)

            self.__logger.warning('insert order too fast, order_ref: {}'.format(strategy_order_ref))
            return

        order = LimitOrder.fromReq(req, trait=self.getInstrumentTrait(ticker))
        if logic_id:
            counts = self.__logic_order_counts.get(logic_id, 0) + 1
            self.__logic_order_counts[logic_id] = counts
            order.setLogicId(logic_id)

        if order.getExpire() is None:
            order.setExpire(self.__max_order_life)

        if self.isSelfTrading(order):
            self.onError(type_=PriceExcessLimit.PROTO_CODE,
                         strategy_order_ref=order.getReqId(),
                         msg="self-trading risk")
            return

        order.switchState(order.State.SUBMITTED)
        self.registerOrder(order)

        self.getTaskHandler().addCoroutineTask(self.sendLimitOrder(order))
        self.__last_order_time = datetime.now().timestamp()

        self.__order_num += 1

    def onBatchOrder(self, req):
        batch_id = req.batch_order.batch_id
        if datetime.now().timestamp() - self.__last_order_time < self.__order_interval:
            self.onError(type_=trade_pb2.Error.ErrorType.Value('insert_order_too_fast'),
                         strategy_order_ref=req.batch_order.batch_id)

            self.__logger.warning(f'insert order too fast, batch id: {req.batch_order.batch_id}')
            return

        order_list = list()
        for order_req in req.batch_order.order_reqs:
            ticker = order_req.insert_order.ticker
            trait = self.getInstrumentTrait(ticker)
            if trait is None:
                raise InvalidSymbol("trait not found for {}".format(ticker))

            # print(order_req)
            # print(order_req.insert_order.extra, type(order_req.insert_order.extra))
            order_req.timestamp = req.timestamp
            order = LimitOrder.fromReq(order_req, trait)

            order.switchState(order.State.SUBMITTED)
            order.setLogicId(req.logic_id)

            expire = order_req.insert_order.expire
            if expire == 0 or expire is None:
                expire = self.__max_order_life
            order.setExpire(expire)

            self.registerOrder(order)

            order_list.append(order)

        self.getTaskHandler().addCoroutineTask(self.sendOrdersInBatch(batch_id, order_list))

    def onEtfConvertReq(self, etf_convert: EtfConvertRequest):
        self.registerActEtfConvert(etf_convert)
        self.getTaskHandler().addCoroutineTask(self.sendEtfConvert(etf_convert))

    def onCancelOrderReq(self, strategy_order_ref):
        """
        order is canceled and unregistered if order time is no late than <now> - <self.__cancel_interval>,
        otherwise req is cached.
        :return:
        """
        order = self.getActOrder(strategy_order_ref)
        if order is None:
            self.__logger.warning(
                'cancel order not found, id: {}'.format(strategy_order_ref))
            self.onError(type_=trade_pb2.Error.ErrorType.Value('order_not_exist'),
                         strategy_order_ref=strategy_order_ref)
            return

        if (datetime.now() - order.getSubmitDateTime()).total_seconds() < self.__cancel_interval:
            self.onError(type_=trade_pb2.Error.ErrorType.Value('cancel_order_too_fast'),
                         strategy_order_ref=strategy_order_ref,
                         exchange_order_ref=order.getExchangeId(),
                         msg='rejected by trade server')
            self.__logger.warning('canceling request too fast, strategy_order_ref: {}'.format(strategy_order_ref))
            return

        self.getLogger().info(
            f'canceling order, strategy_order_ref: {strategy_order_ref}, exchange_ref: {order.getExchangeId()}')

        self.getTaskHandler().addCoroutineTask(self.cancelOrder(order))
        self.__last_cancel_time = datetime.now()

    def onBatchCancelOrderReq(self, req: trade_pb2.ReqMessage):
        orders = list()
        for cancel_req in req.batch_cancel.cancel_req:
            order = self.getActOrder(cancel_req.order_req_id)
            if order:
                self.getLogger().info(f'canceling order, strategy_order_ref: {order.getId()}, exchange_ref: {order.getExchangeId()}')
                orders.append(order)
            else:
                self.getLogger().info(f"order strategy_ref {cancel_req.order_req_id} not found")

        if orders:
            self.getLogger().info(f"canceling {len(orders)} orders")
            self.getTaskHandler().addCoroutineTask(self.cancelOrdersInBatch(orders))

    # handle request end

    # handle respond start
    def _sendResp(self, resp: trade_pb2.RespMessage):
        resp.timestamp = datetime.now().timestamp()
        msg_str = resp.SerializeToString()
        self.__async_task_h.addCoroutineTask(self.__zmq_client.send(msg_str))

        log_tsk = self.__zmq_client.pushLog(
            hostname=self.__hostname,
            type_='resp',
            msg_str=msg_str,
        )
        self.__async_task_h.addCoroutineTask(log_tsk)

    def onEtfConvertResp(self, etf_convert: EtfConvertRequest):
        resp_pb = trade_pb2.RespMessage()
        resp_pb.head = trade_client.RespType.ON_ORDER_ACTION
        resp_pb.resp_id = str(uuid.uuid4())
        resp_pb.timestamp = datetime.now().timestamp()
        resp_pb.req_id = etf_convert.getReqId()

        resp_pb.order_resp.action = etf_convert.getAction()

        self._sendResp(resp_pb)
        # self.unRegisterActEtfConvert(etf_convert) # 不可注销，不然股票交收回报可能取不到req_id

    def onEtfConvertComponentResp(self, exchange_ref: str, convert_req_id: str, etf_ticker: str,
                                  stock_ticker: str, quantity: int):
        resp = trade_pb2.RespMessage()
        resp.head = trade_client.RespType.ETF_CONVERT_COMPONENT
        resp.etf_component.etf_ticker = etf_ticker
        resp.etf_component.component_ticker = stock_ticker
        resp.etf_component.quantity = quantity
        resp.etf_component.req_id = convert_req_id
        resp.etf_component.exchange_id = exchange_ref
        resp.resp_id = str(uuid.uuid4())

        self._sendResp(resp)

    def onError(self, type_, exchange_order_ref: str=None, strategy_order_ref: str=None,
                msg:str=None, logic_id:str=None):
        resp_pb = trade_pb2.RespMessage()
        resp_pb.head = resp_pb.Head.Value('ON_ERROR')
        resp_pb.timestamp = datetime.now().timestamp()
        resp_pb.resp_id = str(uuid.uuid4())

        if msg:
            resp_pb.error.msg = msg

        if exchange_order_ref:
            resp_pb.error.exchange_order_ref = exchange_order_ref
            resp_pb.error.order_req_id = self.getStratOrderRef(exchange_order_ref)

        if strategy_order_ref:
            resp_pb.error.order_req_id = strategy_order_ref

        resp_pb.error.type = type_
        resp_pb.error.account_name = self.__account_name

        if type_==trade_pb2.Error.ErrorType.Value('insert_order_too_fast'):
            order = self.getActOrder(strategy_order_ref)
            if order:
                self.unRegisterOrder(order)

        elif type_ == OrderNotFound.PROTO_CODE:
            order = self.getActOrder(strategy_order_ref)
            if order:
                self.unRegisterOrder(order)
            else:
                self.getLogger().error(f"cannot find order_req_id: {strategy_order_ref}")

        # notice: 这里一个一个处理是为了防范风险，else -> unregister order可能导致特殊性情况下一直报单
        elif type_ == trade_pb2.Error.ErrorType.Value('auth_failed'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_==trade_pb2.Error.ErrorType.Value('insufficient_fund'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_==trade_pb2.Error.ErrorType.Value('insufficient_qty'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('price_too_low'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('price_too_high'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('price_excess_limit'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('duplicate_custom_orderid'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('invalid_orderid'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('duplicate_orderid'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('invalid_symbol'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('instrument_do_not_match'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('instrument_expired'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('instrument_has_no_market_price'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('accounts_do_not_match'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('invalid_account'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('account_suspended'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('permission_denied'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('invalid_type'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('invalid_order'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('request_timeout'):
            self.unRegisterOrderById(strategy_order_ref)

        elif type_ == trade_pb2.Error.ErrorType.Value('excess_daily_quota'):
            self.unRegisterOrderById(strategy_order_ref)

        self._sendResp(resp_pb)

        self.__error_num += 1

    def onTrade(self, ticker: str, price: float, quantity: float, commission: float,
                dateTime: datetime, exchange_order_ref: str,  exchange_trade_ref: str, strategy_order_ref: str,
                msg=None):
        order = self.getActOrder(strategy_order_ref)

        if order is None:
            self.getLogger().info(f"order not found, order req_id: {strategy_order_ref}, "
                                  f"exchange_order_ref: {exchange_order_ref}, exchange_trade_ref: {exchange_trade_ref}")
            return

        if order.getState() == LimitOrder.State.SUBMITTED:
            self.onOrderAcceptedResp(
                exchange_order_ref=exchange_order_ref,
                strategy_order_ref=strategy_order_ref,
                accepted_time=dateTime
            )

        exec_info = OrderExecutionInfo(price=price,
                                       quantity=quantity,
                                       commission=commission,
                                       dateTime=dateTime,
                                       exchange_ref=exchange_trade_ref)
        order.addExecutionInfo(exec_info)

        if order.getRemaining() == 0:
            self.unRegisterOrder(order)
            if_last = True
            self.getLogger().info('order fully filled, exchange_ref: {}'.format(exchange_order_ref))
            self.__filled_order_ids.add(order.getReqId())

        else:
            if_last = False
            self.getLogger().info('order partially filled, remaining: {},'
                                  ' exchange_ref: {}'.format(order.getRemaining(),
                                                             exchange_order_ref))

        pb_ins = trade_pb2.RespMessage()
        pb_ins.head = trade_pb2.RespMessage.Head.Value('ON_TRADE')
        pb_ins.timestamp = datetime.now().timestamp()

        pb_ins.trade.ticker = ticker

        pb_ins.trade.price = price
        pb_ins.trade.quantity = quantity
        pb_ins.trade.action = order.getAction()

        dt = datetime.now()
        pb_ins.trade.timestamp = dt.timestamp()
        pb_ins.trade.exchange_order_ref = str(exchange_order_ref)
        pb_ins.trade.exchange_trade_ref = str(exchange_trade_ref)
        pb_ins.trade.order_req_id = strategy_order_ref
        pb_ins.trade.if_last = if_last

        pb_ins.trade.account_name = self.__account_name
        if msg is not None:
            pb_ins.trade.msg = str(msg)

        pb_ins.resp_id = str(uuid.uuid4())

        self._sendResp(pb_ins)

        self.__trade_num += 1

    def onExecInfo(self, exchange_order_ref: str, dateTime, fill_quantity, avg_fill_price, cost, msg=None):
        """ 成交回报数据全量更新时使用

        :param exchange_order_ref:
        :param dateTime:
        :param fill_quantity:
        :param avg_fill_price:
        :param cost:
        :param msg:
        :return:
        """
        # print(exchange_order_ref, type(exchange_order_ref))
        order = self.getOrderByExchangeId(exchange_order_ref)
        if order is None:
            self.getLogger().info(f'unrecognised order, exchange_order_ref: {exchange_order_ref}')
            return

        if fill_quantity > 0:
            new_filled = fill_quantity - order.getFilled()
            if new_filled > 0 and not np.isclose(fill_quantity, 0):
                new_commission = cost - order.getCommissions()
                if order.getAvgFillPrice() is None:
                    new_filled_price = avg_fill_price

                else:
                    new_filled_price = ((avg_fill_price * fill_quantity) - (
                            order.getFilled() * order.getAvgFillPrice())) / new_filled
                exchange_ref = '{}-{}'.format(order.getReqId(), len(order.getAllExcutionInfo()))

                self.onTrade(ticker=order.getTicker(),
                             price=new_filled_price,
                             quantity=new_filled,
                             commission=new_commission,
                             dateTime=dateTime,
                             exchange_order_ref=order.getExchangeId(),
                             exchange_trade_ref=exchange_ref,
                             strategy_order_ref=order.getReqId(),
                             msg=msg)

    def onOrderAcceptedResp(self, exchange_order_ref: str, strategy_order_ref: str, accepted_time: datetime=None):
        """对同一个订单，可以重复调用

        :param exchange_order_ref:
        :param strategy_order_ref:
        :param accepted_time:
        :return:
        """
        self.__logger.info('order: {} accepted, update exchange ref: {}'.format(strategy_order_ref,
                                                                                exchange_order_ref))

        order = self.getActOrder(strategy_order_ref)
        if order is None:
            self.getLogger().info(f"order not found")
            return

        order.setExchangeId(exchange_order_ref, accepted_time)

        if order.getState() >= Order.State.ACCEPTED:
            return

        self.registerOrder(order)  # register again to update exchange ref
        order.switchState(Order.State.ACCEPTED)

        pb_ins = trade_pb2.RespMessage()
        pb_ins.head = trade_pb2.RespMessage.Head.Value('ON_ORDER_ACTION')
        pb_ins.resp_id = str(uuid.uuid4())

        if accepted_time is None:
            pb_ins.timestamp = datetime.now().timestamp()
        else:
            pb_ins.timestamp = accepted_time.timestamp()

        pb_ins.order_resp.exchange_order_ref = str(exchange_order_ref)
        pb_ins.order_resp.state = trade_pb2.OrderState.Value('ACCEPTED')
        pb_ins.order_resp.order_req_id = str(strategy_order_ref)

        self._sendResp(pb_ins)

    def onOrderCanceledResp(self, order: LimitOrder, canceled_time=None):
        self.__logger.info(f'order: {order.getReqId()}, '
                           f'canceled at {canceled_time}')

        order.switchState(Order.State.CANCELED)
        self.unRegisterOrder(order)

        pb_ins = trade_pb2.RespMessage()
        pb_ins.head = trade_pb2.RespMessage.Head.Value('ON_ORDER_ACTION')
        pb_ins.resp_id = str(uuid.uuid4())

        if canceled_time is None:
            pb_ins.timestamp = datetime.now().timestamp()
        else:
            pb_ins.timestamp = canceled_time.timestamp()

        pb_ins.order_resp.exchange_order_ref = str(order.getExchangeId())
        pb_ins.order_resp.state = trade_pb2.OrderState.Value('CANCELED')
        pb_ins.order_resp.order_req_id = str(order.getReqId())

        self._sendResp(pb_ins)

    def updateQryOrderRes(self, qry_order_list: typing.List[QryOrderResult]):
        self.__qry_order_res = qry_order_list.copy()
        self.getTaskHandler().addCoroutineTask(self._pubActOrders())

    async def _pubActOrders(self):
        pb_ins = trade_pb2.RespMessage()
        pb_ins.head = trade_pb2.RespMessage.Head.Value('ON_QUERY_ORDER')
        pb_ins.resp_id = str(uuid.uuid4())
        pb_ins.timestamp = datetime.now().timestamp()

        pb_ins.qry_order_resp.update_timestamp = datetime.now().timestamp()
        pb_ins.qry_order_resp.account_name = self.__account_name

        self.__logger.info('assembling {} orders'.format(len(self.getActOrders())))
        for order_id in self.getActOrders():
            order = self.getActOrder(order_id)
            pending_order = pb_ins.qry_order_resp.pending_order.add()
            pending_order.ticker = order.getTicker()

            pending_order.limit_price = order.getLimitPrice()
            pending_order.quantity = order.getQuantity()
            pending_order.action = order.getAction()
            pending_order.created_timestamp = order.getSubmitDateTime().timestamp()
            pending_order.order_req_id = order.getReqId()

            if order.getExchangeId():
                pending_order.exchange_order_ref = str(order.getExchangeId())

            if order.getFilled():
                pending_order.filled_quantity = order.getFilled()

        for qry_order_res in self.__qry_order_res:
            if qry_order_res.getExchangeOrderRef() in self.__order_id_mapping:
                continue

            pending_order = pb_ins.qry_order_resp.pending_order.add()
            pending_order.ticker = qry_order_res.getTicker()

            pending_order.limit_price = qry_order_res.getLimitPrice()
            pending_order.quantity = qry_order_res.getQuantity()
            pending_order.action = qry_order_res.getAction()
            pending_order.created_timestamp = qry_order_res.getCreateTime().timestamp()

            pending_order.order_req_id = "manual"
            pending_order.exchange_order_ref = qry_order_res.getExchangeOrderRef()
            pending_order.filled_quantity = qry_order_res.getFilled()

        self._sendResp(resp=pb_ins)

    def updateAccountHoldings(self, holdings: typing.Dict[str, AccountHolding]):
        self.__account_holdings = holdings
        self.__account_holding_update_time = datetime.now()
        self.getLogger().debug("account holding updated")
        self.addCoroutineTask(self.onQryPosition())

    def updateAccountBalance(self, balance: typing.Dict[str, AccountBalance]):
        self.__account_balance = balance
        self.__account_balance_update_time = datetime.now()
        self.getLogger().debug("account balance updated")

    async def onQryPosition(self):
        if self.__account_holding_update_time is None:
            return

        lag = (datetime.now() - self.__account_holding_update_time).seconds
        if lag >= 60:
            return

        pb_ins = trade_pb2.RespMessage()
        pb_ins.head = trade_pb2.RespMessage.Head.Value('ON_QUERY_POSITION')
        pb_ins.timestamp = datetime.now().timestamp()
        pb_ins.resp_id = str(uuid.uuid4())

        pb_ins.position.update_timestamp = self.__account_holding_update_time.timestamp()

        for ticker in self.__account_holdings:
            holding = self.__account_holdings[ticker]
            if np.isclose(0, holding.getLongHolding()) and np.isclose(0, holding.getShortHolding()):
                continue

            posi = pb_ins.position.positions.add()
            posi.ticker = ticker
            posi.long_volume = holding.getLongHolding()
            posi.long_avg_cost = holding.getLongAvgCost()
            posi.long_available = holding.getLongAvailable()
            if holding.getLongMargin():
                posi.long_margin = holding.getLongMargin()

            posi.short_volume = holding.getShortHolding()
            posi.short_avg_cost = holding.getShortAvgCost()
            posi.short_available = holding.getShortAvailable()
            if holding.getShortMargin():
                posi.short_margin = holding.getShortMargin()

            if holding.getLongOpenedToday():
                posi.long_opened_today = holding.getLongOpenedToday()

            if holding.getShortOpenedToday():
                posi.short_opened_today = holding.getShortOpenedToday()

        self._sendResp(pb_ins)

    async def onQryAccountBalance(self):
        lag = (datetime.now() - self.__account_holding_update_time).seconds
        if lag >= 60:
            self.onError(
                type_=ExchangeError.PROTO_CODE,
                msg=f"account balance lag: {lag}"
            )
            return

        for currency in self.getAccountBalance():
            balance = self.getAccountBalance()[currency]

            pb_ins = trade_pb2.RespMessage()
            pb_ins.head = trade_pb2.RespMessage.Head.Value('ON_QUERY_ACCOUNT')
            pb_ins.resp_id = str(uuid.uuid4())

            pb_ins.account_bal.account_id = self.getAccount()
            pb_ins.account_bal.update_timestamp = self.__account_balance_update_time.timestamp()
            pb_ins.account_bal.balance = balance.getBalance()
            pb_ins.account_bal.withdraw = balance.getCashWithdrawable()
            pb_ins.account_bal.cash_available = balance.getCashAvailable()
            pb_ins.account_bal.cash_forriskasset = balance.getCashForRiskAsset()
            pb_ins.account_bal.cash_forcovershort = balance.getCashForCoverShort()
            pb_ins.account_bal.cash_credit = balance.getCashBorrowable()
            pb_ins.account_bal.margin = balance.getMargin()
            pb_ins.account_bal.currency = currency

            self._sendResp(resp=pb_ins)

    async def onQueryInstrumentResp(self):
        pb_ins = trade_pb2.RespMessage()
        pb_ins.head = trade_pb2.RespMessage.Head.Value('ON_QUERY_INSTRUMENT')
        pb_ins.resp_id = str(uuid.uuid4())

        pb_ins.instrument.timestamp = datetime.now().timestamp()

        for instrument in self.__traits.values():
            inst = pb_ins.instrument.instrument.add()
            inst.ticker = instrument['ticker']
            inst.is_trading = instrument['is_trading']
            inst.price_tick = instrument['price_tick']
            inst.min_limit_order_volume = instrument['min_limit_order_volume']
            inst.trade_unit = instrument['trade_unit']
            inst.maker_fee = instrument.get('maker_fee', 0)
            inst.taker_fee = instrument.get('taker_fee', 0)
            inst.type = instrument['type']
            inst.volume_precision = instrument['volume_precision']
            inst.quote_precision = instrument['quote_precision']
            if instrument['contract_size']:
                inst.contract_size = instrument['contract_size']

        self._sendResp(pb_ins)
        self.getLogger().info('query instrument resp sent')

    def runPeriodicAsyncJob(self, interval, callback, *args, **kwargs):
        self.__async_task_h.runPeriodicAsyncJob(interval, callback, *args, **kwargs)

    def checkUpdate(self):
        balance_lag = datetime.now().timestamp() - self.__account_balance_update_time.timestamp()
        position_lag = datetime.now().timestamp() - self.__account_holding_update_time.timestamp()
        if position_lag > 30 or balance_lag > 30:
            self.__logger.warning(f'balance lag: {int(balance_lag)} seconds, '
                                  f'position lag: {int(position_lag)} seconds, ')
            return False
        return True

    async def updateSnapshot(self):
        """
        1. position
        2. order
        3. recent trades

        :return:
        """

        if not self.checkUpdate():
            return

        snapshot = dict()
        snapshot['update_timestamp'] = datetime.now().timestamp()
        snapshot["holding_update_timestamp"] = self.__account_holding_update_time.timestamp()
        snapshot["balance_update_timestamp"] = self.__account_balance_update_time.timestamp()

        #  holdings
        holdings = dict()
        for ticker in self.__account_holdings:
            holding = self.__account_holdings[ticker]
            holdings[ticker] = holding.toDict()
        snapshot["holdings"] = holdings

        # balance
        balance = dict()
        if self.__account_balance:
            for symbol in self.__account_balance:
                balance[symbol] = self.__account_balance[symbol].toDict()
        snapshot["balance"] = balance

        # orders
        orders = list()
        exchange_ids = set()
        counter = 0
        for order_id in self.getActOrders():
            order = self.getActOrder(order_id)
            order_doc = {'order_id': order_id,
                         'ticker': order.getTicker(),
                         'limit_price': order.getLimitPrice(),
                         'quantity': order.getQuantity(),
                         'action': order.getAction(),
                         'submit_time': order.getSubmitDateTime().timestamp(),
                         'exchange_order_ref': order.getExchangeId(),
                         'strategy_order_ref': order.getReqId(),
                         'filled': order.getFilled(),
                         'logic_id': order.getLogicId(),
                         'state': order.getState(),
                         'account': self.__account_name}
            orders.append(order_doc)
            exchange_ids.add(order.getExchangeId())

            counter += 1
            if counter == 10:
                break

        counter = 0
        for qry_order_res in self.getQryOrderResults()[:10]:
            if qry_order_res.getExchangeOrderRef() in exchange_ids:
                continue

            if qry_order_res.getFilled() > 0:
                state = 'filled'
            else:
                state = 'accepted'

            order = {'order_id': 'manual',
                     'ticker': qry_order_res.getTicker(),
                     'limit_price': qry_order_res.getLimitPrice(),
                     'quantity': qry_order_res.getQuantity(),
                     'action': qry_order_res.getAction(),
                     'submit_time': qry_order_res.getCreateTime().timestamp(),
                     'exchange_order_ref': qry_order_res.getExchangeOrderRef(),
                     'strategy_order_ref': "manual",
                     'filled': qry_order_res.getFilled(),
                     'logic_id': 'invalid',
                     'state': state,
                     'account': self.__account_name}
            orders.append(order)

            counter += 1
            if counter == 10:
                break

        snapshot['orders'] = orders
        snapshot['update_timestamp'] = datetime.now().timestamp()
        snapshot['hostname'] = self.__hostname

        snapshot['stat'] = {
            "order_num": self.__order_num,
            "trade_num": self.__trade_num,
            "error_num": self.__error_num,
        }

        await self.__zmq_client.pushLog(
            hostname=self.__hostname,
            type_='snapshot',
            msg_str=json.dumps(snapshot, cls=CustomEncoder).encode(),
        )

    async def _cancelExpiredOrders(self):
        """expired orders are only canceled once. cancel请求发送之后即注销，变为外部订单。

        :return:
        """
        await asyncio.sleep(0.1)

        # expired orders
        for order in list(self.getActOrders().values()):
            if np.isclose(order.getExpire(), 0):
                continue

            submit_past = (datetime.now() - order.getSubmitDateTime()).total_seconds()
            if submit_past > min(self.__max_order_life, order.getExpire()):
                if order.getState() in (LimitOrder.State.ACCEPTED, LimitOrder.State.PARTIALLY_FILLED):
                    log_msg = f'cancelling expired order: {order.getReqId()}, submitted: {order.getSubmitDateTime()}, ' \
                              f'order life: {order.getExpire()} '
                    self.getLogger().info(log_msg)
                    self.onCancelOrderReq(strategy_order_ref=order.getReqId())

                else:
                    state = LimitOrder.State.toString(order.getState())
                    self.getLogger().info(f"cannot cancel expired order: {order.getReqId()} in state: {state}")

        # cancel orders registered before server start, strategy_order_ref lost
        to_cancel = list()
        for qry_order_resp in self.getQryOrderResults():
            if qry_order_resp.getCreateTime() < self.__start_time:
                if qry_order_resp.getExchangeOrderRef() not in self.__order_cancel_time:
                    self.getLogger().info(f"canceling active order created before start,"
                                          f" order_id: {qry_order_resp.getExchangeOrderRef()}")
                    order = LimitOrder(
                        action=qry_order_resp.getAction(),
                        ticker=qry_order_resp.getTicker(),
                        limitPrice=qry_order_resp.getLimitPrice(),
                        quantity=qry_order_resp.getQuantity(),
                        instrumentTraits=self.getInstrumentTrait(qry_order_resp.getTicker()),
                    )
                    order.setSubmitted(req_id='unknown', dateTime=qry_order_resp.getCreateTime())

                    order.setExchangeId(exchange_id=qry_order_resp.getExchangeOrderRef(),
                                        accepted_time=qry_order_resp.getCreateTime())

                    to_cancel.append(order)
                    self.__order_cancel_time[qry_order_resp.getExchangeOrderRef()] = datetime.now()

        if len(to_cancel):
            self.getLogger().info(f'canceling {len(to_cancel)} out-dated orders')
            self.getTaskHandler().addCoroutineTask(self.cancelOrdersInBatch(to_cancel))

    def run_forever(self):
        import os
        self.__account_name = self.__hostname
        self.__order_interval = float(os.environ.get("TRADE_ORDER_INTERVAL", 0))
        self.__qry_interval = float(os.environ.get("TRADE_QUERY_INTERVAL", 0))
        self.__cancel_interval = float(os.environ.get("TRADE_CANCEL_INTERVAL", 0))
        self.__max_order_life = float(os.environ.get("TRADE_MAX_ORDER_LIFE", 600))

        self.__logger.info('starting zmq, request port: 53001, resp port: 53002')
        self.__zmq_client = zmqClient.AsyncClient(req_port=53001,
                                                  resp_port=53002,
                                                  )

        self.__logger.info('starting trade server')

        # 实现来控制查询频率
        self.__async_task_h.runPeriodicAsyncJob(30, self.qryActiveOrder)
        self.__async_task_h.runPeriodicAsyncJob(30, self.qryAccountHolding)
        self.__async_task_h.runPeriodicAsyncJob(30, self.qryAccountBalance)
        self.__async_task_h.runPeriodicAsyncJob(5, self._cancelExpiredOrders)
        self.__async_task_h.runPeriodicAsyncJob(5, self.updateSnapshot)

        self.getLogger().info('listening to requests')

        self.getTaskHandler().addCoroutineTask(self._monitorReq())
        self.getTaskHandler().getLoop().run_forever()

    def stop(self):
        loop = asyncio.get_event_loop()
        loop.stop()
        self.__async_task_h.stop()

    @abc.abstractmethod
    async def sendLimitOrder(self, order: LimitOrder):
        raise NotImplementedError

    @abc.abstractmethod
    async def cancelOrder(self, order: LimitOrder):
        raise NotImplementedError

    @abc.abstractmethod
    async def sendOrdersInBatch(self, batch_id, order_list: typing.List[LimitOrder]):
        raise NotImplementedError

    @abc.abstractmethod
    async def cancelOrdersInBatch(self, order_list: typing.List[LimitOrder]):
        raise NotImplementedError

    @abc.abstractmethod
    async def qryAccountHolding(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def qryActiveOrder(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def sendEtfConvert(self, etf_convert: EtfConvertRequest):
        raise NotImplementedError

    @abc.abstractmethod
    async def login(self, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    async def qryAccountBalance(self):
        raise NotImplementedError()
