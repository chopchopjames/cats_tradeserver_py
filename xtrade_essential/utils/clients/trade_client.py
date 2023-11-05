# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import zmq
import zmq.asyncio
import uuid
import threading

from datetime import datetime

from xtrade_essential.proto import trade_pb2
from xtrade_essential.utils.taskhandler import multithread
from xtrade_essential.utils.order import *


class RespType:
    ON_START = trade_pb2.RespMessage.Head.Value("ON_START")
    ON_LOGIN = trade_pb2.RespMessage.Head.Value("ON_LOGIN")
    ON_LOGOUT = trade_pb2.RespMessage.Head.Value("ON_LOGOUT")
    ON_TRADE = trade_pb2.RespMessage.Head.Value("ON_TRADE")
    ON_ORDER_ACTION = trade_pb2.RespMessage.Head.Value("ON_ORDER_ACTION")
    ON_QUERY_ORDER = trade_pb2.RespMessage.Head.Value("ON_QUERY_ORDER")
    ON_QUERY_POSITION = trade_pb2.RespMessage.Head.Value("ON_QUERY_POSITION")
    ON_QUERY_ACCOUNT = trade_pb2.RespMessage.Head.Value("ON_QUERY_ACCOUNT")
    ON_QUERY_INSTRUMENT = trade_pb2.RespMessage.Head.Value("ON_QUERY_INSTRUMENT")
    ETF_CONVERT_COMPONENT = trade_pb2.RespMessage.Head.Value("ETF_CONVERT_COMPONENT")

    ON_STRATEGY_REG = trade_pb2.RespMessage.Head.Value("ON_STRATEGY_REG")
    ON_ERROR = trade_pb2.RespMessage.Head.Value("ON_ERROR")

    UNKNOWN = 10086


class ReqType:
    RegisterStrategy = trade_pb2.ReqMessage.Head.Value("RegisterStrategy")
    UnRegisterStrategy = trade_pb2.ReqMessage.Head.Value("UnRegisterStrategy")
    LOGIN = trade_pb2.ReqMessage.Head.Value("LOGIN")
    LOGOUT = trade_pb2.ReqMessage.Head.Value("LOGOUT")
    INSERT_ORDER = trade_pb2.ReqMessage.Head.Value("INSERT_ORDER")
    ALTER_ORDER = trade_pb2.ReqMessage.Head.Value("ALTER_ORDER")
    QUERY_ORDER = trade_pb2.ReqMessage.Head.Value("QUERY_ORDER")
    QUERY_TRADE = trade_pb2.ReqMessage.Head.Value("QUERY_TRADE")
    QUERY_POSITION = trade_pb2.ReqMessage.Head.Value("QUERY_POSITION")
    QUERY_ACCOUNT =trade_pb2.ReqMessage.Head.Value("QUERY_ACCOUNT")
    HEART_BEAT = trade_pb2.ReqMessage.Head.Value("HEART_BEAT")
    QUERY_INSTRUMENT = trade_pb2.ReqMessage.Head.Value("QUERY_INSTRUMENT")
    INSERT_BATCH_ORDER = trade_pb2.ReqMessage.Head.Value("INSERT_BATCH_ORDER")

    UNKNOWN = 10086


def parseReq(pb_ins: trade_pb2.ReqMessage):
    if pb_ins.head == ReqType.ETF_CONVERT:
        obj = EtfConvertRequest(
            action=pb_ins.etf_convert.action,
            ticker=pb_ins.etf_convert.ticker,
            quantity=pb_ins.etf_convert.quantity,
            logic_id=pb_ins.logic_id,
            min_exchange_unit=int(pb_ins.etf_convert.min_exchange_unit)
        )
        obj.setSubmitted(pb_ins.req_id, dateTime=datetime.fromtimestamp(pb_ins.timestamp))
        return ReqType.ETF_CONVERT, obj

    return ReqType.UNKNOWN, None


def parseResp(resp_pb: str):
    pb_ins = trade_pb2.RespMessage()
    pb_ins.ParseFromString(resp_pb)
    if pb_ins.head == RespType.ON_TRADE:
        exec_info = OrderExecutionInfo(price=pb_ins.trade.price,
                                       quantity=pb_ins.trade.quantity,
                                       commission=pb_ins.trade.commission,
                                       dateTime=datetime.fromtimestamp(pb_ins.trade.timestamp),
                                       exchange_ref=pb_ins.trade.exchange_trade_ref)
        return RespType.ON_TRADE, exec_info

    return RespType.UNKNOWN, None


def getReqSock(addr, port):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    conn_str = "tcp://{}:{}".format(addr, port)
    socket.connect(conn_str)

    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
    return socket


def getSubSock(addr, port, pattern_str=''):
    addr_str = '{0}:{1}'.format(addr, port)
    context = zmq.Context()
    sock = context.socket(zmq.SUB)
    sock.setsockopt_string(zmq.SUBSCRIBE, pattern_str)
    sock.connect('tcp://%s' % (addr_str))

    sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
    sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
    sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
    sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
    return sock


class TradeClient:
    def __init__(self, addr, req_port, resp_port, callback):
        self.__addr = addr
        self.__req_port = req_port
        self.__resp_port = resp_port
        self.__callback = callback

        self.__req_sock = None
        self.__resp_sock = None

        self.__latest_qry_order_resp = None
        self.__qry_order_resp_update = None
        self.__latest_qry_holding_resp = None
        self.__qry_holding_update = None

        self.__eof = threading.Event()

    def getLatestQryOrderResp(self):
        return self.__latest_qry_order_resp

    def getLatestQryHoldingResp(self):
        return self.__latest_qry_holding_resp

    def getQryOrderRespUpdateTime(self):
        return self.__qry_order_resp_update

    def getQryHoldingRespUpdateTime(self):
        return self.__qry_holding_update

    def getHost(self):
        return self.__addr

    def connect(self):
        self.__req_sock = getReqSock(self.__addr, self.__req_port)
        self.__resp_sock = getSubSock(self.__addr, self.__resp_port)

        self.__task_handler = multithread.TaskHandler()
        self.__task_handler.runJob(self._monitorResp)

    def _monitorResp(self):
        while not self.__eof.is_set():
            try:
                msg = self.__resp_sock.recv()
                if msg:
                    pb_ins = trade_pb2.RespMessage()
                    pb_ins.ParseFromString(msg)

                    if pb_ins.head == trade_pb2.RespMessage.Head.Value('ON_QUERY_ORDER'):
                        self.__latest_qry_order_resp = pb_ins
                        self.__qry_order_resp_update = datetime.now()

                    elif pb_ins.head == trade_pb2.RespMessage.Head.Value('ON_QUERY_POSITION'):
                        self.__latest_qry_holding_resp = pb_ins
                        self.__qry_holding_update = datetime.now()

                    self.__callback(pb_ins)

            except Exception as e:
                self.stop()
                raise e

    def sendReq(self, req: BaseRequest):
        req_id = str(uuid.uuid4())
        submitted_time = datetime.now()

        req.setSubmitted(str(uuid.uuid4()), submitted_time)
        req_pb = req.getReq()
        req_pb.req_id = req.getReqId()
        req_pb.timestamp = submitted_time.timestamp()

        if req_pb:
            self.__req_sock.send(req_pb.SerializeToString())
        else:
            raise Exception("request is not ready")

        return req_id

    def sendLimitOrder(self, order: LimitOrder):
        return self.sendReq(req=order)

    def sendBatchOrder(self, batch_order: BatchOrder):
        req_id = str(uuid.uuid4())
        submitted_time = datetime.now()

        batch_order.setSubmitted(req_id, submitted_time)
        for order in batch_order.getOrders():
            order.setSubmitted(str(uuid.uuid4()), datetime.now())

        req = batch_order.getReq()
        req.req_id = batch_order.getReqId()
        req.timestamp = submitted_time.timestamp()

        self.__req_sock.send(req.SerializeToString())
        return req_id

    def cancelOrder(self, order):
        req = trade_pb2.ReqMessage()
        req.head = trade_pb2.ReqMessage.Head.Value('ALTER_ORDER')
        req.req_id = str(uuid.uuid4())
        req.logic_id = str(order.getLogicId())

        req.cancel_order.order_req_id = str(order.getReqId())
        req.timestamp = datetime.now().timestamp()
        self.__req_sock.send(req.SerializeToString())
        return req.req_id

    def batchCancel(self, logic_id: str, order_ids: list):
        req_id = str(uuid.uuid4())
        req = trade_pb2.ReqMessage()
        req.head = trade_pb2.ReqMessage.Head.Value("BATCH_CANCEL")
        req.timestamp = datetime.now()

        for order_id in order_ids:
            cancel_order = req.batch_cancel.cancel_req.add()

            cancel_order.strategy_id = str(logic_id)
            cancel_order.strategy_order_ref = str(order_id)
            cancel_order.batch_id = req_id

        req.req_id = req_id
        self.__req_sock.send(req.SerializeToString())
        return req_id

    def _sendQry(self, qry_type):
        if self.__resp_sock is None:
            raise Exception("must run connect before sending query")

        req = trade_pb2.ReqMessage()

        if qry_type == 'account':
            req.head = trade_pb2.ReqMessage.Head.Value('QUERY_ACCOUNT')
        elif qry_type == 'order':
            req.head = trade_pb2.ReqMessage.Head.Value('QUERY_ORDER')
        elif qry_type == 'trade':
            req.head = trade_pb2.ReqMessage.Head.Value('QUERY_TRADE')
        elif qry_type == 'position':
            req.head = trade_pb2.ReqMessage.Head.Value('QUERY_POSITION')
        elif qry_type == 'instrument':
            req.head = trade_pb2.ReqMessage.Head.Value('QUERY_INSTRUMENT')
        else:
            raise Exception('invalid qry type')

        req.timestamp = datetime.now().timestamp()
        req.req_id = str(uuid.uuid4())
        self.__req_sock.send(req.SerializeToString())
        return req.req_id

    def qryPosition(self):
        return self._sendQry('position')

    def qryOrder(self):
        return self._sendQry('order')

    def qryAccount(self):
        return self._sendQry('account')

    def qryInstrument(self):
        return self._sendQry('instrument')

    def stop(self):
        self.__eof.set()

    def eof(self):
        return self.__eof.is_set()