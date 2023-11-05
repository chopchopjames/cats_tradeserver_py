# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import zmq
import zmq.asyncio
from datetime import datetime

from ..trade_client import *

from xtrade_essential.proto import trade_pb2
from xtrade_essential.utils.order import BaseRequest


class AsyncTradeClient(TradeClient):
    def __init__(self, addr, req_port, resp_port, callback, loop):
        TradeClient.__init__(self, addr, req_port, resp_port, callback)
        self.__addr = addr
        self.__req_port = req_port
        self.__resp_port = resp_port
        self.__callback = callback
        self.__loop = loop

    def connect(self):
        addr_str = 'tcp://{0}:{1}'.format(self.__addr, self.__resp_port)
        ctx = zmq.asyncio.Context()
        self.__resp_sock = ctx.socket(zmq.SUB)
        self.__resp_sock.connect(addr_str)
        self.__resp_sock.setsockopt_unicode(zmq.SUBSCRIBE, "")
        self.__resp_sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.__resp_sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        self.__resp_sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
        self.__resp_sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)

        addr_str = 'tcp://{0}:{1}'.format(self.__addr, self.__req_port)
        print('req addr: {}'.format(addr_str))
        ctx = zmq.asyncio.Context()
        self.__req_sock = ctx.socket(zmq.PUSH)
        self.__req_sock.connect(addr_str)
        self.__req_sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.__req_sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        self.__req_sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
        self.__req_sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)

        self.__loop.create_task(self._monitorResp())

    async def sendReq(self, req: BaseRequest):
        req_id = str(uuid.uuid4())
        submitted_time = datetime.now()

        req.setSubmitted(req_id, submitted_time)
        req_pb = req.getReq()
        req_pb.req_id = req.getReqId()
        req_pb.timestamp = submitted_time.timestamp()

        if req_pb:
            await self.__req_sock.send(req_pb.SerializeToString())
        else:
            raise Exception("request is not ready")

        return req_id

    async def cancelOrder(self, order):
        req = trade_pb2.ReqMessage()
        req.head = trade_pb2.ReqMessage.Head.Value('ALTER_ORDER')
        req.req_id = str(uuid.uuid4())
        req.logic_id = str(order.getLogicId())
        req.timestamp = datetime.now().timestamp()

        req.cancel_order.order_req_id = str(order.getReqId())
        await self.__req_sock.send(req.SerializeToString())

    async def batchCancel(self, orders: list):
        req = trade_pb2.ReqMessage()
        req.head = trade_pb2.ReqMessage.Head.Value("BATCH_CANCEL")
        for order in orders:
            cancel_order = req.batch_cancel.cance_req.add()

            cancel_order.strategy_id = str(order.getLogicId())
            cancel_order.strategy_order_ref = str(order.getId())
            cancel_order.timestamp = datetime.now().timestamp()

        await self.__req_sock.send(req.SerializeToString())

    async def cancelOrderById(self, req_id: str, logic_id='unknown'):
        req = trade_pb2.ReqMessage()
        req.head = trade_pb2.ReqMessage.Head.Value('ALTER_ORDER')
        req.cancel_order.strategy_id = logic_id
        req.cancel_order.strategy_order_ref = req_id
        req.cancel_order.timestamp = datetime.now().timestamp()
        await self.__req_sock.send(req.SerializeToString())

    async def _monitorResp(self):
        print('trade client {} start receiving'.format(self.__addr))
        while not self.eof():
            try:
                msg = await self.__resp_sock.recv()
                resp = trade_pb2.RespMessage()
                resp.ParseFromString(msg)
                await self.__callback(resp)
            except Exception as e:
                self.stop()
                raise e