# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""
import json

import zmq
from zmq.asyncio import Context
import asyncio

from datetime import datetime
from xtrade_essential.proto import trade_pb2
from xtrade_essential.xlib.protobuf_to_dict import protobuf_to_dict
from xtrade_essential.utils.taskhandler.async_task import TaskHandler
from xtrade_essential.utils.clients.async_support.redis import RedisClient
from xtrade_essential.utils.clients.async_support.mongo import MongoClient
from xtrade_essential.xlib.logger import getLogger


TIMEOUT = 30000
KEY_EXPIRE = 1209600 # 14 days
MSG_PULL_PORT = 55001


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()


class TradeRecorder:
    def __init__(self):
        context = Context()
        pull_addr_str = "tcp://localhost:{}".format(MSG_PULL_PORT)
        self.__pull_req_sock = context.socket(zmq.PULL)
        self.__pull_req_sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.__pull_req_sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        self.__pull_req_sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
        self.__pull_req_sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
        self.__pull_req_sock.connect(pull_addr_str)

        self.__req_redis = list()
        self.__resp_redis = list()
        self.__redis = RedisClient()

        self.__req_mongo = list()
        self.__resp_mongo = list()
        self.__mongo = MongoClient()

        self.__task_h = TaskHandler()

        self.__logger = getLogger('trade_recorder')

    async def monitorMsg(self):
        while 1:
            msg = await self.__pull_req_sock.recv()
            hostname, type_, msg_bytes = msg.split(b'\t', 2)
            print(hostname, type_, msg_bytes)

            if type_ == b"req":
                pb_ins = trade_pb2.ReqMessage()
                pb_ins.ParseFromString(msg_bytes)
                doc = protobuf_to_dict(pb_ins)
                doc['hostname'] = hostname.decode()

                self.__req_redis.append(doc)

            elif type_ == b'resp':
                pb_ins = trade_pb2.RespMessage()
                pb_ins.ParseFromString(msg_bytes)
                doc = protobuf_to_dict(pb_ins)
                doc['hostname'] = hostname.decode()

                self.__resp_redis.append(doc)

            elif type_ == b'snapshot':
                doc = json.loads(msg_bytes)
                doc['hostname'] = hostname.decode()

                await self.updateSnapshotToRedis(doc)

    async def updateSnapshotToRedis(self, snapshot: dict):
        await self.__redis.setSnapshot(
            module="TradeServer",
            id_=snapshot['hostname'],
            value=snapshot,
            timeout=KEY_EXPIRE)

    async def updateReqRespToMongo(self):
        dbname = "tradeserver_log"
        if self.__req_mongo:
            await self.__mongo.bulkInsert(dbname, 'request', self.__req_mongo)
            self.__logger.info(f' {len(self.__req_mongo)} records inserted to {dbname}.request')
            self.__req_mongo.clear()

        if self.__resp_mongo:
            await self.__mongo.bulkInsert(dbname, 'response', self.__resp_mongo)
            self.__logger.info(f' {len(self.__resp_mongo)} records inserted to {dbname}.response')
            self.__resp_mongo.clear()

    async def updateReqRespToRedis(self):
        """ 写入redis之后再存放到mongo

        """
        if not self.__redis.connected:
            await self.__redis.connect()

        if self.__req_redis:
            trade_redis = self.__redis.getTradeConn()

            for doc in self.__req_redis:
                key = f"REQ|||{doc['hostname']}|||{datetime.now().strftime('%Y%m%d')}"
                await trade_redis.rpush(key, json.dumps(doc, default=myconverter))
                await trade_redis.expire(key, KEY_EXPIRE)

                self.__req_mongo.append(doc)

            self.__req_redis.clear()

        if self.__resp_redis:
            trade_redis = self.__redis.getTradeConn()

            for doc in self.__resp_redis:
                key = f"RESP|||{doc['hostname']}|||{datetime.now().strftime('%Y%m%d')}"
                await trade_redis.rpush(key, json.dumps(doc, default=myconverter))
                await trade_redis.expire(key, KEY_EXPIRE)

                self.__resp_mongo.append(doc)

            self.__resp_redis.clear()

    def run_forever(self):
        self.__logger.info(f'monitor msg on port {MSG_PULL_PORT}')
        self.__task_h.addCoroutineTask(self.monitorMsg())

        self.__task_h.runPeriodicAsyncJob(
            interval=3,
            task_func=self.updateReqRespToRedis,
        )

        self.__task_h.runPeriodicAsyncJob(
            interval=10,
            task_func=self.updateReqRespToMongo
        )

        self.__task_h.getLoop().run_forever()


if __name__ == '__main__':
    r = TradeRecorder()
    r.run_forever()

