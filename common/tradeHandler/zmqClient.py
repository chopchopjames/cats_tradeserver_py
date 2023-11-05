
# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import zmq
from zmq.asyncio import Context


class AsyncClient(object):
    def __init__(self, req_port, resp_port):
        pull_context = Context()
        pull_addr_str = "tcp://*:{}".format(req_port)
        self.__pull_sock = pull_context.socket(zmq.PULL)
        self.__pull_sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.__pull_sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        self.__pull_sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
        self.__pull_sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
        self.__pull_sock.bind(pull_addr_str)

        pub_context = Context()
        pub_addr_str = "tcp://*:{}".format(resp_port)
        self.__pub_sock = pub_context.socket(zmq.PUB)
        self.__pub_sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.__pub_sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        self.__pub_sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
        self.__pub_sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
        self.__pub_sock.bind(pub_addr_str)

        # log server
        log_context = Context()
        self.__log_sock = log_context.socket(zmq.PUSH)
        log_addr_str = f"tcp://*:55001"
        self.__log_sock.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.__log_sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        self.__log_sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
        self.__log_sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
        self.__log_sock.bind(log_addr_str)

    async def recv(self):
        msg = await self.__pull_sock.recv()
        return msg

    async def send(self, data):
        await self.__pub_sock.send(data)

    async def pushLog(self, hostname: str, type_: str, msg_str: bytes):
        await self.__log_sock.send(f"{hostname}\t{type_}\t".encode() + msg_str)


