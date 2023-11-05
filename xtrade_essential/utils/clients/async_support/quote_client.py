# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

from ..quote_client import *


def getZmqSubSocket():
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
    return socket


class AsyncClient(Client):
    def __init__(self, host, port, pattern=u"", parser='protobuf'):
        Client.__init__(self, host=host, port=port, parser=parser)
        self.__host = host
        self.__port = port
        self.__pattern = pattern
        self.__parser_type = parser

        addr_str = 'tcp://{0}:{1}'.format(self.__host, self.__port)
        self.__sock = getZmqSubSocket()
        self.__sock.connect(addr_str)
        self.__sock.setsockopt_unicode(zmq.SUBSCRIBE, self.__pattern)

        self.__parser = self.loadParser(parser)

    def getSocket(self):
        return self.__sock

    async def recv_data(self):
        raw = await self.__sock.recv()
        # print(raw)
        if self.__parser_type == "protobuf":
            type_, ticker, pb_str = raw.split(b'\t', 2)
            if len(self.getFilter()) and ticker not in self.getFilter():
                return None, None, None

            else:
                pb_ins = quotation_pb2.Message()
                pb_ins.ParseFromString(pb_str)
                return protobufParser(pb_ins)

        elif self.__parser_type == "json":
            quote = json.loads(raw)
            ticker = quote["ticker"]
            if len(self.getFilter()) and ticker not in self.getFilter():
                return None, None, None

            else:
                # print(quote)
                return jsonParser(quote)

        return None, None, None

    async  def recv_raw(self):
        return await self.__sock.recv()

    async def recv_dict(self):
        raw = await self.__sock.recv()
        return self.parseDict(raw)
