## coding: utf8

"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import zmq
import zmq.asyncio
import orjson as json

import pytz
from datetime import datetime

from xtrade_essential.utils.quote import *
from xtrade_essential.xlib.protobuf_to_dict import protobuf_to_dict
from xtrade_essential.proto import quotation_pb2


SGD_EXCHANGE_MAPPING = {
    65: "SH",
    70: "SZ",
}


def protoStrParser(pb_str: str):
    pb_ins = quotation_pb2.Message()
    pb_ins.ParseFromString(pb_str)
    return protobufParser(pb_ins)


def protobufParser(pb_ins):
    if pb_ins.data_type == DataType.TICK:
        ticker = pb_ins.tick_body.ticker
        type_ = DataType.BAR
        ret = BasicTick(
            trading_session=pb_ins.trading_session,
            dateTime=datetime.fromtimestamp(pb_ins.tick_body.timestamp),
            open_=pb_ins.tick_body.open,
            high=pb_ins.tick_body.high,
            low=pb_ins.tick_body.low,
            close=pb_ins.tick_body.new_price,
            volume=pb_ins.tick_body.volume,
            amount=pb_ins.tick_body.amount,
            bp=list(pb_ins.tick_body.bps),
            bv=list(pb_ins.tick_body.bvs),
            ap=list(pb_ins.tick_body.aps),
            av=list(pb_ins.tick_body.avs),
            preclose=pb_ins.tick_body.preclose,
            bought_amount=pb_ins.tick_body.bought_amount,
            bought_volume=pb_ins.tick_body.bought_qty,
            sold_amount=pb_ins.tick_body.sold_amount,
            sold_volume=pb_ins.tick_body.sold_qty,
            upper_limit=pb_ins.tick_body.upper_limit,
            lower_limit=pb_ins.tick_body.lower_limit,
            extra={
                "server_sent_time": datetime.fromtimestamp(pb_ins.sending_time),
                "server_recv_time": datetime.fromtimestamp(pb_ins.receiving_time),

                "open_interest": pb_ins.tick_body.open_interest,
                "vwap": pb_ins.tick_body.vwap,
                "bv1_orders": list(pb_ins.tick_body.bv1_orders),
                "av1_orders": list(pb_ins.tick_body.av1_orders),
            },
            frequency=Frequency.TICK
        )

    elif pb_ins.data_type == DataType.BAR:
        ticker = pb_ins.bar_body.ticker
        type_ = DataType.BAR
        ret = BasicBar(
            dateTime=datetime.fromtimestamp(pb_ins.bar_body.timestamp),
            open_=pb_ins.bar_body.open,
            high=pb_ins.bar_body.high,
            low=pb_ins.bar_body.low,
            close=pb_ins.bar_body.close,
            volume=pb_ins.bar_body.volume,
            amount=pb_ins.bar_body.amount,
            frequency=pb_ins.bar_body.frequency,
            extra={
                "server_sent_time": datetime.fromtimestamp(pb_ins.sending_time),
                "server_recv_time": datetime.fromtimestamp(pb_ins.receiving_time),
            }
        )

    elif pb_ins.data_type == DataType.IOPV:
        ticker = pb_ins.iopv_body.ticker
        type_ = DataType.IOPV
        ret = BasicIopv(
            dateTime=datetime.fromtimestamp(pb_ins.iopv_body.update_time),
            new_price=pb_ins.iopv_body.new_price,
            ap1=pb_ins.iopv_body.ap1,
            bp1=pb_ins.iopv_body.bp1,
            ap1_adj=pb_ins.iopv_body.ap1_adj,
            bp1_adj=pb_ins.iopv_body.bp1_adj,
            preclose=pb_ins.iopv_body.preclose,
            min_exchange_unit=pb_ins.iopv_body.min_exchange_unit,
            created_bskt_num=pb_ins.iopv_body.created_bskt_num,
            redeemed_bskt_num=pb_ins.iopv_body.redeemed_bskt_num,
            create_limit=pb_ins.iopv_body.create_limit,
            redeem_limit=pb_ins.iopv_body.redeem_limit,
            limit_up_adjust=pb_ins.iopv_body.limit_up_adjust,
            limit_down_adjust=pb_ins.iopv_body.limit_down_adjust,
            must_halt_adjust=pb_ins.iopv_body.must_halt_adjust,
            non_must_halt_adjust=pb_ins.iopv_body.non_must_halt_adjust,
            extra={
                "server_sent_time": datetime.fromtimestamp(pb_ins.sending_time),
                "server_recv_time": datetime.fromtimestamp(pb_ins.receiving_time),
                "last_quote_time": datetime.fromtimestamp(pb_ins.iopv_body.quote_time),

                "limit_up_adjust_detail": list(pb_ins.iopv_body.limit_up_adjust_detail),
                "limit_down_adjust_detail": list(pb_ins.iopv_body.limit_down_adjust_detail),
                "must_halt_adjust_detail": list(pb_ins.iopv_body.must_halt_adjust_detail),
                "non_must_halt_adjust_detail": list(pb_ins.iopv_body.non_must_halt_adjust_detail),
            },
        )

    elif pb_ins.data_type == DataType.MD_TRADE:
        ticker = pb_ins.md_trade.ticker
        type_ = DataType.MD_TRADE
        ret = BasicTrade(
            dateTime=datetime.fromtimestamp(pb_ins.md_trade.transact_time),
            id_=pb_ins.md_trade.exec_id,
            price=pb_ins.md_trade.trade_price,
            volume=pb_ins.md_trade.trade_quantity,
            type_=pb_ins.md_trade.exec_type,
            side=pb_ins.md_trade.side,
            extra={
                "server_sent_time": datetime.fromtimestamp(pb_ins.sending_time),
                "server_recv_time": datetime.fromtimestamp(pb_ins.receiving_time),
            }
        )

    elif pb_ins.data_type == DataType.DEPTH:
        ticker = pb_ins.depth_body.ticker
        type_ = DataType.DEPTH
        ret = BasicDepth(
            dateTime=datetime.fromtimestamp(pb_ins.depth_body.timestamp),
            trading_session=pb_ins.trading_session,
            bp=list(pb_ins.depth_body.bps),
            bv=list(pb_ins.depth_body.bvs),
            ap=list(pb_ins.depth_body.aps),
            av=list(pb_ins.depth_body.avs),
            extra={
                "server_sent_time": datetime.fromtimestamp(pb_ins.sending_time),
                "server_recv_time": datetime.fromtimestamp(pb_ins.receiving_time),
            }
        )

    else:
        return None, None, None

    return ticker, type_, ret


def jsonParser(quote: dict):
    try:
        # print(quote["data_type"], DataType.TICK)
        if quote["data_type"] == DataType.TICK:
           ret = BasicTick(
               trading_session=quote.get("trading_session", TradingPhase.TRADING),
               dateTime=datetime.fromtimestamp(quote['dt']),
               open_=quote.get('open', 0),
               high=quote.get('high', 0),
               low=quote.get('low', 0),
               close=quote.get('close', 0),
               volume=quote.get('volume', 0),
               amount=quote.get('amount', 0),
               bp=list(quote.get('bps', list())),
               bv=list(quote.get('bvs', list())),
               ap=list(quote.get('aps', list())),
               av=list(quote.get('avs', list())),
               preclose=quote.get("preclose", 0),
               bought_amount=quote.get('bought_amount', 0),
               sold_amount=quote.get("sold_amount"),
               upper_limit=quote.get("upper_limit"),
               lower_limit=quote.get("lower_limit"),
               extra=quote,
               frequency=Frequency.TICK,
               bought_volume=quote.get('bought_volume', 0),
               sold_volume=quote.get('sold_volume', 0),
           )

        else:
            return None, None, None

        return quote["ticker"], DataType.TICK, ret

    except Exception as e:
        print(quote)
        raise e


def sgdJsonParser(quote: dict):
    dt_str = f"{quote['TradingDay']} {quote['UpdateTime']}"
    dt = datetime.strptime(dt_str, "%Y%m%d %H:%M:%S")
    tz = pytz.timezone('Asia/Shanghai')
    dt = tz.localize(dt).astimezone(pytz.utc).replace(tzinfo=None)

    quote["AskPrice"].reverse()
    quote["AskVolume"].reverse()
    ret = BasicTick(
        trading_session=quote.get("trading_session", TradingPhase.TRADING),
        dateTime=dt,
        open_=quote.get('OpenPrice', 0),
        high=quote.get('HighPrice', 0),
        low=quote.get('LowPrice', 0),
        close=quote.get('LastPrice', 0),
        volume=quote.get('Volume', 0),
        amount=quote.get('Turnover', 0),
        bp=list(quote.get('BidPrice', list())),
        bv=list(quote.get('BidVolume', list())),
        ap=list(quote.get('AskPrice', list())),
        av=list(quote.get('AskVolume', list())),
        preclose=quote.get("PreClose", 0),
        bought_amount=0,
        sold_amount=0,
        upper_limit=quote.get("UpperLimitPrice"),
        lower_limit=quote.get("LowerLimitPrice"),
        extra=quote,
        frequency=Frequency.TICK,
        bought_volume=0,
        sold_volume=0,
    )
    return ret


def dictParser(json_str):
    quote = json.loads(json_str)
    quote['datetime'] = datetime.fromtimestamp(quote['dt'])
    return quote


def getZmqSubPort():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)

    return socket


class Client(object):
    def __init__(self, host, port, parser='protobuf'):
        self.__parser_type = parser
        self.__parser = self.loadParser(parser)

        addr_str = '{0}:{1}'.format(host, port)
        self.__sock = getZmqSubPort()
        self.__sock.setsockopt_string(zmq.SUBSCRIBE,  u"")
        print('tcp://%s'%(addr_str))
        self.__sock.connect('tcp://%s'%(addr_str))
        self.__filter = set()

    def loadParser(self, parser):
        if parser == 'protobuf':
            return protobufParser
        elif parser == 'json':
            return jsonParser

        elif parser == 'sgdJson':
            return sgdJsonParser

    def getFilter(self):
        return self.__filter

    def recv_raw(self):
        return self.__sock.recv()

    def recv_dict(self) -> dict:
        raw = self.__sock.recv()
        return self.parseDict(raw)

    def parseDict(self, raw: bytes) -> dict:
        if self.__parser == protobufParser:
            type_, ticker, pb_str = raw.split(b'\t', 2)

            pb_ins = quotation_pb2.Message()
            pb_ins.ParseFromString(pb_str)

            if pb_ins.data_type == DataType.TICK:
                ret = protobuf_to_dict(pb_ins.tick_body)
                ret['datetime'] = ret["timestamp"]

            elif pb_ins.data_type == DataType.BAR:
                ret = protobuf_to_dict(pb_ins.bar_body)
                ret['datetime'] = ret["timestamp"]

            elif pb_ins.data_type == DataType.IOPV:
                ret = protobuf_to_dict(pb_ins.iopv_body)
                ret['datetime'] = ret['update_time']

            elif pb_ins.data_type == DataType.MD_TRADE:
                ret = protobuf_to_dict(pb_ins.md_trade)
                ret['datetime'] = ret["transact_time"]

            elif pb_ins.data_type == DataType.MD_ORDER:
                ret = protobuf_to_dict(pb_ins.md_order)
                ret['datetime'] = ret["transact_time"]

            elif pb_ins.data_type == DataType.DEPTH:
                ret = protobuf_to_dict(pb_ins.depth_body)
                ret['datetime'] = ret["timestamp"]

            else:
                raise Exception('invalid pb_str: {}'.format(pb_str))

            ret['data_type'] = pb_ins.data_type
            ret['server_sent_time'] = pb_ins.sending_time
            ret['server_recv_time'] = pb_ins.receiving_time

        elif self.__parser == jsonParser:
            ret = json.loads(raw)
            ret["datetime"] = ret["dt"]

        elif self.__parser == sgdJsonParser:
            if len(raw) < 3:
                return None

            quote = json.loads(raw)
            dt_str = f"{quote['TradingDay']} {quote['UpdateTime']}"
            quote["AskPrice"].reverse()
            quote["AskVolume"].reverse()
            ret = dict(
                trading_session=quote.get("trading_session", TradingPhase.TRADING),
                type='S',
                data_type=DataType.TICK,
                datetime=datetime.strptime(dt_str, "%Y%m%d %H:%M:%S").timestamp(),
                open=quote.get('OpenPrice', 0),
                high=quote.get('LowestPrice', 0),
                low=quote.get('HighestPrice', 0),
                close=quote.get('LastPrice', 0),
                volume=quote.get('Volume', 0),
                amount=quote.get('Turnover', 0),
                bps=list(quote.get('BidPrice', list())),
                bvs=list(quote.get('BidVolume', list())),
                aps=list(quote.get('AskPrice', list())),
                avs=list(quote.get('AskVolume', list())),
                preclose=quote.get("PreClosePrice", 0),
                bought_amount=0,
                sold_amount=0,
                upper_limit=quote.get("UpperLimitPrice"),
                lower_limit=quote.get("LowerLimitPrice"),
                extra={},
                frequency=Frequency.TICK,
                bought_volume=0,
                sold_volume=0,
                ticker=f"{quote['Code']}.{SGD_EXCHANGE_MAPPING[quote['ExchangeID']]}",
                client_recv_time=datetime.now().timestamp()
            )

        else:
            raise Exception("invalid parser type: {}".format(self.__parser))

        ret["client_recv_time"] = datetime.now().timestamp()
        return ret

    def recv_data(self):
        raw = self.__sock.recv()
        if self.__parser_type == "protobuf":
            type_, ticker, pb_str = raw.split(b'\t', 2)
            if len(self.__filter) and ticker not in self.__filter:
                return None, None, None

            else:
                pb_ins = quotation_pb2.Message()
                pb_ins.ParseFromString(pb_str)
                return protobufParser(pb_ins)

        elif self.__parser_type == "json":
            quote = json.loads(raw)
            ticker = quote["ticker"].encode()
            # if ticker == b"510300.SH":
            #     print(ticker, self.__filter, ticker in self.__filter)

            if len(self.__filter) and ticker not in self.__filter:
                return None, None, None

            else:
                return jsonParser(quote)

        elif self.__parser_type == 'sgdJson':
            if len(raw) < 3:
                return None, None, None

            quote = json.loads(raw)
            ticker = f"{quote['Code']}.{SGD_EXCHANGE_MAPPING[quote['ExchangeID']]}"

            if len(self.__filter) and ticker not in self.__filter:
                return None, None, None

            else:
                return ticker, DataType.TICK, sgdJsonParser(quote)

        return None, None, None

    def setTickerFilter(self, tickers: set):
        self.__filter = tickers


if __name__ == '__main__':
    c = Client(host='127.0.0.1', port='5556', parser='sgdJson')
    while 1:
        # r = c.recv_raw()
        # print(r)

        ticker, _, tick = c.recv_data()
        if ticker and ticker.find("600008") >= 0:
            print(ticker, tick.getAp1(), tick.getClose())
        #
        # quote_dict = c.recv_dict()
        # print(quote_dict)



