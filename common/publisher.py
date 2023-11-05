# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import os
import zmq
import zmq.asyncio
import asyncio
import queue

from datetime import datetime

from xtrade_essential.proto import quotation_pb2
from .taskhandler import async_task
from common import data_type as SecurityType


PUSH_HOST = os.environ.get("ZMQ_PUSH_HOST")
PUSH_PORT = os.environ.get("ZMQ_PUSH_PORT")

def get_push_socket(host, port):
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.PUSH)
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
    socket.bind(f"tcp://{host}:{port}")
    return socket


class AsyncPublisher:
    def __init__(self):
        self.__tick_info = dict()
        self.__socket = get_push_socket(host=PUSH_HOST, port=PUSH_PORT)

        self.__count = 0
        self.__task_handler = async_task.TaskHandler()
        self.__quote_q = queue.Queue()

        self.__task_handler.addCoroutineTask(self._pubQuoteTask())

    def getTaskHandler(self) -> async_task.TaskHandler:
        return self.__task_handler

    # @do_profile(follow=[])
    def onTick(self, ticker: str, timestamp: float, open: float, high: float, low: float, close: float, volume: float,
               turnover: float, bought_turnover: float=None, sold_turnover=None, bought_qty: float=None, sold_qty: float=None,
               vwap_buy: float=None, vwap_sell: float=None, withdraw_buy_number: int=None, withdraw_sell_number: int=None,
               withdraw_buy_money: float=None, withdraw_sell_money: float=None, withdraw_buy_qty: float=None,
               withdraw_sell_qty: float=None, total_buy_number: int=None, total_sell_number: int=None,
               buy_trade_max_duration: int=None, sell_trade_max_duration: int=None, num_buy_orders: int=None,
               num_sell_orders: int=None, norminal_price: float=None, short_sell_shares_traded: float=None,
               short_sell_turnover: float=None, ref_price: float=None, complex_event_start_time: float=None,
               complex_event_end_time: float=None, channel_no: int=None, iopv: float=None, purchase_number: int=None,
               purchase_qty: float=None, purchase_money: float=None, redemption_number: int=None,
               redemption_qty: float=None, redemption_money: float=None, number_of_trades: int=None,
               open_interest: float=None, upper_limit: float=None, lower_limit: float=None, vwap=None,
               curr_delta: float=None, settlement_price: float=None, aps: list=[], avs: list=[], bps: list=[],
               bvs: list=[], bv1_orders: list=[], av1_orders: list=[], bid_ids: list=[], ask_ids: list=[],
               security_type=SecurityType.SecurityType.OTHER, trading_phase=SecurityType.TradingPhase.TRADING,
               pre_close: float = None,
               ):

        pb_ins = quotation_pb2.Message()
        pb_ins.receiving_time = datetime.now().timestamp()
        pb_ins.data_type = SecurityType.DataType.TICK
        pb_ins.security_type = security_type
        pb_ins.trading_session = trading_phase
        pb_ins.tick_body.ticker = ticker
        pb_ins.tick_body.timestamp = timestamp
        pb_ins.tick_body.open = open
        pb_ins.tick_body.close = close  # TODO: close的含义是收盘价，现在有兼容性问题，以后更改
        pb_ins.tick_body.new_price = close
        pb_ins.tick_body.high = high
        pb_ins.tick_body.low = low
        pb_ins.tick_body.volume = volume
        pb_ins.tick_body.amount = turnover

        if pre_close:
            pb_ins.tick_body.preclose = pre_close

        if bought_turnover:
            pb_ins.tick_body.bought_amount = bought_turnover
            pb_ins.tick_body.sold_amount = sold_turnover

        if bought_qty:
            pb_ins.tick_body.bought_qty = bought_qty
            pb_ins.tick_body.sold_qty = sold_qty

        if vwap_buy:
            pb_ins.tick_body.vwap_buy = vwap_buy
            pb_ins.tick_body.vwap_sell = vwap_sell

        if withdraw_buy_money:
            pb_ins.tick_body.withdraw_buy_money = withdraw_buy_money
            pb_ins.tick_body.withdraw_sell_money = withdraw_sell_money

        if withdraw_buy_qty:
            pb_ins.tick_body.withdraw_sell_qty = withdraw_sell_qty
            pb_ins.tick_body.withdraw_buy_qty = withdraw_buy_qty

        if withdraw_buy_number:
            pb_ins.tick_body.withdraw_buy_number = withdraw_buy_number
            pb_ins.tick_body.withdraw_sell_number = withdraw_sell_number

        if total_buy_number:
            pb_ins.tick_body.total_buy_number = total_buy_number
            pb_ins.tick_body.total_sell_number = total_sell_number

        if buy_trade_max_duration:
            pb_ins.tick_body.buy_trade_max_duration = buy_trade_max_duration
            pb_ins.tick_body.sell_trade_max_duration = sell_trade_max_duration

        if num_buy_orders:
            pb_ins.tick_body.num_buy_orders = num_buy_orders
            pb_ins.tick_body.num_sell_orders = num_sell_orders

        if norminal_price:
            pb_ins.tick_body.norminal_price = norminal_price

        if short_sell_shares_traded:
            pb_ins.tick_body.short_sell_shares_traded = short_sell_shares_traded
            pb_ins.tick_body.short_sell_turnover = short_sell_turnover

        if ref_price:
            pb_ins.tick_body.ref_price = ref_price

        if complex_event_start_time:
            pb_ins.tick_body.complex_event_end_time = complex_event_end_time
            pb_ins.tick_body.complex_event_start_time = complex_event_start_time

        if channel_no:
            pb_ins.tick_body.channel_no = str(channel_no)

        if iopv:
            pb_ins.tick_body.iopv = iopv

        if purchase_number:
            pb_ins.tick_body.purchase_number = purchase_number
            pb_ins.tick_body.purchase_qty = purchase_qty
            pb_ins.tick_body.purchase_money = purchase_money

        if redemption_number:
            pb_ins.tick_body.redemption_number = redemption_number
            pb_ins.tick_body.redemption_qty = redemption_qty
            pb_ins.tick_body.redemption_moeny = redemption_money

        if number_of_trades:
            pb_ins.tick_body.number_of_trades = number_of_trades

        if open_interest:
            pb_ins.tick_body.open_interest = open_interest

        if upper_limit:
            pb_ins.tick_body.upper_limit = upper_limit
            pb_ins.tick_body.lower_limit = lower_limit

        if vwap:
            pb_ins.tick_body.vwap = vwap

        if curr_delta:
            pb_ins.tick_body.curr_delta = curr_delta

        if settlement_price:
            pb_ins.tick_body.settlement_price = settlement_price

        if bid_ids:
            pb_ins.tick_body.bid_ids.extend(bid_ids)

        if ask_ids:
            pb_ins.tick_body.ask_ids.extend(ask_ids)

        pb_ins.tick_body.bps.extend(bps)
        pb_ins.tick_body.bvs.extend(bvs)

        pb_ins.tick_body.aps.extend(aps)
        # pb_ins.tick_body.aps = aps
        pb_ins.tick_body.avs.extend(avs)

        if bv1_orders:
            pb_ins.tick_body.bv1_orders.extend(bv1_orders)
            pb_ins.tick_body.av1_orders.extend(av1_orders)

        # print(pb_ins)
        self._send(ticker, pb_ins)

    def _send(self, ticker, pb_ins):
        pb_ins.sending_time = datetime.now().timestamp()
        type_str = SecurityType.DataType.toString(pb_ins.data_type)
        to_pub = type_str.encode() + b'\t' + ticker.encode() + b'\t' + pb_ins.SerializeToString()
        # print(to_pub)
        self.__quote_q.put(to_pub)

    async def _pubQuoteTask(self):
        while 1:
            quote = self.__quote_q.get()
            await self.__socket.send(quote)
            # print(quote)
            self.__count += 1
            if self.__count % 1000 == 0:
                print("{} quote published".format(self.__count))
                print("queue size: {}".format(self.__quote_q.qsize()))
