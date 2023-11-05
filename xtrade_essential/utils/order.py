# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

__all__ = ["OrderExecutionInfo",
           "Order",
           "LimitOrder",
           "BatchOrder",
           "InstrumentTraits",
           "EtfConvertRequest",
           "BaseRequest"
           ]

import json
import abc
import uuid
import numpy as np

from datetime import datetime
from xtrade_essential.proto import trade_pb2

from pyalgotrade.broker import InstrumentTraits as BaseTrait


class InstrumentTraits(BaseTrait):
    def __init__(self, trade_unit, price_tick, maker_fee, taker_fee,
                 quote_precision, volume_precision, min_limit_order_volume,
                 max_limit_order_volume, base, quote, is_derivative,
                 exchange_name):
        """

        :param trade_unit:
        :param price_tick:
        :param maker_fee:
        :param taker_fee:
        :param quote_precision:
        :param volume_precision:
        :param min_limit_order_volume:
        :param max_limit_order_volume:
        :param base:
        :param quote:
        :param is_derivative:
        """

        self.__trade_unit = trade_unit
        self.__price_tick = price_tick
        self.__maker_fee = maker_fee
        self.__taker_fee = taker_fee
        self.__quote_precision = int(quote_precision)
        self.__volume_precision = int(volume_precision)
        self.__min_limit_order_volume = min_limit_order_volume
        self.__max_limit_order_volume = max_limit_order_volume
        self.__base = base
        self.__quote = quote
        self.__is_derivative = is_derivative
        self.__exchange_name = exchange_name

    def roundOrderQuantity(self, quantity):
        return round(quantity / self.__min_limit_order_volume) * self.__min_limit_order_volume

    def roundTradeQuantity(self, quantity):
        return round(quantity / self.__trade_unit) * self.__trade_unit

    def roundQuantity(self, quantity):
        return self.roundOrderQuantity(quantity)

    def roundPrice(self, price):
        price = round(price / self.__price_tick) * self.__price_tick
        return round(price, self.__quote_precision)

    def getPriceTick(self):
        return self.__price_tick

    def getTradeUnit(self):
        return self.__trade_unit

    def getMinimumOpen(self):
        return self.__min_limit_order_volume

    def getMaxOpen(self):
        return self.__max_limit_order_volume

    def getBaseSymbol(self):
        return self.__base

    def getQuoteSymbol(self):
        return self.__quote

    def isDerivative(self):
        return self.__is_derivative

    def getExchange(self):
        return self.__exchange_name


class ConvertBondTraits(InstrumentTraits):
    def __init__(self, underlying, convert_price, **kwargs):
        InstrumentTraits.__init__(self, **kwargs)

        self.__underlying = underlying
        self.__convert_price = convert_price

    def getConvertPrice(self):
        return self.__convert_price

    def getUnderlying(self):
        return self.__underlying


class FutureTraits(InstrumentTraits):
    def __init__(self, underlying, contract_size, **kwargs):
        InstrumentTraits.__init__(self, **kwargs)

        self.__underlying = underlying
        self.__contract_size = contract_size

    def getContractSize(self):
        return self.__contract_size

    def getUnderlying(self):
        return self.__underlying


class OptionTraits(InstrumentTraits):
    class Type:
        CALL = 'c'
        PUT = 'p'

    class Style:
        European = "European"
        American = "American"

    def __init__(self, underlying, contract_size, strike, expire_time: datetime, option_type, exercise_style, **kwargs):
        InstrumentTraits.__init__(self, **kwargs)
        assert(option_type in ('c', 'p'))
        assert(exercise_style in ("American", "European"))
        assert(strike is not None)

        self.__underlying = underlying
        self.__contract_size = contract_size
        self.__strike = strike
        self.__expire_time = expire_time
        self.__option_type = option_type
        self.__exercise_style = exercise_style

    def getOptionType(self):
        return self.__option_type

    def getOptionStyle(self):
        return self.__exercise_style

    def getContractSize(self):
        return self.__contract_size

    def getUnderlying(self):
        return self.__underlying

    def getStrike(self):
        return self.__strike

    def getExpireTime(self):
        return self.__expire_time


class BaseRequest(object):
    __metaclass__ = abc.ABCMeta
    def __init__(self):
        self.__create_id = str(uuid.uuid4())
        self.__req_id = None
        self.__req_submit_time = None

        self.__cust_id = None
        self.__exchange_id = None
        self.__exchange_accepted_time = None

        self.__logic_id = "unknown"

    @abc.abstractmethod
    def getReq(self) -> trade_pb2.ReqMessage:
        raise NotImplementedError

    def getCreateId(self):
        return self.__create_id

    def setExchangeId(self, exchange_id: str, accepted_time):
        self.__exchange_id = exchange_id
        self.__exchange_accepted_time = accepted_time

    def getExchangeId(self):
        return self.__exchange_id

    def getExchangeAcceptedTime(self):
        return self.__exchange_accepted_time

    def setCustId(self, cust_id: str):
        self.__cust_id = cust_id

    def getCustId(self) -> str:
        return self.__cust_id

    def setSubmitted(self, req_id: str, dateTime: datetime):
        assert(self.__req_id is None or req_id == self.__req_id)

        self.__req_id = req_id
        self.__req_submit_time = dateTime

    def getReqId(self):
        return self.__req_id

    def getSubmitDateTime(self):
        """Returns the datetime when the order was submitted."""
        return self.__req_submit_time

    def setLogicId(self, logic_id: str):
        self.__logic_id = logic_id

    def getLogicId(self):
        return self.__logic_id


class Order(object):
    """Base class for orders.

    :param type_: The order type
    :type type_: :class:`Order.Type`
    :param action: The order action.
    :type action: :class:`Order.Action`
    :param instrument: Instrument identifier.
    :type instrument: string.
    :param quantity: Order quantity.
    :type quantity: int/float.

    .. note::
        This is a base class and should not be used directly.

        Valid **type** parameter values are:

         * Order.Type.MARKET
         * Order.Type.LIMIT
         * Order.Type.STOP
         * Order.Type.STOP_LIMIT

        Valid **action** parameter values are:

         * Order.Action.BUY
         * Order.Action.BUY_TO_COVER
         * Order.Action.SELL
         * Order.Action.SELL_SHORT
    """
    __metaclass__ = abc.ABCMeta

    class Action(object):
        BUY = trade_pb2.OrderAction.Value('BUY')
        BUY_TO_COVER = trade_pb2.OrderAction.Value('BUY_TO_COVER')
        SELL = trade_pb2.OrderAction.Value('SELL')
        SELL_SHORT = trade_pb2.OrderAction.Value('SELL_SHORT')

        @classmethod
        def toString(cls, action):
            if action == cls.BUY:
                return "BUY"
            elif action == cls.SELL_SHORT:
                return "SELL_SHORT"
            elif action == cls.SELL:
                return "SELL"
            elif action == cls.BUY_TO_COVER:
                return "BUY_TO_COVER"

    class State(object):
        INITIAL = trade_pb2.OrderState.Value('INITIAL')  # Initial state.
        SUBMITTED = trade_pb2.OrderState.Value('SUBMITTED')  # Order has been submitted.
        ACCEPTED = trade_pb2.OrderState.Value('ACCEPTED')  # Order has been acknowledged by the broker.
        CANCELING = trade_pb2.OrderState.Value('CANCELING')
        CANCELED = trade_pb2.OrderState.Value('CANCELED')  # Order has been canceled.
        PARTIALLY_FILLED = trade_pb2.OrderState.Value('PARTIALLY_FILLED')  # Order has been partially filled.
        FILLED = trade_pb2.OrderState.Value('FILLED')  # Order has been completely filled.
        TO_BE_CANCELED = trade_pb2.OrderState.Value('TO_BE_CANCELED')

        @classmethod
        def toString(cls, state):
            if state == cls.INITIAL:
                return "INITIAL"
            elif state == cls.SUBMITTED:
                return "SUBMITTED"
            elif state == cls.ACCEPTED:
                return "ACCEPTED"
            elif state == cls.CANCELING:
                return 'CANCELING'
            elif state == cls.CANCELED:
                return "CANCELED"
            elif state == cls.PARTIALLY_FILLED:
                return "PARTIALLY_FILLED"
            elif state == cls.FILLED:
                return "FILLED"
            elif state == cls.TO_BE_CANCELED:
                return 'TO_BE_CANCELED'
            else:
                raise Exception("Invalid state")

    class Type(object):
        MARKET = trade_pb2.OrderType.Value('MARKET')
        LIMIT = trade_pb2.OrderType.Value('LIMIT')
        STOP = trade_pb2.OrderType.Value('STOP')
        STOP_LIMIT = trade_pb2.OrderType.Value('STOP_LIMIT')

    class Purpose:
        INIT = trade_pb2.OrderReq.Purpose.Value("INIT")
        HEDGE = trade_pb2.OrderReq.Purpose.Value("HEDGE")

        @classmethod
        def toString(cls, purpose):
            if purpose == cls.INIT:
                return "INIT"
            elif purpose == cls.HEDGE:
                return "HEDGE"

    class PriceType:
        PRICE = trade_pb2.OrderReq.PriceType.Value("PRICE")
        IMPLIED_VOL = trade_pb2.OrderReq.PriceType.Value("IMPLIED_VOL")

        @classmethod
        def toString(cls, price_type):
            if price_type == cls.PRICE:
                return "PRICE"
            elif price_type == cls.IMPLIED_VOL:
                return "IMPLIED_VOL"

    class CtpOffsetFlag:
        AUTO = trade_pb2.CtpOffsetFlag.Value("AUTO")
        OPEN = trade_pb2.CtpOffsetFlag.Value("OPEN")
        CLOSE = trade_pb2.CtpOffsetFlag.Value("CLOSE")

        @classmethod
        def toString(cls, flag):
            if flag == cls.AUTO:
                return "AUTO"
            elif flag == cls.OPEN:
                return "OPEN"
            elif flag == cls.CLOSE:
                return "CLOSE"

    # Valid state transitions.
    VALID_TRANSITIONS = {
        State.INITIAL: [State.SUBMITTED, State.CANCELED, State.TO_BE_CANCELED],
        State.SUBMITTED: [State.ACCEPTED, State.CANCELING, State.CANCELED, State.TO_BE_CANCELED],
        State.ACCEPTED: [State.PARTIALLY_FILLED, State.FILLED, State.CANCELING, State.CANCELED, State.TO_BE_CANCELED],
        State.PARTIALLY_FILLED: [State.PARTIALLY_FILLED, State.FILLED, State.CANCELING, State.CANCELED, State.TO_BE_CANCELED],
        State.CANCELING: [State.FILLED, State.CANCELED, State.PARTIALLY_FILLED, State.ACCEPTED, State.TO_BE_CANCELED],
        State.TO_BE_CANCELED: [State.CANCELED, State.FILLED, State.CANCELING, State.PARTIALLY_FILLED]
    }

    def __init__(self, type_, action, ticker, quantity, instrumentTraits: InstrumentTraits):
        if quantity <= 0:
            raise Exception("Invalid quantity")

        self.__id = str(uuid.uuid4())
        self.__type = type_
        self.__action = action
        self.__ticker = ticker
        self.__quantity = quantity
        self.__instrumentTraits = instrumentTraits
        self.__filled = 0
        self.__avgFillPrice = None
        self.__executionInfo = list()
        self.__goodTillCanceled = False
        self.__commissions = 0
        self.__allOrNone = False
        self.__state = Order.State.INITIAL

        self.__submitDateTime = None

        self.__cancel_time = None

        self.__msg = None

    # This is to check that orders are not compared directly. order ids should be compared.
    def __eq__(self, other):
        if not isinstance(other, Order):
            return False

        return self.getId() == other.getId()

    # This is to check that orders are not compared directly. order ids should be compared.
    def __ne__(self, other):
        if not isinstance(other, Order):
            return False

        return self.getId() != other.getId()

    def setMsg(self, msg: str):
        self.__msg = msg

    def getMsg(self):
        return self.__msg

    def getTicker(self):
        return self.__ticker

    def getInstrumentTraits(self):
        return self.__instrumentTraits

    def getId(self):
        """
        Returns the order id.

        .. note::

            This will be None if the order was not submitted.
        """
        return self.__id

    def getType(self):
        """Returns the order type. Valid order types are:

         * Order.Type.MARKET
         * Order.Type.LIMIT
         * Order.Type.STOP
         * Order.Type.STOP_LIMIT
        """
        return self.__type

    def getAction(self):
        """Returns the order action. Valid order actions are:

         * Order.Action.BUY
         * Order.Action.BUY_TO_COVER
         * Order.Action.SELL
         * Order.Action.SELL_SHORT
        """
        return self.__action

    def getState(self):
        """Returns the order state. Valid order states are:

         * Order.State.INITIAL (the initial state).
         * Order.State.SUBMITTED
         * Order.State.ACCEPTED
         * Order.State.CANCELED
         * Order.State.PARTIALLY_FILLED
         * Order.State.FILLED
        """
        return self.__state

    def isActive(self):
        """Returns True if the order is active."""
        return self.__state not in [Order.State.CANCELED, Order.State.FILLED]

    def isInitial(self):
        """Returns True if the order state is Order.State.INITIAL."""
        return self.__state == Order.State.INITIAL

    def isSubmitted(self):
        """Returns True if the order state is Order.State.SUBMITTED."""
        return self.__state == Order.State.SUBMITTED

    def isAccepted(self):
        """Returns True if the order state is Order.State.ACCEPTED."""
        return self.__state == Order.State.ACCEPTED

    def isCanceling(self):
        return self.__state == Order.State.CANCELING

    def isCanceled(self):
        """Returns True if the order state is Order.State.CANCELED."""
        return self.__state == Order.State.CANCELED

    def isPartiallyFilled(self):
        """Returns True if the order state is Order.State.PARTIALLY_FILLED."""
        return self.__state == Order.State.PARTIALLY_FILLED

    def isFilled(self):
        """Returns True if the order state is Order.State.FILLED."""
        return self.__state == Order.State.FILLED

    def getInstrument(self):
        """Returns the instrument identifier."""
        return self.__ticker

    def getQuantity(self):
        """Returns the quantity."""
        return self.__quantity

    def getFilled(self):
        """Returns the number of shares that have been executed."""
        return self.__filled

    def getLastFilled(self):
        return self.getExecutionInfo().getQuantity()

    def getRemaining(self):
        """Returns the number of shares still outstanding."""
        return self.__instrumentTraits.roundTradeQuantity(self.__quantity - self.__filled)

    def getAvgFillPrice(self):
        """Returns the average price of the shares that have been executed, or None if nothing has been filled."""
        return self.__avgFillPrice

    def getCommissions(self):
        return self.__commissions

    def getGoodTillCanceled(self):
        """Returns True if the order is good till canceled."""
        return self.__goodTillCanceled

    def setGoodTillCanceled(self, goodTillCanceled):
        """Sets if the order should be good till canceled.
        Orders that are not filled by the time the session closes will be will be automatically canceled
        if they were not set as good till canceled

        :param goodTillCanceled: True if the order should be good till canceled.
        :type goodTillCanceled: boolean.

        .. note:: This can't be changed once the order is submitted.
        """
        if self.__state != Order.State.INITIAL:
            raise Exception("The order has already been submitted")
        self.__goodTillCanceled = goodTillCanceled

    def getAllOrNone(self):
        """Returns True if the order should be completely filled or else canceled."""
        return self.__allOrNone

    def setAllOrNone(self, allOrNone):
        """Sets the All-Or-None property for this order.

        :param allOrNone: True if the order should be completely filled.
        :type allOrNone: boolean.

        .. note:: This can't be changed once the order is submitted.
        """
        if self.__state != Order.State.INITIAL:
            raise Exception("The order has already been submitted")
        self.__allOrNone = allOrNone

    def addExecutionInfo(self, orderExecutionInfo):
        if orderExecutionInfo.getQuantity() > self.getRemaining():
            raise Exception("Invalid fill size. %s remaining and %s filled" % (self.
                                                                               getRemaining(), orderExecutionInfo.getQuantity()))

        if self.__avgFillPrice is None:
            self.__avgFillPrice = orderExecutionInfo.getPrice()
        else:
            self.__avgFillPrice = (self.
                                   __avgFillPrice * self.__filled + orderExecutionInfo.getPrice() * orderExecutionInfo.getQuantity()) / float(self.
                                                                                                                                              __filled + orderExecutionInfo.getQuantity())

        self.__executionInfo.append(orderExecutionInfo)
        self.__filled = self.getInstrumentTraits().roundTradeQuantity(self.__filled + orderExecutionInfo.getQuantity())
        self.__commissions += orderExecutionInfo.getCommission()

        if np.isclose(self.getRemaining(), 0):
            self.switchState(Order.State.FILLED)
        else:
            assert(not self.__allOrNone)
            self.switchState(Order.State.PARTIALLY_FILLED)

    def switchState(self, newState):
        validTransitions = Order.VALID_TRANSITIONS.get(self.__state, [])
        if newState not in validTransitions:
            raise Exception("Invalid order state transition from %s to %s" % (Order.
                                                                              State.toString(self.__state), Order.State.toString(newState)))
        else:
            self.__state = newState

    def setState(self, newState):
        self.__state = newState

    def getExecutionInfo(self):
        """Returns the execution information for this order, or None if nothing has been filled so far.
        This will be different every time an order, or part of it, gets filled.

        :rtype: :class:`OrderExecutionInfo`.
        """
        return self.__executionInfo[-1]

    def getAllExcutionInfo(self):
        return self.__executionInfo

    # Returns True if this is a BUY or BUY_TO_COVER order.
    def isBuy(self):
        return self.__action in [Order.Action.BUY, Order.Action.BUY_TO_COVER]

    # Returns True if this is a SELL or SELL_SHORT order.
    def isSell(self):
        return self.__action in [Order.Action.SELL, Order.Action.SELL_SHORT]


class LimitOrder(Order, BaseRequest):
    """Base class for limit orders.

    .. note::

        This is a base class and should not be used directly.
    """

    def __init__(self, action, ticker, limitPrice, quantity, instrumentTraits: InstrumentTraits,
                 purpose=Order.Purpose.INIT, quote_time=None, expire=None,
                 pending_only=False, auto_close=False, price_type=Order.PriceType.PRICE,
                 ctp_offset_flag=None, extra=None):

        Order.__init__(self, Order.Type.LIMIT, action, ticker, quantity, instrumentTraits)
        BaseRequest.__init__(self)

        self.__create_time = datetime.now()
        self.__limitPrice = limitPrice
        self.__purpose = purpose
        self.__expire = expire
        self.__pending_only = pending_only
        self.__auto_close = auto_close
        self.__price_type = price_type

        self.__quote_time = quote_time

        if extra is None:
            self.__extra = dict()
        else:
            assert(isinstance(extra, dict))
            self.__extra = extra

        self.__ctp_offset_flag = ctp_offset_flag

    def getId(self):
        return self.getReqId()

    def getCreatedTime(self) -> datetime:
        return self.__create_time

    def getCtpOffsetFlag(self):
        return self.__ctp_offset_flag

    def getLimitPrice(self):
        """Returns the limit price."""
        return self.__limitPrice

    def isInit(self):
        return self.__purpose == Order.Purpose.INIT

    def isHedge(self):
        return self.__purpose == Order.Purpose.HEDGE

    def isAutoClose(self):
        return self.__auto_close

    def isPendingOnly(self):
        return self.__pending_only

    def getPurpose(self):
        return self.__purpose

    def getPriceType(self):
        return self.__price_type

    def getExtra(self) -> dict:
        return self.__extra

    def setExpire(self, expire):
        self.__expire = expire

    def getExpire(self):
        return self.__expire

    def getQuoteTime(self):
        return self.__quote_time

    def getReq(self):
        if self.getId() is None:
            return None

        action_dict = {Order.Action.BUY: trade_pb2.OrderAction.Value('BUY'),
                       Order.Action.SELL: trade_pb2.OrderAction.Value('SELL'),
                       Order.Action.BUY_TO_COVER: trade_pb2.OrderAction.Value('BUY_TO_COVER'),
                       Order.Action.SELL_SHORT: trade_pb2.OrderAction.Value('SELL_SHORT')}

        type_dict = {Order.Type.LIMIT: trade_pb2.OrderType.Value('LIMIT'),
                     Order.Type.MARKET: trade_pb2.OrderType.Value('MARKET'),
                     Order.Type.STOP: trade_pb2.OrderType.Value('STOP'),
                     Order.Type.STOP_LIMIT: trade_pb2.OrderType.Value('STOP_LIMIT')}

        pb_ins = trade_pb2.ReqMessage()
        pb_ins.req_id = self.getReqId()
        pb_ins.head = trade_pb2.ReqMessage.Head.Value('INSERT_ORDER')

        pb_ins.insert_order.ticker = self.getInstrument()
        pb_ins.insert_order.action = action_dict[self.getAction()]
        pb_ins.insert_order.type = type_dict[self.getType()]
        pb_ins.insert_order.quantity = self.getQuantity()
        pb_ins.insert_order.limit_price = self.getLimitPrice()

        pb_ins.insert_order.create_timestamp = self.__create_time.timestamp()

        pb_ins.insert_order.price_type = self.__price_type
        pb_ins.insert_order.pending_only = self.__pending_only
        pb_ins.insert_order.auto_close = self.__auto_close

        pb_ins.insert_order.purpose = self.__purpose

        if self.getLogicId():
            pb_ins.logic_id = self.getLogicId()

        if self.getExpire():
            pb_ins.insert_order.expire = self.getExpire()

        if self.getQuoteTime():
            pb_ins.insert_order.quote_timestamp = self.getQuoteTime().timestamp()

        if self.getMsg():
            pb_ins.insert_order.msg = self.getMsg()

        if self.__extra:
            pb_ins.insert_order.extra = json.dumps(self.__extra)

        if self.__ctp_offset_flag:
            pb_ins.insert_order.ctp_offset_flag = self.__ctp_offset_flag

        return pb_ins

    @classmethod
    def fromReq(cls, req: trade_pb2.ReqMessage, trait: InstrumentTraits):
        if req.insert_order.extra is None or req.insert_order.extra == '':
            extra = dict()
        else:
            extra = json.loads(req.insert_order.extra)

        order = LimitOrder(action=req.insert_order.action,
                           ticker=req.insert_order.ticker,
                           limitPrice=req.insert_order.limit_price,
                           quantity=req.insert_order.quantity,
                           instrumentTraits=trait,
                           purpose=req.insert_order.purpose,
                           expire=req.insert_order.expire,
                           pending_only=req.insert_order.pending_only,
                           auto_close=req.insert_order.auto_close,
                           price_type=req.insert_order.price_type,
                           extra=extra,
                           ctp_offset_flag=req.insert_order.ctp_offset_flag
                           )

        order.setSubmitted(req.req_id, datetime.fromtimestamp(req.timestamp))
        return order


class OrderExecutionInfo(object):
    """Execution information for an order."""
    def __init__(self, price, quantity, commission, dateTime, exchange_ref, strategy_order_ref=None):
        self.__price = price
        self.__quantity = quantity
        self.__commission = commission
        self.__dateTime = dateTime
        self.__exchange_ref = exchange_ref
        self.__strategy_order_ref = strategy_order_ref

    def __str__(self):
        return "%s - Price: %s - Amount: %s - Fee: %s" % (self.
                                                          __dateTime, self.__price, self.__quantity, self.__commission)

    def getPrice(self):
        """Returns the fill price."""
        return self.__price

    def getQuantity(self):
        """Returns the quantity."""
        return self.__quantity

    def getCommission(self):
        """Returns the commission applied."""
        return self.__commission

    def getDateTime(self):
        """Returns the :class:`datatime.datetime` when the order was executed."""
        return self.__dateTime

    def getExchangeRef(self):
        return self.__exchange_ref

    def getStrategyOrderRef(self):
        return self.__strategy_order_ref


class BatchOrder(BaseRequest):
    def __init__(self, orders: list):
        BaseRequest.__init__(self)

        ##
        self.__batch_id = str(uuid.uuid4())
        self.__tot_turnover = 0
        self.__filled_turnover = 0
        self.__canceled_turnover = 0

        self.__orders = orders
        for order in orders:
            self.__tot_turnover += order.getQuantity() * order.getLimitPrice()

    def getTotalTurnover(self):
        return self.__tot_turnover

    def setBatchId(self, batch_id: str):
        self.__batch_id = batch_id

    def getBatchId(self):
        return self.__batch_id

    def getId(self):
        return self.__batch_id

    def getReq(self):
        req = trade_pb2.ReqMessage()
        req.head = trade_pb2.ReqMessage.Head.Value("INSERT_BATCH_ORDER")
        req.batch_order.batch_id = str(self.getReqId())
        req.batch_order.order_reqs.extend([order.getReq() for order in self.__orders])

        if self.getLogicId() is not None:
            req.logic_id = self.getLogicId()

        return req

    def getOrders(self) -> list:
        return self.__orders


class EtfConvertRequest(BaseRequest):
    class Action:
        CREATE = trade_pb2.OrderAction.Value("ETF_CREATE")
        REDEEM = trade_pb2.OrderAction.Value("ETF_REDEEM")

    def __init__(self, action: int, ticker: str, quantity: int, min_exchange_unit: int, logic_id: str="unknown"):
        BaseRequest.__init__(self)

        self.__action = action
        self.__ticker = ticker
        self.__quantity = quantity
        self.__min_exchange_unit = int(min_exchange_unit)
        self.__logic_id = logic_id
        self.__cust_id = None
        self.__id = None

    def getId(self):
        return self.getReqId()

    def getMinExchangeUnit(self):
        return self.__min_exchange_unit

    def getReq(self):
        ret = trade_pb2.ReqMessage()
        ret.head = trade_pb2.ReqMessage.Head.Value("INSERT_ORDER")
        ret.logic_id = self.__logic_id
        ret.timestamp = datetime.now().timestamp()
        ret.req_id = self.getReqId()

        ret.insert_order.action = self.getAction()
        ret.insert_order.ticker = self.getTicker()
        ret.insert_order.quantity = self.getQuantity()
        ret.insert_order.min_exchange_unit = self.getMinExchangeUnit()

        return ret

    def getAction(self):
        return self.__action

    def getTicker(self):
        return self.__ticker

    def getQuantity(self):
        return self.__quantity

    @staticmethod
    def constructFromReq(req: trade_pb2.ReqMessage):
        ret = EtfConvertRequest(action=req.insert_order.action,
                                ticker=req.insert_order.ticker,
                                quantity=req.insert_order.quantity,
                                min_exchange_unit=req.insert_order.min_exchange_unit,
                                logic_id=req.logic_id
                                )
        ret.setSubmitted(req.req_id, datetime.fromtimestamp(req.timestamp))

        return ret
