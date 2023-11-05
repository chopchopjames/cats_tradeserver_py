## coding: utf8

"""
.. moduleauthor:: by: James Ge(james.ge@gmail.com)
"""
__all__ = ["MDTrade", "MDOrder", 'SecurityType', "DataType", "TradingPhase", "Frequency",
           "BasicTick", "BasicDepth", "BasicTrade", "BasicBar", "BasicIopv",]

import abc
import datetime

from xtrade_essential.xlib.market_info import Frequency
from xtrade_essential.proto import quotation_pb2


class MDOrder:
    class Type:
        LIMIT = quotation_pb2.MDOrder.OrdType.Value('LIMIT')
        MARKET = quotation_pb2.MDOrder.OrdType.Value("MARKET")
        MARKET_IF_TOUCHED = quotation_pb2.MDOrder.OrdType.Value('MARKET_IF_TOUCHED')
        UNKNOWN = quotation_pb2.MDOrder.OrdType.Value('UNKNOWN')

    class SIDE:
        BUY = quotation_pb2.Side.Value('MD_BUY')
        SELL = quotation_pb2.Side.Value('MD_SELL')
        BORROW = quotation_pb2.Side.Value('MD_BORROW')
        LENDING = quotation_pb2.Side.Value('MD_LEND')
        UNKNOWN = quotation_pb2.Side.Value('MD_UNKNOWN')


class MDTrade:
    class Type:
        FILL = quotation_pb2.MDTrade.ExecType.Value('FILL') ## 成交
        CANCEL = quotation_pb2.MDTrade.ExecType.Value('CANCEL') ## 撤单
        IOC_AUTO_CANCEL = quotation_pb2.MDTrade.ExecType.Value('IOC_AUTO_CANCEL') ## "即时成交剩余撤销委托", 未能成交部分撤单
        ETF_PURCHASE_OR_REDEEM_FILL = quotation_pb2.MDTrade.ExecType.Value('ETF_CREATE_OR_REDEEM_FILL') ## ETF申购 / 赎回成功回报记录
        ETF_PURCHASE_OR_REDEEM_CANCEL = quotation_pb2.MDTrade.ExecType.Value('ETF_CREATE_OR_REDEEM_CANCEL') ## ETF申购 / 赎回撤单回报
        IOC5_AUTO_CANCEL = quotation_pb2.MDTrade.ExecType.Value('IOC5_AUTO_CANCEL') ## “最优五档即时成交剩余撤销委托”未能成交部分的自动撤单
        FOK_AUTO_CANCEL = quotation_pb2.MDTrade.ExecType.Value('FOK_AUTO_CANCEL') ## “全额成交或撤销委托”未能成交时的自动撤单
        BEST_OFFER_AUTO_CANCEL = quotation_pb2.MDTrade.ExecType.Value('BEST_OFFER_AUTO_CANCEL') ## 对手方最优价格委托的撤单回报记录
        MARKET_IF_TOUCHED_AUTO_CANCEL = quotation_pb2.MDTrade.ExecType.Value('MARKET_IF_TOUCHED_AUTO_CANCEL') ## 本方最优价格委托的撤单回报记录
        ETF_CASH_FILL = quotation_pb2.MDTrade.ExecType.Value('ETF_CASH_FILL') ## ETF基金申购 / 赎回成功允许 / 必须现金替代明细回报记录

    class Side:
        BUY = quotation_pb2.Side.Value('MD_BUY')
        SELL = quotation_pb2.Side.Value('MD_SELL')
        UNKNOWN = quotation_pb2.Side.Value('MD_UNKNOWN')


class SecurityType:
    INDEX = quotation_pb2.Message.SecurityType.Value('INDEX')
    STOCK = quotation_pb2.Message.SecurityType.Value('STOCK')
    FUND = quotation_pb2.Message.SecurityType.Value('FUND')
    BOND = quotation_pb2.Message.SecurityType.Value('BOND')
    REPO = quotation_pb2.Message.SecurityType.Value('REPO')
    WARRANT = quotation_pb2.Message.SecurityType.Value('WARRANT')
    OPTION = quotation_pb2.Message.SecurityType.Value('OPTION')
    FOREX = quotation_pb2.Message.SecurityType.Value('FOREX')
    RATE = quotation_pb2.Message.SecurityType.Value('RATE')
    NMETAL = quotation_pb2.Message.SecurityType.Value('NMETAL')
    SPOT = quotation_pb2.Message.SecurityType.Value('SPOT')
    FUTURE = quotation_pb2.Message.SecurityType.Value('FUTURE')
    OTHER = quotation_pb2.Message.SecurityType.Value('OTHER')

    @staticmethod
    def toString(security_type):
        if security_type == SecurityType.INDEX:
            return "index"
        elif security_type == SecurityType.STOCK:
            return "stock"
        elif security_type == SecurityType.FUND:
            return "fund"
        elif security_type == SecurityType.BOND:
            return "bond"
        elif security_type == SecurityType.REPO:
            return 'repo'
        elif security_type == SecurityType.WARRANT:
            return "warrant"
        elif security_type == SecurityType.OPTION:
            return "option"
        elif security_type == SecurityType.FOREX:
            return "forex"
        elif security_type == SecurityType.RATE:
            return "rate"
        elif security_type == SecurityType.NMETAL:
            return "nmetal"
        elif security_type == SecurityType.SPOT:
            return "spot"
        elif security_type == SecurityType.FUTURE:
            return "future"
        elif security_type == SecurityType.OTHER:
            return "other"


class TradingPhase:
    PRE_TRADING = quotation_pb2.Message.TradingSession.Value('PRE_TRADING')
    OPENING = quotation_pb2.Message.TradingSession.Value('OPENING')
    AFTER_OPENING = quotation_pb2.Message.TradingSession.Value('AFTER_OPENING')
    TRADING = quotation_pb2.Message.TradingSession.Value('TRADING')
    NOON_BREAK = quotation_pb2.Message.TradingSession.Value('NOON_BREAK')
    CLOSING = quotation_pb2.Message.TradingSession.Value('CLOSING')
    CLOSED = quotation_pb2.Message.TradingSession.Value('CLOSED')
    AFTER_HOUR_TRADING = quotation_pb2.Message.TradingSession.Value('AFTER_HOUR_TRADING')
    HALT = quotation_pb2.Message.TradingSession.Value('HALT')
    UNSCHEDULED_INTRADAY_ACTION = quotation_pb2.Message.TradingSession.Value('UNSCHEDULED_INTRADAY_ACTION')

    @staticmethod
    def toString(phase):
        if phase == TradingPhase.PRE_TRADING:
            return "pre_trading"
        elif phase == TradingPhase.OPENING:
            return "opening"
        elif phase == TradingPhase.AFTER_OPENING:
            return "after_opening"
        elif phase == TradingPhase.TRADING:
            return "trading"
        elif phase == TradingPhase.NOON_BREAK:
            return "noon_break"
        elif phase == TradingPhase.CLOSING:
            return "closing"
        elif phase == TradingPhase.CLOSED:
            return "closed"
        elif phase == TradingPhase.AFTER_HOUR_TRADING:
            return "after_hour_trading"
        elif phase == TradingPhase.HALT:
            return "halt"
        elif phase == TradingPhase.UNSCHEDULED_INTRADAY_ACTION:
            return "unscheduled_intraday_action"
        else:
            return 'unknown'


class DataType:
    TICK = quotation_pb2.Message.DataType.Value('TICK')
    BAR = quotation_pb2.Message.DataType.Value('BAR')
    IOPV = quotation_pb2.Message.DataType.Value('IOPV')
    MD_TRADE = quotation_pb2.Message.DataType.Value('MD_TRADE')
    MD_ORDER = quotation_pb2.Message.DataType.Value('MD_ORDER')
    DEPTH = quotation_pb2.Message.DataType.Value('DEPTH')

    @classmethod
    def toString(cls, type_):
        if type_ == cls.TICK:
            return 'tick'

        elif type_ == cls.BAR:
            return 'bar'

        elif type_ == cls.IOPV:
            return 'iopv'

        elif type_ == cls.MD_TRADE:
            return 'mdtrade'

        elif type_ == cls.MD_ORDER:
            return 'mdorder'

        elif type_ == cls.DEPTH:
            return 'depth'


class Bar(object):

    """A Bar is a summary of the trading activity for a security in a given period.

    .. note::
        This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def getUseAdjValue(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def getDateTime(self):
        """Returns the :class:`datetime.datetime`."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getOpen(self):
        """Returns the opening price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getHigh(self):
        """Returns the highest price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getLow(self):
        """Returns the lowest price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getClose(self):
        """Returns the closing price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getVolume(self):
        """Returns the volume."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getAmount(self):
        """Returns the volume."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getAdjClose(self):
        """Returns the adjusted closing price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getFrequency(self):
        """The bar's period."""
        raise NotImplementedError()

    def getTypicalPrice(self):
        """Returns the typical price."""
        return (self.getHigh() + self.getLow() + self.getClose()) / 3.0

    def getExtraColumns(self):
        return {}


class BasicBar(Bar):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__open',
        '__close',
        '__high',
        '__low',
        '__volume',
        '__amount',
        '__frequency',
        '__extra',
    )

    def __init__(self, dateTime, open_, high, low, close, volume, amount, frequency,  extra=None):
        if high < low:
            raise Exception("high < low on %s" % (dateTime))
        elif high < open_:
            raise Exception("high < open on %s" % (dateTime))
        elif high < close:
            raise Exception("high < close on %s" % (dateTime))
        elif low > open_:
            raise Exception("low > open on %s" % (dateTime))
        elif low > close:
            raise Exception("low > close on %s" % (dateTime))

        self.__dateTime = dateTime
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__amount = amount
        self.__frequency = frequency
        if extra is None:
            self.__extra = dict()
        else:
            self.__extra = extra

    def __setstate__(self, state):
        (self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__amount,
            self.__frequency,
            self.__extra) = state

    def __getstate__(self):
        return (
            self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__amount,
            self.__frequency,
            self.__extra
        )

    def updateDateTime(self, dateTime: datetime.datetime):
        self.__dateTime = dateTime

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self):
        return self.__open

    def getHigh(self):
        return self.__high

    def getLow(self):
        return self.__low

    def getClose(self):
        return self.__close

    def getVolume(self):
        return self.__volume

    def getAmount(self):
        return self.__amount

    def getFrequency(self):
        return self.__frequency

    def getExtraColumns(self):
        return self.__extra


class BasicTick(object):
    # Optimization to reduce memory footprint.
    __slots__ = (
        "__session",
        '__dateTime',
        '__open',
        '__close',
        '__high',
        '__low',
        '__volume',
        '__amount',
        '__bp',
        '__bv',
        '__ap',
        '__av',
        '__preclose',
        '__new_price',
        '__bought_amount',
        '__sold_amount',
        '__bought_volume',
        '__sold_volume',
        '__frequency',
        '__extra',
        "__upper_limit",
        "__lower_limit",
    )

    def __init__(self, trading_session, dateTime, open_, high, low, close, volume, amount, bp, bv, ap, av, preclose,
                 bought_amount, sold_amount, bought_volume, sold_volume,
                 upper_limit, lower_limit, frequency=Frequency.TICK, extra={}):
        # if trading_session == TradingPhase.TRADING:
        #     if high < low:
        #         raise Exception("high {} < low {} on {}".format(high, low, datetime))
        #     elif high < open_:
        #         raise Exception("high {} < open {} on {}".format(high, open_, datetime))
        #     elif high < close:
        #         raise Exception("high {} < close {} on {}".format(high, close, dateTime))
        #     elif low > open_:
        #         raise Exception("low {} > open {} on {}".format(low, open_, dateTime))
        #     elif low > close:
        #         raise Exception("low {} > close {} on {}".format(low, close, dateTime))

        self.__session = trading_session
        self.__dateTime = dateTime
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__amount = amount
        self.__bp = bp
        self.__ap = ap
        self.__bv = bv
        self.__av = av
        self.__preclose = preclose
        self.__bought_amount = bought_amount
        self.__bought_volume = bought_volume
        self.__sold_amount = sold_amount
        self.__sold_volume = sold_volume
        self.__frequency = frequency
        self.__upper_limit = upper_limit
        self.__lower_limit = lower_limit
        self.__extra = extra

    def __setstate__(self, state):
        (self.__dateTime,
        self.__session,
        self.__open,
        self.__close,
        self.__high,
        self.__low,
        self.__volume,
        self.__amount,
        self.__bp,
        self.__ap,
        self.__bv,
        self.__av,
        self.__preclose,
        self.__bought_amount,
        self.__sold_amount,
        self.__frequency,
        self.__extra) = state

    def __getstate__(self):
        return         (        self.__dateTime,
        self.__session,
        self.__open,
        self.__close,
        self.__high,
        self.__low,
        self.__volume,
        self.__amount,
        self.__bp,
        self.__ap,
        self.__bv,
        self.__av,
        self.__preclose,
        self.__bought_amount,
        self.__sold_amount,
        self.__frequency,
        self.__extra)

    def updateDateTime(self, dateTime: datetime.datetime):
        self.__dateTime = dateTime

    def getSession(self):
        return self.__session

    def getDateTime(self):
        return self.__dateTime

    def getUpperLimit(self):
        return self.__upper_limit

    def getLowerLimit(self):
        return self.__lower_limit

    def getOpen(self):
        return self.__open

    def getHigh(self):
        return self.__high

    def getLow(self):
        return self.__low

    def getClose(self):
        return self.__close

    def getPrice(self):
        return self.__close

    def getVolume(self):
        return self.__volume

    def getAmount(self):
        return self.__amount

    def getFrequency(self):
        return self.__frequency

    def getBp(self):
        return self.__bp

    def getBv(self):
        return self.__bv

    def getAp(self):
        return self.__ap

    def getAv(self):
        return self.__av

    def getAp1(self):
        return self.__ap[-1]

    def getBp1(self):
        return self.__bp[0]

    def getPreclose(self):
        return self.__preclose

    def getBoughtAmount(self):
        return self.__bought_amount

    def getBoughtVolume(self):
        return self.__bought_volume

    def getSoldAmount(self):
        return self.__sold_amount

    def getSoldVolume(self):
        return self.__sold_volume

    def getExtraColumns(self):
        return self.__extra


class BasicDepth(object):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__trading_session',
        '__bp',
        '__bv',
        '__ap',
        '__av',
        "__extra"
    )

    def __init__(self, dateTime, trading_session, bp, bv, ap, av, extra=dict()):
        self.__dateTime = dateTime
        self.__trading_session = trading_session
        self.__bp = bp
        self.__bv = bv
        self.__ap = ap
        self.__av = av
        self.__extra = extra

    def __setstate__(self, state):
        (self.__dateTime,
         self.__trading_session,
        self.__bp,
        self.__ap,
        self.__bv,
        self.__av) = state


    def __getstate__(self):
        return  (self.__dateTime,
         self.__trading_session,
        self.__bp,
        self.__ap,
        self.__bv,
        self.__av)

    def updateDateTime(self, dateTime: datetime.datetime):
        self.__dateTime = dateTime

    def getDateTime(self):
        return self.__dateTime

    def getTradingSession(self):
        return self.__trading_session

    def getAp1(self):
        return self.__ap[-1]

    def getBp1(self):
        return self.__bp[0]

    def getBp(self):
        return self.__bp

    def getBv(self):
        return self.__bv

    def getAp(self):
        return self.__ap

    def getAv(self):
        return self.__av

    def getFrequency(self):
        return Frequency.TRADE

    def getExtra(self):
        return self.__extra


class BasicTrade(object):
    class Side:
        BID = MDTrade.Side.BUY
        ASK = MDTrade.Side.SELL
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__id',
        '__price',
        '__volume',
        '__type',
        "__side",
        "__extra",
    )

    def __init__(self, dateTime, id_, price, volume, type_, side, extra=None):
        self.__dateTime = dateTime
        self.__id = id_
        self.__price = price
        self.__volume = volume
        self.__type = type_
        self.__side = side
        if extra is None:
            self.__extra = dict()
        else:
            self.__extra = extra

    def __setstate__(self, state):
        (self.__dateTime,
         self.__id,
         self.__price,
         self.__volume) = state

    def __getstate__(self):
        return (self.__dateTime,
                self.__id,
                self.__price,
                self.__volume)

    def updateDateTime(self, dateTime: datetime.datetime):
        self.__dateTime = dateTime

    def getDateTime(self):
        return self.__dateTime

    def getSide(self):
        return self.__side

    def getId(self):
        return self.__id

    def getPrice(self):
        return self.__price

    def getVolume(self):
        return self.__volume

    def getType(self):
        return self.__type

    def isBuy(self):
        return self.__type == BasicTrade.Side.BID

    def getFrequency(self):
        return Frequency.TRADE

    def getExtra(self):
        return self.__extra


class BasicIopv(Bar):
    __slots__ = ["__dateTime", '__new_price', "__ap1", '__bp1', '__ap1_adj', '__bp1_adj', "__preclose",
                 "__min_exchange_unit", "__created_bskt_num", "__redeemed_bskt_num", "__redeem_limit",
                 "__create_limit", "__limit_up_adjust", "__limit_down_adjust",
                 "__non_must_halt_adjust", "__must_halt_adjust"]

    def __init__(self, dateTime: datetime, new_price: float, ap1: float, bp1: float, ap1_adj: float, bp1_adj: float,
                 preclose: float, min_exchange_unit, created_bskt_num, redeemed_bskt_num, create_limit,
                 redeem_limit, limit_up_adjust: float, limit_down_adjust: float, non_must_halt_adjust: float,
                 must_halt_adjust: float, extra=None):
        self.__dateTime = dateTime
        self.__new_price = new_price,
        self.__ap1 = ap1
        self.__bp1 = bp1
        self.__ap1_adj = ap1_adj
        self.__bp1_adj = bp1_adj
        self.__preclose = preclose

        self.__min_exchange_unit = min_exchange_unit
        self.__created_bskt_num = created_bskt_num
        self.__redeemed_bskt_num = redeemed_bskt_num
        self.__create_limit = create_limit
        self.__redeem_limit = redeem_limit

        self.__limit_up_adjust = limit_up_adjust
        self.__limit_down_adjust = limit_down_adjust
        self.__non_must_halt_adjust = non_must_halt_adjust
        self.__must_halt_adjust = must_halt_adjust

        if extra is None:
            self.__extra = dict()
        else:
            self.__extra = extra

    def updateDateTime(self, dateTime: datetime.datetime):
        self.__dateTime = dateTime

    def getDateTime(self):
        return self.__dateTime

    def getPrice(self):
        return self.__new_price

    def getAp1(self):
        return self.__ap1

    def getBp1(self):
        return self.__bp1

    def getAp1Adj(self):
        return self.__ap1_adj

    def getBp1Ajd(self):
        return self.__bp1_adj

    def getPreclose(self):
        return self.__preclose

    def getMinExchangeUnit(self):
        return self.__min_exchange_unit

    def getCreatedBasketNum(self):
        return self.__created_bskt_num

    def getRedeemedBasketNum(self):
        return self.__redeemed_bskt_num

    def getCreateLimit(self):
        return self.__create_limit

    def getRedeemLimit(self):
        return self.__redeem_limit

    def getLimitUpAdjust(self):
        return self.__limit_up_adjust

    def getLimitDownAdjust(self):
        return self.__limit_down_adjust

    def getNonMustHaltAdjust(self):
        return self.__non_must_halt_adjust

    def getMustHaltAdjust(self):
        return self.__must_halt_adjust

    def getExtra(self):
        return self.__extra

