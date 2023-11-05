# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

from xtrade_essential.proto import quotation_pb2


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