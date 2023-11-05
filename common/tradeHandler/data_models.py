# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

from datetime import datetime
from xtrade_essential.utils.order import LimitOrder as BaseLimitOrder


class LimitOrder(BaseLimitOrder):
    def __init__(self, *args, **kwargs):
        BaseLimitOrder.__init__(self, *args, **kwargs)

        self.__is_manual = False

    def setManual(self):
        self.__is_manual = True


class QryOrderResult:
    def __init__(self, exchange_order_ref: str, ticker: str, quantity: float, limit_price: float,
                 filled_quantity:float, action: LimitOrder.Action, create_time: datetime):
        self.__exchange_order_ref = exchange_order_ref
        self.__ticker = ticker
        self.__quantity = quantity
        self.__limit_price = limit_price
        self.__action = action
        self.__create_time = create_time
        self.__filled_quantity = filled_quantity

    def getExchangeOrderRef(self):
        return self.__exchange_order_ref

    def getTicker(self):
        return self.__ticker

    def getQuantity(self):
        return self.__quantity

    def getLimitPrice(self):
        return self.__limit_price

    def getAction(self):
        return self.__action

    def getCreateTime(self):
        return self.__create_time

    def getFilled(self):
        return self.__filled_quantity


class AccountHolding:
    def __init__(self, long_avg_cost, long_holding, long_available, long_profit, long_margin,
                 short_avg_cost, short_holding, short_available, short_profit, short_margin,
                 long_market_value=0, short_market_value=0, long_opened_today=0,
                 short_opened_today=0, margin_sell_available=None, long_holding_td: int = 0,
                 short_holding_td: int = 0,  long_holding_yd: int = 0, short_holding_yd: int = 0):
        self.__long_avg_cost = long_avg_cost
        self.__long_holding = long_holding
        self.__long_available = long_available
        self.__long_profit = long_profit
        self.__long_margin = long_margin
        self.__short_avg_cost = short_avg_cost
        self.__short_holding = short_holding
        self.__short_available = short_available
        self.__short_profit = short_profit
        self.__short_margin = short_margin

        self.__long_holding_td = long_holding_td
        self.__short_holding_td = short_holding_td
        self.__long_holding_yd = long_holding_yd
        self.__short_holding_yd = short_holding_yd

        self.__long_mv = long_market_value
        self.__short_mv = short_market_value

        self.__long_opened_today = long_opened_today
        self.__short_opened_today = short_opened_today

        self.__margin_sell_available = margin_sell_available

        self.__update_time = datetime.now()

    def getLongHoldingTd(self):
        return self.__long_holding_td

    def getLongHoldingYd(self):
        return self.__long_holding_yd

    def getShortHoldingTd(self):
        return self.__short_holding_td

    def getShortHoldingYd(self):
        return self.__short_holding_yd

    def getMarginSellAvailable(self):
        return self.__margin_sell_available

    def getLongAvgCost(self):
        return self.__long_avg_cost

    def getLongHolding(self):
        return self.__long_holding

    def getLongAvailable(self):
        return self.__long_available

    def getLongMargin(self):
        return self.__long_margin

    def getLongProfit(self):
        return self.__short_profit

    def getShortAvgCost(self):
        return self.__short_avg_cost

    def getShortHolding(self):
        return self.__short_holding

    def getShortAvailable(self):
        return self.__short_available

    def getShortProfit(self):
        return self.__short_profit

    def getShortMargin(self):
        return self.__short_margin

    def getLongOpenedToday(self):
        return self.__long_opened_today

    def getShortOpenedToday(self):
        return self.__short_opened_today

    def getLongMarketValue(self):
        return self.__long_mv

    def getShortMarketValue(self):
        return self.__short_mv

    def toDict(self):
        ret = {
            "long_avg_cost": self.__long_avg_cost,
            "long_holding": self.__long_holding,
            "long_available": self.__long_available,
            "long_profit": self.__long_profit,
            "long_margin": self.__long_margin,
            "short_avg_cost": self.__short_avg_cost,
            "short_holding": self.__short_holding,
            "short_available": self.__short_available,
            "short_profit": self.__short_profit,
            "short_margin": self.__short_margin,
            "long_balance": self.__long_mv,
            "short_balance": self.__short_mv,
            "long_opened_today": self.__long_opened_today,
            "short_opened_today": self.__short_opened_today,
            "margin_sell_available": self.getMarginSellAvailable()
        }
        return ret


class AccountBalance:
    def __init__(self, balance, cash_balance, cash_available, margin, unrealized_pnl, realized_pnl,
                 total_debit=0, total_asset=0, cash_available_forriskasset=0, cash_withdrawable=0,
                 cash_borrowable=0, cash_forcovershort=0, extra=dict()):

        self.__balance = balance
        self.__cash = cash_balance
        self.__cash_available = cash_available
        self.__margin = margin
        self.__unrealized_pnl = unrealized_pnl
        self.__relized_pnl = realized_pnl

        self.__total_debit = total_debit  # 总负债
        self.__total_asset = total_asset  # 总资产
        self.__cash_available_forriskasset = cash_available_forriskasset  # 买担保品可用
        self.__cash_withdrawable = cash_withdrawable  # 可取
        self.__cash_borrowable = cash_borrowable  # 可借
        self.__cash_forcovershort = cash_forcovershort  # 可买还

        self.__extra = extra

    def getCashForCoverShort(self):
        return self.__cash_forcovershort

    def getTotalDebit(self):
        return self.__total_debit

    def getTotalAsset(self):
        return self.__total_asset

    def getCashForRiskAsset(self):
        return self.__cash_available_forriskasset

    def getCashWithdrawable(self):
        return self.__cash_withdrawable

    def getCashBorrowable(self):
        return self.__cash_borrowable

    def getBalance(self):
        return self.__balance

    def getCash(self):
        return self.__cash

    def getCashAvailable(self):
        return self.__cash_available

    def getMargin(self):
        return self.__margin

    def getUnrealizedPnl(self):
        return self.__unrealized_pnl

    def getRealizedPnl(self):
        return self.__relized_pnl

    def getExtra(self):
        return self.__extra

    def toDict(self):
        ret = {
            "balance": self.getBalance(),
            "cash_balance": self.getCash(),
            "cash_available": self.getCashAvailable(),
            "margin": self.getMargin(),
            "unrealized_pnl": self.getUnrealizedPnl(),
            "realized_pnl": self.getRealizedPnl(),

            "total_debit": self.getTotalDebit(),
            "total_asset": self.getTotalAsset(),
            "cash_available_forriskasset": self.getCashForRiskAsset(),
            "cash_withdrawable": self.getCashWithdrawable(),
            "cash_borrowable": self.getCashBorrowable(),
            "cash_forcovershort": self.getCashAvailable(),

            "extra": self.__extra,
        }
        return ret


class MarginContractHolding:
    def __init__(self):
        self.__contract_records = list()
        self.__short_holding = dict()
        self.__short_mv = dict()

    def getShortQuantity(self, ticker: str) -> float:
        return self.__short_holding.get(ticker)

    def getShortAmount(self, ticker: str) -> float:
        return self.__short_mv.get(ticker)

    def addContractRecord(self, ticker: str, opening_amt: float, open_qty: float,
                          repayment_amt: float, repayment_qty: float, open_serial_num: str
                          ):
        self.__short_holding[ticker] += open_qty - repayment_qty
        self.__short_mv[ticker] += opening_amt - repayment_amt

        self.__contract_records.append([ticker, opening_amt, open_qty, repayment_amt, repayment_qty, open_serial_num])




