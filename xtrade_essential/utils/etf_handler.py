"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

# -*- coding: utf-8 -*-

import typing
import time
import pandas as pd
from datetime import datetime

from xtrade_essential.xlib import logger
from xtrade_essential.utils import quote
from xtrade_essential.utils.clients import http, rds


BENCHMARK_ETF = {'IF': '510300.SH',
                 'IH': '510050.SH',
                 'IC': '510500.SH'}

UNDERLYING_INDEX = {"IF": "399300.SZ",
                    "IH": "000016.SH",
                    "IC": "000905.SH"}

MULTIPLIER = 10000


class EtfInfo(object):
    __slots__ = ('__ticker', "__instrument_id", "__exchange_id",
                 "__idate", "__raw_info", '__component_list',
                 "__etf_basic", "__etf_fees", "__stock_close",
                 "__is_stock_etf", "__compo_qty", '__tot_trnr_ap',
                 "__tot_trnr_bp", "__iopv", "__iopv_preday",
                 "__component_tick", "__component_ap1", "__component_bp1", "__update_times",
                 "__update_host_times", "__update_quote_time", "__update_host_time",
                 "__iopv_ap1", "__iopv_bp1", "__ap_multiplier", "__bp_multiplier",
                 "__iopv_new", "__iopv_dt", "__etf_quote",
                 "__suspend_stocks", "__upper_limit_adjust", "__lower_limit_adjust",
                 "__non_must_halt_adjust", "__must_halt_adjust", "__manual_adjust",
                 "__bm_index", "__weights", "__is_cross_market", "__cash_replaceable")

    class CashReplaceable:
        ALLOW = "allow"
        COMPULSORY = "compulsory"
        FORBIDDEN = "forbidden"
        TB_ALLOW = "tb_allow"
        TB_COMPULSORY = "tb_compulsory"
        TB_FORBIDDEN = "TB_FORBIDDEN"

    """ provide static information on ETF

    """

    def __init__(self, ticker, date):
        self.__ticker = ticker
        self.__instrument_id, self.__exchange_id = ticker.split('.', 1)
        self.__idate = date

        ## raw
        self.__raw_info = dict()

        self.__component_list = pd.DataFrame()
        self.__etf_basic = dict()
        self.__etf_fees = dict()
        self.__cash_replaceable = dict()
        self.__stock_close = dict()
        self.__is_stock_etf = False
        self.__compo_qty = dict()
        self.__tot_trnr_ap = 0
        self.__tot_trnr_bp = 0

        self.__iopv = 0
        self.__iopv_preday = 0
        self.__component_tick = dict()
        self.__component_ap1 = dict()
        self.__component_bp1 = dict()
        self.__update_times = dict()
        self.__update_host_times = dict()
        self.__update_quote_time = None
        self.__update_host_time = None

        self.__iopv_ap1 = 0
        self.__iopv_bp1 = 0
        self.__ap_multiplier = 0
        self.__bp_multiplier = 0
        self.__iopv_new = 0
        self.__iopv_dt = 0

        self.__etf_quote = None

        self.__suspend_stocks = list()

        #  打点
        self.__upper_limit_adjust = dict()
        self.__lower_limit_adjust = dict()
        self.__non_must_halt_adjust = dict()
        self.__must_halt_adjust = dict()
        self.__manual_adjust = dict()

        self.__bm_index = None
        self.__weights = dict()

        self.__is_cross_market = False

    def getCashReplaceable(self, ticker: str) -> CashReplaceable:
        return self.__cash_replaceable.get(ticker)

    def isCrossMarket(self):
        return self.__is_cross_market

    def getExchnageId(self):
        return self.__exchange_id

    def getTicker(self):
        return self.__ticker

    def getPredayIopv(self):
        return self.__iopv_preday

    def getIopvAp1(self):
        if len(self.__weights) == len(self.__component_tick):
            return self.__iopv_ap1

    def getIopvBp1(self):
        if len(self.__weights) == len(self.__component_tick):
            return self.__iopv_bp1

    def getAllComponentQuote(self):
        return self.__component_tick

    def getComponentQuote(self, ticker: str) -> dict:
        return self.__component_tick.get(ticker)

    def getTradingDay(self):
        return self.__idate

    def getEtfBasic(self):
        return self.__etf_basic

    def getStockList(self) -> pd.DataFrame:
        return self.__component_list

    def getEstimateCash(self):
        return self.__etf_basic['est_cash_component']

    def getUpAdjust(self):
        return self.__upper_limit_adjust

    def getDownAdjust(self):
        return self.__lower_limit_adjust

    def getNonMustHaltAdjust(self):
        return self.__non_must_halt_adjust

    def getMustHaltAdjust(self):
        return self.__must_halt_adjust

    def getEtfQuote(self):
        return self.__etf_quote

    def getHaltStocks(self):
        return self.__suspend_stocks

    def getBenchmarkIndex(self):
        return self.__bm_index

    def getAllQuoteUpdateTime(self):
        return self.__update_times

    def getAllHostUpdateTime(self):
        return self.__update_host_times

    def getLastUpdateHostTime(self):
        return self.__update_host_time

    def getLastUpdateQuoteTime(self):
        return self.__update_quote_time

    def getComponentQty(self, ticker: str):
        return self.__compo_qty.get(ticker, 0)

    def getMinExUnit(self):
        return self.__etf_basic['min_exchange_unit']

    def getComponentNum(self):
        return self.__etf_basic['security_component_num']

    def getIopv(self):
        return self.__iopv

    def getCreateFee(self):
        return self.__etf_fees['creation_fee']

    def getRedeemFee(self):
        return self.__etf_fees['redemption_fee']

    def getWeight(self, ticker):
        return self.__weights.get(ticker, 0)

    def getAllWeights(self) -> dict:
        return self.__weights

    def updateManualAdjust(self, adjust: dict):
        self.__manual_adjust = adjust

    def isStockEtf(self):
        return self.__is_stock_etf

    def _setStockEtf(self):
        """ for test purpose
        """
        self.__is_stock_etf = True

    def setRawInfo(self, basic_info: dict, component: list, halt_stocks: list):
        self.__raw_info["etf_basic"] = basic_info
        self.__is_cross_market = not basic_info.get('is_single_exchange', False)

        self.__raw_info["component"] = component
        self.__raw_info["halt"] = halt_stocks

        self.__etf_basic = basic_info

    def loadInfo(self):
        """
        occasionally load in other process, called asyncly or by other process
        :return:
        """
        etf_basic = http.getEtfInfo(date=self.getTradingDay(), ticker=self.getTicker())
        if etf_basic is None:
            return False

        self.__raw_info["etf_basic"] = etf_basic
        self.__raw_info["component"] = http.getEtfComponent(date=self.getTradingDay(), ticker=self.getTicker())
        self.__raw_info["halt"] = rds.getHaltStock(date=datetime.now().date()).to_dict(orient="records")
        self.__is_cross_market = not self.__raw_info["etf_basic"].get('is_single_exchange', False)

        self.__etf_basic = self.__raw_info["etf_basic"]

        self.process()
        return True

    def process(self):
        etf_basics = self.__raw_info["etf_basic"]
        component = self.__raw_info["component"]

        self.__bm_index = etf_basics['benchmark_index']
        self.__iopv_preday = etf_basics['basket_nav'] / etf_basics['min_exchange_unit']

        # LOGGER.info('getting component list')
        if self.getComponentNum() != len(component):
            return Exception('ETF basic suggest {} securities, only got {} in list'.format(self.getComponentNum(),
                                                                                           len(component)))

        self.__component_list = pd.DataFrame(component)

        # component quantity
        stock_list = pd.DataFrame(component)
        stock_list = stock_list[stock_list.cash_replaceable != "compulsory"]
        weights = stock_list.quantity / self.getMinExUnit()
        weights.index = stock_list.component_ticker
        self.__weights = weights.dropna().to_dict()
        self.__cash_replaceable = stock_list.set_index("component_ticker").cash_replaceable.to_dict()

        self._cashComponent()
        self._suspendedStocks()

    def _cashComponent(self):
        self.__iopv_ap1 += self.__etf_basic['est_cash_component'] / self.__etf_basic['min_exchange_unit']
        self.__iopv_bp1 += self.__etf_basic['est_cash_component'] / self.__etf_basic['min_exchange_unit']

        stock_list = self.getStockList()
        compul_cash_replace = stock_list.loc[stock_list.cash_replaceable == "compulsory"]
        self.__iopv_ap1 += compul_cash_replace.compulsory_cash_turnover_create.sum() / self.__etf_basic['min_exchange_unit']
        self.__iopv_bp1 += compul_cash_replace.compulsory_cash_turnover_redeem.sum() / self.__etf_basic['min_exchange_unit']

    def _suspendedStocks(self):
        # LOGGER.info('processing suspend stock')
        halt_stocks = self.__raw_info["halt"]
        component = pd.DataFrame(self.__raw_info["component"])
        component.set_index("component_ticker", inplace=True)

        for doc in halt_stocks:
            if doc["ticker"] in self.__weights:
                tick = {
                    "trading_session": quote.TradingPhase.HALT,
                    "datetime": doc["suspend_date"],
                    "open": doc["latest_price"],
                    "high": doc["latest_price"],
                    "low": doc["latest_price"],
                    "close": doc["latest_price"],
                    'volume': 0,
                    "amount": 0,
                    'bps': [0,],
                    'bvs': [0, ],
                    'aps': [0, ],
                    'avs': [0, ],
                }
                self.__component_tick[doc["ticker"]] = tick
                self.__suspend_stocks.append(doc)

                if component.cash_replaceable[doc["ticker"]] in ("compulsory", "tb_compulsory"):
                    self.__must_halt_adjust[doc["ticker"]] = doc['latest_price'] * self.getWeight(doc['ticker']) * max(0.2, doc['exp_ret'])
                else:
                    self.__non_must_halt_adjust[doc['ticker']] = doc['latest_price'] * self.getWeight(doc['ticker']) * doc['exp_ret']

        self.__suspend_stocks = list()

    def updateIopv(self, ticker: str, tick: dict):
        if ticker in self.getAllWeights():
            # adjust ap1/bp1
            ap1 = tick["aps"][-1]
            bp1 = tick["bps"][0]
            if ap1 == 0 and bp1 > 0:  # up limit
                ap1 = bp1
                self.__upper_limit_adjust[ticker] = ap1 * .05 * self.getWeight(ticker)

            elif bp1 == 0 and ap1 > 0:  # down limit
                bp1 = ap1
                self.__lower_limit_adjust[ticker] = bp1 * .05 * self.getWeight(ticker)

            elif bp1 == 0 and ap1 == 0:  # halt
                if tick.get("close", 0) > 0:
                    ap1 = tick["close"]
                    bp1 = tick["close"]

                else:
                    ap1 = tick["preclose"]
                    bp1 = tick["preclose"]

            else:
                # recover from limit
                if ticker in self.__upper_limit_adjust:
                    del self.__upper_limit_adjust[ticker]

                elif ticker in self.__lower_limit_adjust:
                    del self.__lower_limit_adjust[ticker]

            # adjust pre ap1/bp1
            pre_ap1 = self.__component_ap1.get(ticker, 0)
            pre_bp1 = self.__component_bp1.get(ticker, 0)

            self.__iopv_ap1 += (ap1 - pre_ap1) * self.getWeight(ticker)
            self.__iopv_bp1 += (bp1 - pre_bp1) * self.getWeight(ticker)

            self.__component_tick[ticker] = tick
            self.__component_ap1[ticker] = ap1
            self.__component_bp1[ticker] = bp1

        elif ticker == self.getTicker():
            self.__etf_quote = tick

        now = time.time()
        self.__update_times[ticker] = now
        self.__update_quote_time = now
        self.__update_host_time = now


def getAllEtfInfo(trade_date: datetime.date, etf_tickers: list = None) -> typing.Dict[str, EtfInfo]:
    from xtrade_essential.utils.clients import rds
    all_etf_info = rds.getEtfBasic(date=trade_date)
    all_etf_info = all_etf_info[all_etf_info.security_component_num > 1]  # 过滤货基

    if etf_tickers is not None:
        all_etf_info = all_etf_info[all_etf_info.ticker.isin(etf_tickers)]
        all_etf_info["is_single_exchange"] = all_etf_info["is_single_exchange"].astype(bool)

    print(f"************* loading {len(all_etf_info)} ETF info {trade_date} ****************")

    all_etf_component = rds.getEtfComponent(date=trade_date, tickers=etf_tickers)
    halt_stock = rds.getHaltStock(trade_date)

    ret = dict()
    for i, doc in all_etf_info.iterrows():
        print(f"processing ETF: {doc['ticker']}")

        algo = EtfInfo(ticker=doc["ticker"], date=trade_date)
        algo.setRawInfo(
            basic_info=doc.to_dict(),
            component=all_etf_component[all_etf_component.etf_ticker == doc["ticker"]].to_dict("records"),
            halt_stocks=halt_stock.to_dict("records"),
        )
        algo.process()

        ret[doc['ticker']] = algo

    return ret

