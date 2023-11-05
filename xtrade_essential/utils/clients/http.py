# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import os
import requests
import datetime
import pandas as pd

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from xtrade_essential.utils.order import FutureTraits, InstrumentTraits, OptionTraits, ConvertBondTraits


API_HOST = "http://{}".format(os.environ.get("API_HOST"))
USERNAME = os.environ.get("API_USER", 'user')
PASSWORD = os.environ.get("API_PASSWORD", "vMW2v6bXxmbsFY5")


def requests_retry_session(
        retries=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def _get(path, headers: dict={}, params={}, timeout=3, retries=5, **kwargs):
    url = API_HOST + path
    resp = requests_retry_session(retries=retries).get(url, headers=headers, params=params, timeout=timeout, **kwargs)
    if resp.status_code == 200:
        return resp.json()

    elif resp.status_code == 404:
        return None

    else:
        raise requests.HTTPError(resp.content)


def _put(path, data, timeout=3, retries=5, **kwarg):
    url = API_HOST + path
    resp = requests_retry_session(retries=retries).put(url, data=data, timeout=timeout, **kwarg)
    if resp.status_code == 200:
        return resp.json()

    elif resp.status_code == 404:
        return None

    else:
        raise requests.HTTPError(resp.content)


def _post(path, data, timeout=3, retries=5, **kwarg):
    url = API_HOST + path
    resp = requests_retry_session(retries=retries).post(url, data=data, timeout=timeout, **kwarg)
    if resp.status_code == 200:
        return resp.json()

    elif resp.status_code == 404:
        return None

    else:
        raise requests.HTTPError(resp.content)


def getToken():
    path = '/token/'
    data = {"username": USERNAME, "password": PASSWORD}
    return _post(path, data=data)


def _getAuth(path, **kwargs):
    token = getToken()
    headers = {"Authorization": "Bearer " + token["access"]}
    return _get(path, headers, **kwargs)


def getTradeAccount(hostname):
    path = '/tradeaccount/{}/'.format(hostname)
    return _getAuth(path)


def getQuoteAccount(hostname):
    path = '/quoteaccount/{}/'.format(hostname)
    return _getAuth(path)


def getSpreadTrader(trader_id):
    path = '/spreadtrader/{}/'.format(trader_id)
    return _get(path)


def getEtfBasketTrader(trader_id: str):
    path = f"/etfbaskettrader/{trader_id}/"
    return _getAuth(path)


def getAssistTrader(trader_id=None):
    path = '/assisttrader/'
    if trader_id:
        path += '{}/'.format(trader_id)
    return _get(path)


def getAssetList():
    path = '/asset/'
    return _get(path)


def getTickerList(exchange_name=None, is_trading=True):
    path = '/ticker/'
    ret = _get(path=path, params={'exchange_name': exchange_name}, timeout=20)
    return ret


def getActiveTickerList(exchange_name=None):
    path = '/activeticker/'
    params = {'is_trading': True}
    if exchange_name:
        params["exchange_name"] = exchange_name

    ret = _get(path=path, params=params, timeout=20)
    return ret


def getTicker(ticker):
    path = '/ticker/{}/'.format(ticker)
    return _get(path, timeout=20)


def getVolumeBar(ticker, start_time, end_time, target_volume):
    path = '/pyvector/indicator/volumebar/'

    data = {'ticker': ticker,
            'start_time': start_time,
            'end_time': end_time,
            'target_volume': target_volume}

    ret = _get(path, data=data)
    if ret is not None:
        return pd.read_json(ret)


def getExchangeFee(exchange_name=None):
    path = '/exchangefee/'
    paras = dict()
    if exchange_name:
        paras['exchange_name'] = exchange_name

    ret = _get(path, params=paras)
    return ret


def getEtfInfo(date, ticker=None):
    if ticker:
        path = "/etfinfo/{}/{}/".format(date.strftime('%Y%m%d'), ticker)
    else:
        path = "/etfinfo/{}/".format(date.strftime('%Y%m%d'))
    ret = _get(path)
    return ret


def getEtfComponent(date, ticker):
    path = "/etfinfo/{}/{}/component/".format(date.strftime('%Y%m%d'), ticker)
    ret = _get(path)
    return ret


def getSuspendStockList():
    path = '/suspendstock/'
    return _get(path)


def getPreClose(ticker: str, date: datetime.date) -> dict:
    path = '/quote/preclose/{}/{}/'.format(date.strftime('%Y%m%d'), ticker)
    return _get(path)


def getLogicListByTraderId(trader_id):
    path = '/spreadtraderlogics/{}'.format(trader_id)
    return _get(path)


# TODO:
def sendGroupMessage(type_, title, body):
    pass


def loadTraits(ticker_infos: list):
    traits = dict()
    for ticker in ticker_infos:
        maker_fee = (ticker.get('fee_maker_buy', 0) + ticker.get('fee_maker_sell', 0)) / 2
        taker_fee = (ticker.get('fee_taker_buy', 0) + ticker.get('fee_taker_sell', 0)) / 2

        if ticker['type'] in ('S', 'X', 'E'):
            trait = InstrumentTraits(trade_unit=ticker['trade_unit'],
                                     price_tick=ticker['price_tick'],
                                     maker_fee=maker_fee,
                                     taker_fee=taker_fee,
                                     quote_precision=ticker['quote_precision'],
                                     volume_precision=ticker['volume_precision'],
                                     min_limit_order_volume=ticker['min_limit_order_volume'],
                                     max_limit_order_volume=ticker['max_limit_order_volume'], base=ticker['base'],
                                     quote=ticker['quote'],
                                     is_derivative=ticker['is_derivative'],
                                     exchange_name=ticker['exchange_name'])

        elif ticker['type'] in ('F', "W"):
            trait = FutureTraits(
                trade_unit=ticker['trade_unit'],
                price_tick=ticker['price_tick'],
                maker_fee=maker_fee,
                taker_fee=taker_fee,
                quote_precision=ticker['quote_precision'],
                volume_precision=ticker['volume_precision'],
                min_limit_order_volume=ticker['min_limit_order_volume'],
                max_limit_order_volume=ticker['max_limit_order_volume'], base=ticker['base'],
                quote=ticker['quote'],
                is_derivative=ticker['is_derivative'],
                exchange_name=ticker['exchange_name'],
                contract_size=ticker['contract_size'],
                underlying=ticker['underlying'])

        elif ticker['type'] == "O":
            trait = OptionTraits(
                trade_unit=ticker['trade_unit'],
                price_tick=ticker['price_tick'],
                maker_fee=maker_fee,
                taker_fee=taker_fee,
                quote_precision=ticker['quote_precision'],
                volume_precision=ticker['volume_precision'],
                min_limit_order_volume=ticker['min_limit_order_volume'],
                max_limit_order_volume=ticker['max_limit_order_volume'], base=ticker['base'],
                quote=ticker['quote'],
                is_derivative=ticker['is_derivative'],
                exchange_name=ticker['exchange_name'],
                contract_size=ticker['contract_size'],
                underlying=ticker['underlying'],
                strike=ticker['exercise_price'],
                expire_time=ticker['delist_date'],
                option_type=ticker['option_type'],
                exercise_style=ticker["exercise_style"]
            )

        elif ticker['type'] == "C":
            trait = ConvertBondTraits(
                trade_unit=ticker['trade_unit'],
                price_tick=ticker['price_tick'],
                maker_fee=maker_fee,
                taker_fee=taker_fee,
                quote_precision=ticker['quote_precision'],
                volume_precision=ticker['volume_precision'],
                min_limit_order_volume=ticker['min_limit_order_volume'],
                max_limit_order_volume=ticker['max_limit_order_volume'], base=ticker['base'],
                quote=ticker['quote'],
                is_derivative=ticker['is_derivative'],
                exchange_name=ticker['exchange_name'],
                convert_price=ticker['convert_price'],
                underlying=ticker['underlying'])

        else:
            print(f"skip {ticker}, unrecognized type: {ticker['type']}")
            continue

        traits[ticker['ticker']] = trait

    return traits


def getFutureContractInfo() -> pd.DataFrame:
    import re
    def extractSymbol(ticker):
        s, _ = ticker.split('.')
        result = ''.join(re.split(r'[^A-Za-z]', s))
        return result.lower()

    def extractTerm(ticker):
        s, _ = ticker.split('.')
        result = ''.join(re.split(r'[^0-9]', s))
        if len(result) > 2:
            return int(result)

    url = f"{API_HOST}/ticker/"
    params = {'type': 'F'}
    resp = requests.get(url=url, params=params)
    contract_info = pd.DataFrame(resp.json())
    contract_info['delist_date'] = pd.to_datetime(contract_info['delist_date'])
    contract_info['list_date'] = pd.to_datetime(contract_info['list_date'])

    contract_info['symbol'] = contract_info.ticker.apply(extractSymbol)
    contract_info['exchange'] = contract_info.ticker.apply(lambda x: x.split('.')[1])
    contract_info['quote_ticker'] = contract_info.apply(
        lambda x: f"{x.symbol}{x.delist_date.strftime('%y%m')}.{x.exchange}".upper(), axis=1)

    contract_info = contract_info.groupby(by='ticker').last()

    return contract_info



