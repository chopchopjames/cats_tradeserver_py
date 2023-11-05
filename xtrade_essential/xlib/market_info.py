# -*- coding: utf-8 -*-

"""
.. moduleauthor:: James Ge
"""

from datetime import datetime

MARKETS = {'future': 'future',
           'stock': 'stock'}

MARKET_EXCHANGE_MAP = {'stock': ('SH', 'SZ'),
                       'future': ('DCE', 'SHFE', 'CFFEX', 'CZCE'),
                       'bitcoin': ('okcoin')}

ASSET_EXCHANGE_MAP = {'SHFE': ['ag', 'al', 'au', 'cu', 'fu', 'pb', 'rb', 'ru', 'wr', 'bu', 'zn', 'sn', 'hc', 'ni'],
                       'CZCE': ['CF', 'ER', 'ME', 'RM', 'RI', 'RO', 'SR', 'TA', 'WH', 'FG',
                                'RS', 'TC', 'WS', 'WT', 'SF', 'SM', 'MA', 'OI', 'JR', 'LR', 'ZC'],
                       'CFFEX': ['IF', 'IC', 'IH', 'TF', 'T'],
                       'DCE': ['a', 'b', 'c', 'j', 'l', 'm', 'p', 'v', 'fb', 'pp', 'i', 'jd', 'bb',
                               'jm', 'y', 'PM', 'cs']}


def getMarketByExchange(exchange_id):
    for market in MARKET_EXCHANGE_MAP:
        if exchange_id in MARKET_EXCHANGE_MAP[market]:
            return market


''' {<instrument_id>: <exchange_id>} '''
def get_future_exchange_dict():
    exchange_dict = {}
    for key in ASSET_EXCHANGE_MAP:
        value = ASSET_EXCHANGE_MAP[key]
        for instrument_id in value:
            exchange_dict[instrument_id] = key
    return exchange_dict
    
    
''' {<exchange_id>: <market_name>}'''
def get_reverse_marekt_dict():
    reverse_marekt_dict = {}
    for key in MARKET_EXCHANGE_MAP:
        value = ASSET_EXCHANGE_MAP[key]
        reverse_marekt_dict[key] = key
        for instrument_id in value:
            reverse_marekt_dict[instrument_id] = key
    return reverse_marekt_dict
    
                               
def get_future_market_by_id(instrument_key):
    for key in ASSET_EXCHANGE_MAP:
        if instrument_key in ASSET_EXCHANGE_MAP[key]:
            return key

def get_subscribe_identifier(instrument_info):
    return '{0}_{1}_{2}_{3}'.format(instrument_info['instrument_id'], instrument_info['exchange_id'],
                                    instrument_info['frequency'], instrument_info['vtype'])


def get_holiday(year):
    assert(type(year) is str)
    from .xdb import getMongoClient
    m = getMongoClient()
    ret = m.trade_config.holidays.find_one({'year': year})['holidays']
    return ret


def if_holiday():
    if datetime.now().weekday() in {5, 6}:
        return True

    year = datetime.now().strftime('%Y')
    today_str = datetime.now().strftime('%m%d')
    holidays = get_holiday(year)
    if today_str in holidays:
        return True

    return False


class Frequency(object):

  """Enum like class for bar frequencies. Valid values are:

  * **Frequency.TRADE**: The bar represents a single trade.
  * **Frequency.SECOND**: The bar summarizes the trading activity during 1 second.
  * **Frequency.MINUTE**: The bar summarizes the trading activity during 1 minute.
  * **Frequency.HOUR**: The bar summarizes the trading activity during 1 hour.
  * **Frequency.DAY**: The bar summarizes the trading activity during 1 day.
  * **Frequency.WEEK**: The bar summarizes the trading activity during 1 week.
  * **Frequency.MONTH**: The bar summarizes the trading activity during 1 month.
  """

  # It is important for frequency values to get bigger for bigger windows.
  TRADE = -1
  TICK = 0
  SECOND = 1
  MINUTE = 60
  MINUTE3 = 3*60
  MINUTE5 = 5*60
  MINUTE10 = 10*60
  MINUTE15 = 15*60
  MINUTE30 = 30*60
  HOUR = 60*60
  DAY = 24*60*60
  WEEK = 24*60*60*7
  MONTH = 24*60*60*31


class FinancialStatement():
    class ReportType():
        INCOME_STATEMENT = 'income_statement'
        BALANCE_SHEET = 'balance_sheet'
        CASHFLOW_STATEMENT = 'cashflow_statement'

    class StatementType():
        CONSOLIDATED = 408001000
        CONSOLIDATED_ADJUST = 408004000
        PARENT = 408006000
        PARENT_ADJUST = 408009000

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    