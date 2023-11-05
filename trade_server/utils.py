# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import pytz

from datetime import datetime
from xtrade_essential.utils.order import InstrumentTraits


CN_TZ = pytz.timezone('Asia/Shanghai')


OPTION_TRAITS = InstrumentTraits(
    trade_unit=1,
    price_tick=0.001,
    maker_fee=0,
    taker_fee=0,
    quote_precision=3,
    volume_precision=0,
    min_limit_order_volume=1,
    max_limit_order_volume=10000,
    base='OPTION',
    quote="CNY",
    is_derivative=True,
    exchange_name='ChineseStockOption'
)


STOCK_TRAITS = InstrumentTraits(
    trade_unit=1,
    price_tick=0.01,
    maker_fee=0,
    taker_fee=0,
    quote_precision=2,
    volume_precision=0,
    min_limit_order_volume=100,
    max_limit_order_volume=10000,
    base='STOCK',
    quote="CNY",
    is_derivative=True,
    exchange_name='ChineseStock'
)

def stdCode2Ticker(code: str):
    if int(code[0]) > 3:
        return f"{code}.SH"
    else:
        return f"{code}.SZ"


def parseDatetimeStr(dt_str):
    ret = CN_TZ.localize(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S"))
    return ret.astimezone(pytz.UTC).replace(tzinfo=None)


