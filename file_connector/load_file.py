# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import pandas as pd
from dbfread import DBF

def read_dbf_from_line(filename, start_line):
    ret = list()
    table = DBF(filename, encoding='gbk')
    for i, record in enumerate(table):
        if i >= start_line:
            # Process the record
            ret.append(record)
    return ret

asset = read_dbf_from_line('file_connector\\filedemo\\asset.dbf', 0)
asset_df = pd.DataFrame(asset)

order_updates = read_dbf_from_line('file_connector\\filedemo\\order_updates.dbf', 0)
order_updates_df = pd.DataFrame(order_updates)



