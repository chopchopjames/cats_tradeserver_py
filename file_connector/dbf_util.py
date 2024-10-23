# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import struct
import pandas as pd
from dbfread import DBF
import geopandas as gpd


def readRq(file_path):
    # 调用函数并打印头部信息
    records = list()
    with open(file_path, 'rb') as file:
        # 读取头部信息
        header = file.read(32)

        # 解析头部信息
        num_records, header_len, record_len = struct.unpack('<xxxxLHH20x', header)

        # 跳过头部
        file.seek(header_len)

        # 读取每条记录
        for _ in range(num_records):
            record_str = file.read(record_len).decode('gbk')
            ACCT = record_str[0:31].strip()   # 假设字段1占10个字符
            ACCTTYPE = record_str[32:47].strip()  # 假设字段2占1个字符
            SYMBOL = record_str[48:63].strip()  # 假设字段3占8个字符
            QTY = record_str[64:79].strip()  # 假设字段4占7个字符
            WRITE_TIME = record_str[80:].strip()    # 假设字段5是剩下的部分
            records.append([ACCT, ACCTTYPE, SYMBOL, QTY, WRITE_TIME])

    rq_df = pd.DataFrame(records, columns=['ACCT', 'ACCTTYPE', 'SYMBOL', 'QTY', 'WRITE_TIME'])
    return rq_df


def readCompact(file_path):
    COL_INFO = [
        ("ACCT", 32),
        ("ACCTTYPE", 16),
        ("OPENDATE", 8),
        ("COMPACTID", 64),
        ("CLIENTID", 32),
        ("FUNDACCT", 32),
        ("MONEYTYPE", 4),
        ("STOCKACCT", 32),
        ("STOCKCODE", 16),
        ("CRDTRATIO", 16),
        ("ETRSTNO", 16),
        ("ETRSTPRICE", 16),
        ("ETRSTAMT", 16),
        ("BIZAMOUNT", 16),
        ("BIZBALANCE", 16),
        ("BIZFARE", 16),
        ("CMPTYPE", 4),
        ("CMPSTATUS", 4),
        ("RCMBALANCE", 16),
        ("RCMAMOUNT", 16),
        ("RCMFARE", 16),
        ("RCINTEREST", 16),
        ("RPINTEREST", 16),
        ("RPAMOUNT", 16),
        ("RPBALANCE", 16),
        ("CMINTEREST", 16),
        ("UBBALANCE", 16),
        ("YEARRATE", 16),
        ("ENDDATE", 8),
        ("CLEARDATE", 8),
        ("WRITE_TIME", 32),
    ]

    records = list()
    with open(file_path, 'rb') as file:
        # 读取头部信息
        header = file.read(32)

        # 解析头部信息
        num_records, header_len, record_len = struct.unpack('<xxxxLHH20x', header)

        # 跳过头部
        file.seek(header_len)

        # 读取每条记录
        for _ in range(num_records):
            record_str = file.read(record_len).decode('gbk')

            start_loc = 0
            tmp = dict()
            for col, length in COL_INFO:
                end_loc = start_loc + length - 1
                tmp[col] = record_str[start_loc:end_loc].strip()
                start_loc = end_loc + 1
            records.append(tmp)

    compact_df = pd.DataFrame(records, columns=[col for col, _ in COL_INFO])
    return compact_df


def read_dbf_from_line(filename, start_line):
    try:
        table = DBF(filename, load=False)
    except PermissionError as e:
        return None

    if start_line < len(table):
        ret = gpd.read_file(filename)
        return ret.iloc[start_line:]
    else:
        return None


if __name__ == '__main__':
    import os
    file_dir = os.path.join(os.path.dirname(__file__))
    data = read_dbf_from_line(os.path.join(file_dir, 'filedemo', 'OptionPosition.dbf'), 0)
    data = pd.DataFrame(data)
    # print(data)

    # Attempt to read the DBF file using GeoPandas, which can handle DBF files as well
    file_path = os.path.join(file_dir, 'filedemo', 'OptionPosition.dbf')

    data = gpd.read_file(file_path)
    print(data)

