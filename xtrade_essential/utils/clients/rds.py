# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import os
import datetime

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, Table, MetaData, func
from sqlalchemy.orm import sessionmaker

import pymysql

pymysql.install_as_MySQLdb()


def getEngine(dbname):
    conn = {'user': os.environ['RDS_USER'],
            'pwd': os.environ['RDS_PWD'],
            'host': os.environ['RDS_HOST'],
            'port': os.environ['RDS_PORT'],
            'db': dbname}
    engine = create_engine('mysql://{user}:{pwd}@{host}:{port}/{db}'.format(**conn))

    return engine


def getSession(engine):
    pass


def autoLoadTable(table_name, engine):
    meta = MetaData(engine)
    return Table(table_name, meta, autoload=True, autoload_with=engine, extend_existing=True)


def _getTable(engine, table_name):
    meta = MetaData(engine)
    table = Table(table_name, meta, autoload=True)
    return table


def _getDataFrame(engine, table, filters):
    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)
    session = Session()

    q = session.query(table).filter(*filters)
    return pd.read_sql_query(q.statement, con=engine)


def getHaltStock(date: datetime.date) -> pd.DataFrame:
    dbname = "django"
    table_name = "autotrader_suspendstock"

    engine = getEngine(dbname)
    table = _getTable(engine, table_name)
    filters = list()
    filters.append(table.c.get("update_date") == date)

    ret = _getDataFrame(engine, table, filters)
    ret = ret[(ret.resume_date > date) | (ret.resume_date.isnull())]
    return ret


def getEtfComponent(date: datetime.date, tickers: list = None) -> pd.DataFrame:
    engine = getEngine("django")
    table = _getTable(engine, "autotrader_etfcomponent")

    if tickers is None:
        tickers = list()

    ticker_filter = list()
    for ticker in tickers:
        ticker_filter.append(table.c.get("etf_ticker") == ticker)

    ticker_filter = sa.or_(*ticker_filter)

    filters = list()
    filters.append(table.c.get("date") == date)
    filters.append(ticker_filter)
    ret = _getDataFrame(engine, table, filters)
    return ret


def getEtfBasic(date: datetime.date, ticker: str=None) -> pd.DataFrame:
    engine = getEngine("django")
    table = _getTable(engine, "autotrader_etfbasic")
    filters = list()
    filters.append(table.c.get("date") == date)

    if ticker:
        filters.append(table.c.get("ticker") == ticker)

    ret = _getDataFrame(engine, table, filters)
    if len(ret):
        ret["ticker"] = ret.ticker
    else:
        ret = pd.DataFrame([])
    return ret


def getDailyQuote(start_date: datetime.date, end_date: datetime.date, ticker: str=None) -> pd.DataFrame:
    table_name = "autotrader_dailyquote"
    engine = getEngine("django")

    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)
    session = Session()

    table = Table(table_name, meta, autoload=True)

    filters = list()
    filters.append(table.c.get('date') >= start_date)
    filters.append(table.c.get('date') <= end_date)

    if ticker:
        filters.append(table.c.get("ticker") == ticker)

    q = session.query(table).filter(*filters)
    return pd.read_sql_query(q.statement, con=engine)


def getInsertOrder(logic_id=None, ticker=None, start_time=None, end_time=None):
    table_name = 'insert_order'
    engine = getEngine('trade')

    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)
    session = Session()

    table = Table(table_name, meta, autoload=True)

    filters = list()
    if logic_id:
        filters.append(table.c.get('strategy_id') == logic_id)

    if ticker:
        filters.append(table.c.get('ticker') == ticker)

    if start_time:
        filters.append(table.c.get('order_create_time') >= start_time)

    if end_time:
        filters.append(table.c.get('order_create_time') <= end_time)

    q = session.query(table).filter(*filters)
    return pd.read_sql_query(q.statement, con=engine)


def getCancelReq(logic_id=None, start_time=None, end_time=None):
    table_name = 'cancel_req'
    engine = getEngine('trade')

    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)
    session = Session()

    table = Table(table_name, meta)
    Table(table_name, meta, autoload=True, autoload_with=engine, extend_existing=True)

    filters = list()
    if logic_id:
        filters.append(table.c.get('strategy_id') == logic_id)

    if start_time:
        filters.append(table.c.get('host_time') >= start_time)

    if end_time:
        filters.append(table.c.get('host_time') <= end_time)

    q = session.query(table).filter(*filters)

    return pd.read_sql_query(q.statement, con=engine)


def getOrderResp(start_time=None, end_time=None):
    table_name = 'order_resp'
    engine = getEngine('trade')

    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)
    session = Session()

    table = Table(table_name, meta)
    Table(table_name, meta, autoload=True, autoload_with=engine, extend_existing=True)

    filters = list()
    if start_time:
        filters.append(table.c.get('host_time') >= start_time)

    if end_time:
        filters.append(table.c.get('host_time') <= end_time)

    q = session.query(table).filter(*filters)

    return pd.read_sql_query(q.statement, con=engine)


def getError(start_time=None, end_time=None):
    table_name = 'error'
    engine = getEngine('trade')

    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)
    session = Session()

    table = Table(table_name, meta)
    Table(table_name, meta, autoload=True, autoload_with=engine, extend_existing=True)

    filters = list()
    if start_time:
        filters.append(table.c.get('host_time') >= start_time)

    if end_time:
        filters.append(table.c.get('host_time') <= end_time)

    q = session.query(table).filter(*filters)

    return pd.read_sql_query(q.statement, con=engine)


def getTrade(ticker=None, start_time=None, end_time=None):
    table_name = 'trade'
    engine = getEngine('trade')

    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)
    session = Session()

    table = Table(table_name, meta)
    Table(table_name, meta, autoload=True, autoload_with=engine, extend_existing=True)

    filters = list()

    if ticker:
        filters.append(table.c.get('ticker') == ticker)

    if start_time:
        filters.append(table.c.get('host_time') >= start_time)

    if end_time:
        filters.append(table.c.get('host_time') <= end_time)

    q = session.query(table).filter(*filters)

    return pd.read_sql_query(q.statement, con=engine)


def getTable(db_name, table_name):
    engine = getEngine(db_name)
    meta = MetaData(engine)
    Session = sessionmaker(bind=meta)
    Session.configure(bind=engine)

    table = Table(table_name, meta)
    Table(table_name, meta, autoload=True, autoload_with=engine, extend_existing=True)
    return table


def insertDataframe(dataframe, table_name, engine, if_exists='append'):
    to_add = dataframe.copy()
    to_add = _removeTzinfo(to_add)
    type_dict = _getTypes(to_add)

    to_add.to_sql(table_name, engine, dtype=type_dict, if_exists=if_exists, index=False)


def _getTypes(df):
    from sqlalchemy.types import VARCHAR
    from sqlalchemy.dialects.mysql import TIMESTAMP

    types = {}
    for col, col_type in df.dtypes.iteritems():
        if str(col_type) == 'object':
            types[col] = VARCHAR(1024)
        elif str(col_type) == 'datetime64[ns]':
            types[col] = TIMESTAMP(fsp=6)
    return types


def _removeTzinfo(df):
    from datetime import datetime

    for col, col_type in df.dtypes.iteritems():
        if str(col_type) == 'datetime64[ns, UTC]':
            df[col] = df[col].dt.tz_localize(None)
    return df
