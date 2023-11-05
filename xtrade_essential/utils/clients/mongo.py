# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import time
import pymongo.errors
import pandas as pd
from datetime import datetime
from datetime import timedelta

from xtrade_essential.xlib import xdb as mongo


def safeMongoCall(call):
  def _safeMongoCall(*args, **kwargs):
    for i in range(5):
      try:
        return call(*args, **kwargs)
      except pymongo.errors.AutoReconnect:
        time.sleep(pow(2, i))
    raise Exception('Error: Failed operation!')
  return _safeMongoCall


def getOrderBook(tick, price_level=None):
    def getOrderLevel(x, i):
        try:
            return x[i]
        except IndexError:
            return None
        except TypeError:
            return None

    if price_level is None:
        price_level = len(tick.loc[0, 'aps'])

    order_book = pd.DataFrame()
    for i in range(price_level):
        for col in ['ap', 'av']:
            order_book[col + str(i + 1)] = tick.loc[:, col + 's'].apply(lambda x: getOrderLevel(x, len(x) - i - 1))
        for col in ['bp', 'bv']:
            order_book[col + str(i + 1)] = tick.loc[:, col + 's'].apply(lambda x: getOrderLevel(x, i - len(x)))
    return order_book


@safeMongoCall
def getDepth(dbname, ticker, start_dt, end_dt, price_level=5, cur=False):
    collection = mongo.getMongoClient().get_database(dbname).depth

    qry = {'ticker': ticker,
           'datetime': {'$gte': start_dt,
                        '$lte': end_dt}}
    proj = {'_id': False,
            'timestamp': False,
            'host_time': False}

    cursor = collection.find(qry, proj)
    if cur:
        return cursor

    ret = pd.DataFrame(list(cursor))
    if ret is not None and len(ret):
        ret = pd.concat([ret, getOrderBook(ret, price_level)], axis=1).drop(['aps', 'avs', 'bps', 'bvs'], axis=1)

    return ret


@safeMongoCall
def getTick(dbname, ticker, start_dt, end_dt, price_level=5, cur=False):
    collection = mongo.getMongoClient().get_database(dbname).tick

    qry = {'ticker': ticker,
           'datetime': {'$gte': start_dt,
                        '$lte': end_dt}}
    proj = {'_id': False,
            'timestamp': False,
            'host_time': False}

    cursor = collection.find(qry, proj)
    if cur:
        return cursor

    ret = pd.DataFrame(list(cursor))
    if ret is not None and len(ret):
        ret = pd.concat([ret, getOrderBook(ret, price_level)], axis=1).drop(['aps', 'avs', 'bps', 'bvs'], axis=1)

    return ret


@safeMongoCall
def getMinute(dbname, ticker, start_dt, end_dt):
    collection = mongo.getMongoClient().get_database(dbname).minute

    qry = {'ticker': ticker,
           'datetime': {'$gte': start_dt,
                        '$lte': end_dt}}
    proj = {'_id': False}

    ret = pd.DataFrame(list(collection.find(qry, proj)))
    if len(ret):
        return ret


@DeprecationWarning
@safeMongoCall
def getLatestBasicInfo(market):
    collection = mongo.getMongoClient().get_database(market).basic_info

    last_dt = collection.find_one(sort=[('datetime', -1)])['datetime']
    qry = {'datetime': last_dt}
    proj = {'_id': False}
    return pd.DataFrame(list(collection.find(qry, proj)))


@DeprecationWarning
@safeMongoCall
def getUpdateStatus(market):
    collection = mongo.getMongoClient().get_database("{}_quote".format(market)).update_status
    return pd.DataFrame(list(collection.find({}, {'_id': False})))


@safeMongoCall
def safeInsert(db_name, collection_name, to_inserts):
    collection = mongo.getMongoClient().get_database(db_name).get_collection(collection_name)
    try:
        collection.insert_many(to_inserts)
    except pymongo.errors.BulkWriteError:
        for doc in to_inserts:
            try:
                collection.insert_one(doc)
            except pymongo.errors.DuplicateKeyError:
                pass


@DeprecationWarning
@safeMongoCall
def dropCollection(db_name, collection_name):
    collection = mongo.getMongoClient().get_database(db_name).get_collection(collection_name)
    collection.drop()


@safeMongoCall
def getDuplicatedLiveTick(db_name: str, dateTime: datetime, fields=None):
    """
    
    :param db_name: 
    :type db_name: str
    :param dateTime: 
    :type dateTime: str
    :param fields: 
    :type fields: list
    
    :return: 
    """
    if fields is None:
        fields = ['datetime', 'ticker']
    collection = mongo.getMongoClient().get_database(db_name).live_tick
    start_dt = dateTime
    end_dt = start_dt + timedelta(days=1)
    pipe = [
        {'$match': {'datetime': {'$gte': start_dt, '$lt': end_dt}}},
        {'$group': {'_id': {x: '$'+x for x in fields},
                   'dups': {'$push': '$_id'},
                   'count': {"$sum": 1}}},
        { "$match": { "count": { "$gt": 1 } }}
        ]
    cur = collection.aggregate(pipe, allowDiskUse=True)
    return list(cur)


@DeprecationWarning
@safeMongoCall
def insertLiveTick2Tick(db_name: str, dateTime: datetime):
    db = mongo.getMongoClient().get_database(db_name)
    start_dt = dateTime
    end_dt = start_dt + timedelta(days=1)
    pipe = [
        {'$match': {'datetime': {'$gte': start_dt, '$lt': end_dt}}},
        {'$out': 'tick'}
    ]

    db.live_tick.aggregate(pipe)


@safeMongoCall
def cleanUpLiveTick(db_name, dateTime):
    db = mongo.getMongoClient().get_database(db_name)
    dups = pd.DataFrame(getDuplicatedLiveTick(db_name, dateTime))

    if len(dups) == 0:
        return

    for i, qry in enumerate(dups.ix[:, '_id']):
        dup_docs = db.live_tick.find(qry, {'datetime': True})
        for i, doc in enumerate(dup_docs):
            new_dt = doc['datetime'] + timedelta(microseconds=(i + 1) * 1000)
            db.live_tick.update_one({'_id': doc['_id']}, {'$set': {'datetime': new_dt}})


@safeMongoCall
def getLatestDatetime(db_name, collection_name, qry, dt_label):
    collection = mongo.getMongoClient().get_database(db_name).get_collection(collection_name)
    ret = collection.find_one(qry, {dt_label: True}, sort=[(dt_label, -1)])

    if ret:
        return ret[dt_label]


@safeMongoCall
def getDistinctTicker(db_name, collection_name):
    return mongo.getMongoClient().get_database(db_name).get_collection(collection_name).distinct('ticker')


@safeMongoCall
def getReqs(market: str, account=None, type_=None, logic_id=None, start_time=None, end_time=None,
            cur=False, skip_order_qry=False):
    c = mongo.getMongoClient().get_database('ex_'+market).request

    qry = dict()
    if account:
        qry['account_name'] = account

    if type_:
        if logic_id:
            qry['{}.strategy_id'.format(type_)] = logic_id
        else:
            qry[type_] = {'$exists': True}

    if start_time:
        qry['update_time'] = dict()
        qry['update_time']['$gte'] = start_time

    if end_time:
        if 'update_time' in qry:
            qry['update_time']['$lte'] = end_time
        else:
            qry['update_time'] = dict()
            qry['update_time']['$lte'] = end_time

    if cur:
        return c.find(qry)
    else:
        return pd.DataFrame(list(c.find(qry)))


@safeMongoCall
def getResps(market: str, account=None, type_=None, logic_id=None, start_time=None, end_time=None,
             cur=False, skip_order_qry=False, skip_position_qry=False):
    c = mongo.getMongoClient().get_database('ex_'+market).response

    qry = dict()
    if account:
        qry['account_name'] = account

    if type_:
        if logic_id:
            qry['{}.strategy_id'.format(type_)] = logic_id
        else:
            qry[type_] = {'$exists': True}

    if start_time:
        qry['update_time'] = dict()
        qry['update_time']['$gte'] = start_time

    if end_time:
        if 'update_time' in qry:
            qry['update_time']['$lte'] = end_time
        else:
            qry['update_time'] = dict()
            qry['update_time']['$lte'] = end_time

    if skip_order_qry:
        qry['qry_order_resp'] = {'$exists': False}

    if skip_position_qry:
        qry['position'] = {'$exists': False}

    if cur:
        return c.find(qry)
    else:
        return pd.DataFrame(list(c.find(qry)))


@safeMongoCall
def getPyvectorJobAndUpdateStatus(task_id):
    job_collection = mongo.getMongoClient().pyvector.get_collection(task_id)

    qry = {'status': 'pending'}
    update ={'$set': {'status': 'running'}}
    job = job_collection.find_one_and_update(qry, update)
    return job


@safeMongoCall
def updatePyvectorJobRes(task_id, job_id, res):
    collection = mongo.getMongoClient().pyvector.get_collection(str(task_id))
    qry = {'job_id': job_id}
    update ={'$set': {'status': 'finished',
                      'result': res}}
    collection.find_one_and_update(qry, update)


@safeMongoCall
def updatePyvectorJobError(task_id, job_id, err_msg=''):
    collection = mongo.getMongoClient().pyvector.get_collection(str(task_id))
    qry = {'job_id': job_id}
    update ={'$set': {'status': 'error',
                      'err_msg': 'err_msg'}}
    collection.find_one_and_update(qry, update)


@safeMongoCall
def getPyvectorTestResCur(task_id):
    collection = mongo.getMongoClient().pyvector.get_collection(task_id)
    return collection.find({'status': 'finished'})


@safeMongoCall
def getPyvectorTask(task_id):
    collection = mongo.getMongoClient().pyvector.summary
    return collection.find_one({'task_id': task_id})


@safeMongoCall
def getPyvectorTaskAll():
    collection = mongo.getMongoClient().pyvector.summary
    return list(collection.find())