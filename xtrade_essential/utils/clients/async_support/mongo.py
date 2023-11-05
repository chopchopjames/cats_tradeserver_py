# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import motor.motor_asyncio
from urllib import parse

from xtrade_essential.xlib import xdb

def getMongoConStr():
    host = '{}:{}'.format(xdb.MONGO_HOST, xdb.MONGO_PORT)
    con_str = 'mongodb://{0}:{1}@{2}/'.format(parse.quote_plus(xdb.MONGO_USER),
                                              parse.quote_plus(xdb.MONGO_PWD),
                                              host)
    return con_str

class MongoClient():
    def __init__(self):
        self.__conn = motor.motor_asyncio.AsyncIOMotorClient(getMongoConStr())

    async def bulkInsert(self, dbname: str, collection: str, docs: list):
        collection = self.__conn[dbname][collection]
        await collection.insert_many(docs)

