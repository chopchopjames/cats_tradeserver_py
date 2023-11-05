# -*- coding: utf-8 -*-

"""
.. moduleauthor:: James Ge
"""


import os
import redis
import pymongo
from urllib import parse

MONGO_HOST = os.environ.get('MONGO_HOST', 'mongo')
MONGO_PORT = os.environ.get('MONGO_PORT')
MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PWD = os.environ.get('MONGO_PWD')

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_PWD = os.environ.get('REDIS_PWD')
TRADE_PAGE = int(os.environ.get('TRADE_PAGE'))
QUOTE_PAGE = int(os.environ.get('QUOTE_PAGE'))
LOG_PAGE = int(os.environ.get('LOG_PAGE'))


def getMongoClient():
    host = '{}:{}'.format(MONGO_HOST, MONGO_PORT)
    client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/'.format(parse.quote_plus(MONGO_USER),
                                                                 parse.quote_plus(MONGO_PWD),
                                                                 host))
    return client


def getTradeRedis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST,
                    port=REDIS_PORT,
                    password=REDIS_PWD,
                    db=TRADE_PAGE)
    return r


def getQuoteRedis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST,
                    port=REDIS_PORT,
                    password=REDIS_PWD,
                    db=QUOTE_PAGE)
    return r


def getLogRedis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST,
                    port=REDIS_PORT,
                    password=REDIS_PWD,
                    db=LOG_PAGE)
    return r