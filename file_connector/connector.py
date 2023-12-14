# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
支持：
    1. 普通账号
    2. 两融账号
    3. 期权账号

"""

import os
import csv
import dbf
import json
import pytz
import logging
import asyncio
import async_task
import aioredis
import pandas as pd

from datetime import datetime
from dbfread import DBF

from dbf_util import readRq, readCompact


DATETIME_FORMAT = '%Y%m%d%H%M%S%f'

if not os.path.exists('log'):
    os.makedirs('log')

logging.basicConfig(level=logging.DEBUG,
                    filename=f"log\\connector_{datetime.now().strftime('%Y%m%d%H%M%S')}.log")
LOGGER = logging.getLogger()


def read_dbf_from_line(filename, start_line):
    ret = list()
    table = DBF(filename, encoding='gbk')
    for i, record in enumerate(table):
        if i >= start_line:
            # Process the record
            ret.append(record)
    return ret


class CatsConnector(object):
    def __init__(self, redis_host, redis_port, redis_user, redis_pwd, redis_page, pub_channel, sub_channel,
                 account_id, result_path: str):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_user = redis_user
        self.redis_pwd = redis_pwd
        self.redis_page = redis_page

        self.__pub_channel = pub_channel
        self.__sub_channel = sub_channel
        self.__account_id = account_id

        self.__dt_str = datetime.now().strftime('%Y%m%d')

        self.__asset_resfile = os.path.join(result_path, f"asset.dbf")
        self.__orderupdate_resfile = os.path.join(result_path, f"order_updates.dbf")
        self.__orderupdateext_resfile = os.path.join(result_path, f"order_updates_ExtInfo.dbf")
        self.__cancelreject_resfile = os.path.join(result_path, f"order_updates_ExtInfo.dbf")
        self.__creditcompact_resfile = os.path.join(result_path, f"creditcompact.dbf")
        self.__creditenslosecuqty_resfile = os.path.join(result_path, f"creditenslosecuqty.dbf")
        self.__req_file = os.path.join(result_path, f"instructions.dbf")

        self.__optionfund_resfile = os.path.join(result_path, f"OptionFund.dbf")
        self.__optionposi_resfile = os.path.join(result_path, f"OptionPosition.dbf")

        LOGGER.info(f"asset: {self.__asset_resfile},"
                    f"order_update: {self.__orderupdate_resfile},")

        self.__orderupdate_skip_rowid = 0

        self.__eof = False

    def isEof(self):
        return self.__eof

    async def autoStop(self):
        """
        结束时间(Asia/Shanghai):
        1. 15:15
        2. 21:00

        :return:
        """
        cur = datetime.now(tz=pytz.UTC).astimezone(pytz.timezone('Asia/Shanghai'))
        if cur.hour > 21:
            self.__eof = True
            logging.info("auto stop")

        else:
            await asyncio.sleep(20)

    async def connect(self):
        self.__r = await aioredis.from_url(f"redis://{self.redis_host}:{self.redis_port}",
                                           password=self.redis_pwd,
                                           username=self.redis_user,
                                           db=self.redis_page,
                                           socket_timeout=10)
        await self.__r.set('test', 1)

    async def _monitorRedisChannel(self):
        pubsub = self.__r.pubsub()
        await pubsub.subscribe(self.__sub_channel)
        print(f"monitoring {self.__sub_channel}")

        while not self.isEof():
            try:
                msg_str = await pubsub.get_message(ignore_subscribe_messages=True)

            except ConnectionResetError as e:
                LOGGER.warning(f"ConnectionResetError: {e}")
                await asyncio.sleep(1)
                await self.connect()
                pubsub = self.__r.pubsub()
                await pubsub.subscribe(self.__sub_channel)
                continue

            if msg_str is not None:
                msg = json.loads(msg_str['data'])
                if msg['action'] == 'io':
                    # 报单
                    self.writeInsertOrderToFile(msg['data'])

                elif msg['action'] == 'del':
                    # 撤单
                    self.writeCancelOrderToFile(msg['data'])

                else:
                    LOGGER.warn(f"unknown action: {msg['action']}")
                    continue

    def writeInsertOrderToFile(self, data: list):
        """
        data = [
               [O,fads1,0,20800018203,不填,512010.SH,1,100,0.42,0],
               [O,<cust_id>,<柜台类型>,<账号>,<order_id>,<代码>,<买卖方向>,<qty>,<价格>,<委托类型>],
               ]

        :param data:
        :return:
        """
        table = dbf.Table(self.__req_file)

        # 添加新记录
        with table:
            for record in data:
                print(record)
                table.append(tuple(record))

    def writeCancelOrderToFile(self, data: list):
        """
        data = [
            [C,0,20800018203,0_20800018203_100100970806]
            [C,<柜台类型>,<client_id>,<order_no>]
        ]
        :param data:
        :return:
        """
        table = dbf.Table(self.__req_file)

        # 添加新记录
        with table:
            for record in data:
                print(record)
                table.append(tuple(record))

    async def _publish(self, account_id, msg):
        print(f"{self.__pub_channel}|{account_id}", len(msg))
        await self.__r.publish(channel=f"{self.__pub_channel}|{account_id}",
                               message=msg)

    async def qryActiveOrder(self):
        order_updates = read_dbf_from_line(self.__orderupdate_resfile, 0)
        order_updates_df = pd.DataFrame(order_updates)

        grouped = order_updates_df[order_updates_df['ORD_TIME'] != ''].groupby(by='ORD_NO')
        active_order = grouped.filter(lambda x: not x['ORD_STATUS'].isin(['4', '5']).any())
        active_order = active_order.groupby(by="ORD_NO").last()
        active_order = active_order[active_order['ORD_STATUS'] == '0']

        for account_id, group in active_order.groupby(by='ACCT'):
            if len(group) > 0:
                to_send = {'prefix': 'ActOrd', 'data': group.groupby(by="ORD_NO").last().to_dict()}
                await self._publish(account_id, json.dumps(to_send))

    async def handelAccountResFile(self):
        """ 读取持仓和可用资金

        :return:
        """
        asset_res = dict()
        asset = read_dbf_from_line(self.__asset_resfile, 0)
        asset_df = pd.DataFrame(asset)
        for account_id, group in asset_df.groupby(by='ACCT'):
            if len(group) > 0:
                asset_res[account_id] = group.to_dict()

        compact_res = dict()
        compact_df = readCompact(self.__creditcompact_resfile)
        for account_id, group in compact_df.groupby(by='ACCT'):
            compact_res[account_id] = group.to_dict()

        rq_res = dict()
        rq_df = readRq(self.__creditenslosecuqty_resfile)
        for account_id, group in rq_df.groupby(by='ACCT'):
            rq_res[account_id] = group.to_dict()

        account_ids = set()
        account_ids.update(asset_res.keys())
        account_ids.update(compact_res.keys())
        account_ids.update(rq_res.keys())

        for account_id in account_ids:
            to_send = {'prefix': 'Asset',
                       'asset': asset_res.get(account_id, dict()),
                       "compact": compact_res.get(account_id, dict()),
                       "rq": rq_res.get(account_id, dict()),
                       }
            await self._publish(account_id, json.dumps(to_send))

    async def handleOrderUpdateFile(self):
        order_updates = read_dbf_from_line(self.__orderupdate_resfile, self.__orderupdate_skip_rowid)

        if len(order_updates) == 0:
            return

        for account_id, group in pd.DataFrame(order_updates).groupby(by='ACCT'):
            if len(group) > 0:
                to_send = {'prefix': 'OrderUpdate', 'data': group.to_dict()}
                await self._publish(account_id, json.dumps(to_send))

        self.__orderupdate_skip_rowid += len(order_updates)

        print(self.__orderupdate_skip_rowid, len(order_updates))

    async def qryOrderAndTradeResp(self):
        """ 顺序会对结果有影响，如果成交回报再报单确认之前，会找不到cust_id造成丢单

        :return:
        """
        await self.handleOrderUpdateFile()

    async def readOptionFundAndPosition(self):
        fund = read_dbf_from_line(self.__optionfund_resfile, 0)
        fund_df = pd.DataFrame(fund)

        for account_id, group in fund_df.groupby(by='ACCT'):
            if len(group) > 0:
                to_send = {'prefix': 'OptionFund', 'data': fund_df.to_dict()}
                await self._publish(account_id, json.dumps(to_send))

        posi = read_dbf_from_line(self.__optionposi_resfile, 0)
        posi_df = pd.DataFrame(posi)

        for account_id, group in posi_df.groupby(by='ACCT'):
            if len(group) > 0:
                to_send = {'prefix': 'OptionPosition', 'data': posi_df.to_dict()}
                await self._publish(account_id, json.dumps(to_send))

    def run(self):
        LOGGER.info('connecting')

        task_handler = async_task.TaskHandler()
        task_handler.getLoop().run_until_complete(self.connect())

        task_handler.runPeriodicAsyncJob(10, self.handelAccountResFile)
        task_handler.runPeriodicAsyncJob(10, self.qryActiveOrder)

        task_handler.runPeriodicAsyncJob(0.01, self.qryOrderAndTradeResp)

        # task_handler.runPeriodicAsyncJob(10, self.readOptionFundAndPosition)

        task_handler.runPeriodicAsyncJob(interval=60 * 60, task_func=self.autoStop)

        task_handler.getLoop().run_until_complete(self._monitorRedisChannel())


if __name__ == '__main__':
    c = CatsConnector(
        redis_host="r-uf6obwre24sbecs67gpd.redis.rds.aliyuncs.com",
        redis_user="public_user",
        redis_pwd="Uq5_4iP5Sf3S",
        redis_port='6379',
        redis_page=1,
        account_id='1648039391',
        pub_channel='cats-connector-pub|xd',
        sub_channel='cats-connector-sub|xd',
        result_path='C:\\Wealth CATS 4.0\\scan_order',
    )

    c.run()


