# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
支持：
    1. 普通账号
    2. 两融账号
    3. 期权账号

"""

import os
import dbf
import json
import pytz
import logging
import asyncio
import async_task
import aioredis
import pandas as pd

from datetime import datetime
from dbf_util import read_dbf_from_line
from dbf_util import readRq, readCompact

DATETIME_FORMAT = '%Y%m%d%H%M%S%f'

if not os.path.exists('log'):
    os.makedirs('log')

logging.basicConfig(level=logging.DEBUG,
                    filename=f"log\\connector_{datetime.now().strftime('%Y%m%d%H%M%S')}.log")
LOGGER = logging.getLogger()


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

        self.__creditfund_resfile = os.path.join(result_path, f"CreditFund.dbf")
        self.__creditposi_resfile = os.path.join(result_path, f"CreditPosition.dbf")

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
        if cur.hour > 16:
            self.__eof = True
            logging.info("auto stop")
            exit(0)

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
                table.append(tuple(record))

    async def _publish(self, account_id, msg):
        print(f"{self.__pub_channel}|{account_id}", len(msg))
        await self.__r.publish(channel=f"{self.__pub_channel}|{account_id}",
                               message=msg)

    async def qryActiveOrder(self):
        try:
            order_updates_df = read_dbf_from_line(self.__orderupdate_resfile, 0)
            if order_updates_df is None:
                return

            grouped = order_updates_df[order_updates_df['ORD_TIME'] != ''].groupby(by='ORD_NO')
            active_order = grouped.filter(lambda x: not x['ORD_STATUS'].isin(['4', '5']).any())
            active_order = active_order.groupby(by="ORD_NO").last()
            active_order = active_order[active_order['ORD_STATUS'] == '0']

        except Exception as e:
            LOGGER.error(e)
            return

        for account_id, group in active_order.groupby(by='ACCT'):
            if len(group) > 0:
                to_send = {'prefix': 'ActOrd', 'data': group.groupby(by="ORD_NO").last().to_dict()}
                await self._publish(account_id, json.dumps(to_send))

    async def readStockFundAndPosition(self):
        """ 读取持仓和可用资金

        :return:
        """
        asset_res = dict()
        asset_df = read_dbf_from_line(self.__asset_resfile, 0)
        if asset_df is None:
            return
        for account_id, group in asset_df.groupby(by='ACCT'):
            if len(group) > 0:
                asset_res[account_id] = group.to_dict()

        account_ids = set()
        account_ids.update(asset_res.keys())
        for account_id in account_ids:
            to_send = {'prefix': 'Asset',
                       'asset': asset_res.get(account_id, dict()),
                       }
            await self._publish(account_id, json.dumps(to_send))

    async def handleOrderUpdateFile(self):
        order_updates_df = read_dbf_from_line(self.__orderupdate_resfile, self.__orderupdate_skip_rowid)

        if order_updates_df is None:
            return

        print(order_updates_df)

        for account_id, group in order_updates_df.groupby(by='ACCT'):
            if len(group) > 0:
                to_send = {'prefix': 'OrderUpdate', 'data': group.to_dict()}
                await self._publish(account_id, json.dumps(to_send))

        self.__orderupdate_skip_rowid += len(order_updates_df)

    async def qryOrderAndTradeResp(self):
        """ 顺序会对结果有影响，如果成交回报再报单确认之前，会找不到cust_id造成丢单

        :return:
        """
        await self.handleOrderUpdateFile()

    async def readCreditFundAndPosition(self):
        fund_df = read_dbf_from_line(self.__creditfund_resfile, 0)
        if fund_df is None:
            return

        account_data_map = dict()
        for account_id, group in fund_df.groupby(by='ACCT'):
            if len(group) > 0:
                account_data_map[account_id] = dict()
                account_data_map[account_id]['fund'] = group.to_dict()

        posi_df = read_dbf_from_line(self.__creditposi_resfile, 0)
        for account_id, group in posi_df.groupby(by='ACCT'):
            if account_id in account_data_map and len(group) > 0:
                account_data_map[account_id]['posi'] = group.to_dict()

        compact_res = dict()
        if os.path.exists(self.__creditcompact_resfile):
            compact_df = readCompact(self.__creditcompact_resfile)
            for account_id, group in compact_df.groupby(by='ACCT'):
                compact_res[account_id] = group.to_dict()

        rq_res = dict()
        if os.path.exists(self.__creditenslosecuqty_resfile):
            rq_df = readRq(self.__creditenslosecuqty_resfile)
            for account_id, group in rq_df.groupby(by='ACCT'):
                rq_res[account_id] = group.to_dict()

        for account_id in account_data_map.keys():
            to_send = {'prefix': 'Asset',
                       'fund': account_data_map[account_id]['fund'],
                       'posi': account_data_map[account_id].get('posi', {}),
                       "compact": compact_res.get(account_id, dict()),
                       "rq": rq_res.get(account_id, dict()),
                       }
            await self._publish(account_id, json.dumps(to_send))

    async def readOptionFundAndPosition(self):
        fund_df = read_dbf_from_line(self.__optionfund_resfile, 0)
        if fund_df is None:
            return

        account_data_map = dict()
        for account_id, group in fund_df.groupby(by='ACCT'):
            if len(group) > 0:
                account_data_map[account_id] = dict()
                account_data_map[account_id]['fund'] = group.to_dict()

        posi_df = read_dbf_from_line(self.__optionposi_resfile, 0)
        for account_id, group in posi_df.groupby(by='ACCT'):
            if account_id in account_data_map and len(group) > 0:
                account_data_map[account_id]['posi'] = group.to_dict()

        for account_id in account_data_map.keys():
            to_send = {'prefix': 'Asset', 'fund': account_data_map[account_id]['fund'],
                       'posi': account_data_map[account_id].get('posi', {})}
            await self._publish(account_id, json.dumps(to_send))

    def run(self):
        LOGGER.info('connecting')

        task_handler = async_task.TaskHandler()
        task_handler.getLoop().run_until_complete(self.connect())

        task_handler.runPeriodicAsyncJob(10, self.readStockFundAndPosition)
        task_handler.runPeriodicAsyncJob(10, self.readOptionFundAndPosition)
        task_handler.runPeriodicAsyncJob(10, self.readCreditFundAndPosition)
        task_handler.runPeriodicAsyncJob(10, self.qryActiveOrder)

        task_handler.runPeriodicAsyncJob(0.01, self.qryOrderAndTradeResp)

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
        pub_channel='cats-connector-pub|rb',
        sub_channel='cats-connector-sub|rb',
        result_path='C:\\Wealth CATS 4.0\\scan_order',
    )

    c.run()


