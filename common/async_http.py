# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import aiohttp

from datetime import datetime, timedelta
from xtrade_essential.utils.clients.http import API_HOST, USERNAME, PASSWORD


class HttpClient:
    def __init__(self):
        self.__access_token = None
        self.__refresh_token = None
        self.__refresh_token_expire_time = datetime.now()
        self.API_HOST = API_HOST

    async def getToken(self):
        async with aiohttp.ClientSession() as session:
            data = {'username': USERNAME, "password": PASSWORD}
            async with session.post(f"{API_HOST}/token/", data=data) as response:
                token_resp = await response.json()

                self.__access_token = token_resp['access']
                self.__refresh_token = token_resp['refresh']

    async def refreshToken(self):
        if self.__access_token is None or self.__refresh_token_expire_time < datetime.now():
            await self.getToken()
            self.__refresh_token_expire_time += timedelta(hours=23)
        else:
            async with aiohttp.ClientSession() as session:
                data = {'access': self.__access_token,
                        'refresh': self.__refresh_token}
                async with session.post(f"{API_HOST}/token/refresh/", data=data) as response:
                    resp = await response.json()
                    self.__access_token = resp['access']

    async def getQuoteAccount(self, hostname: str):
        await self.refreshToken()
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": "Bearer " + self.__access_token}
            async with session.get(f'{API_HOST}/quoteaccount/{hostname}/', headers=headers) as response:
                resp = await response.json()
                return resp

    async def getTradeAccount(self, hostname: str):
        await self.refreshToken()
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": "Bearer " + self.__access_token}
            async with session.get(f'{API_HOST}/tradeaccount/{hostname}/', headers=headers) as response:
                resp = await response.json()
                return resp

