# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import asyncio
import threading
import functools


class TaskHandler():
    def __init__(self):
        self.__threads = list()
        self.__loop = asyncio.get_event_loop()
        self.__debug = False
        self.__tid = threading.current_thread().name

    def runPeriodicAsyncJob(self, interval, task_func, *args, **kwargs):
        async def runAsyncJob(interval, task_func, *args, **kwargs):
            while 1:
                try:
                    await task_func(*args, **kwargs)

                except Exception as e:
                    raise e

                await asyncio.sleep(interval)

        task = runAsyncJob(interval, task_func, *args, **kwargs)
        self.addCoroutineTask(task)

    def addCoroutineTask(self, task):
        if threading.current_thread().name == self.__tid:
            self.getLoop().create_task(task)

        else:
            f = functools.partial(self.getLoop().create_task, task)
            self.getLoop().call_soon_threadsafe(f)

    def getLoop(self):
        return self.__loop

    def eof(self):
        self.__loop.close()
