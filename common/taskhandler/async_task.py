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
        self.__eof = False

    def runPeriodicAsyncJob(self, interval, task_func, *args, **kwargs):
        async def runAsyncJob(interval, task_func, *args, **kwargs):
            while not self.__eof:
                try:
                    await task_func(*args, **kwargs)
                except Exception as e:
                    await asyncio.sleep(interval)

        task = runAsyncJob(interval, task_func, *args, **kwargs)
        self.addCoroutineTask(task)

    def addCoroutineTask(self, task):
        if threading.current_thread().name == self.__tid:
            self.getLoop().create_task(task)

        else:
            # print("schedule from thread: {}".format(threading.current_thread().name))
            f = functools.partial(self.getLoop().create_task, task)
            self.getLoop().call_soon_threadsafe(f)

    def getLoop(self):
        # print(self.__loop.is_closed())
        if not self.__loop.is_closed():
            return self.__loop

        else:
            self.__loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.__loop)
            return self.__loop

    def stop(self):
        self.__eof = True

    def eof(self):
        return self.__eof