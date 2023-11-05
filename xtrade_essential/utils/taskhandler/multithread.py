# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import traceback
import threading
import functools

from xtrade_essential.xlib import logger


LOGGER  = logger.getLogger()


class TaskHandler():
    def __init__(self, debug=False):
        self.__threads = list()
        self.__eof = threading.Event()
        self.__debug = False
        self.__tid = threading.current_thread().name

        if debug:
            LOGGER.setLevel('DEBUG')
            self.__debug = True

    def runPeriodicThreadJob(self, interval, func, *args, **kwargs):
        LOGGER.info('add periodic task: {}'.format(func.__name__))
        def runJob(interval, func, *args, **kwargs):
            while not self.__eof.wait(interval): #sleep
                try:
                    func(*args, **kwargs)
                    LOGGER.debug('running periodic job: {}'.format(func.__name__))
                except Exception as e:
                    LOGGER.error('error {} occurred  when running {}'.format(e, func.__name__), exc_info=True)

        t = threading.Thread(target=functools.partial(runJob, interval, func, *args, **kwargs))
        t.daemon = True
        t.start()
        self.__threads.append(t)

    def runJob(self, func, *args, **kwargs):
        t = threading.Thread(target=functools.partial(func, *args, **kwargs))
        t.daemon = True
        t.start()
        self.__threads.append(t)

    def eof(self):
        self.__eof.set()
