## coding: utf8

# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# import sys
# import codecs
# sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
# celery worker里面会报错

import os
import logging
import threading

initLock = threading.Lock()
rootLoggerInitialized = False

format = "||%(asctime)s||%(name)s||%(levelname)s||%(module)s||%(funcName)s||%(lineno)s||%(message)s||"


def getLogger(name=None, level=logging.INFO):
    global rootLoggerInitialized
    with initLock:
        if not rootLoggerInitialized:
            logger = logging.getLogger(name)
            logger.setLevel(level)

            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(format))
            logger.addHandler(handler)

            rootLoggerInitialized = True

            return logger

    return logging.getLogger(name)



def getFileLogger(name=None, level=logging.INFO, log_dir=None, log_file=None):
    if log_dir is not None and not os.path.exists(log_dir):
        os.mkdir(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    filename = os.path.join(log_dir, log_file)
    file_handler = logging.FileHandler(filename)  # 创建一个文件处理器实例
    file_handler.setLevel(level)  # 设置文件处理器的日志记录级别
    formatter = logging.Formatter(format)  # 创建一个格式器实例
    file_handler.setFormatter(formatter)  # 将格式器设置给文件处理器
    logger.addHandler(file_handler)

    return logger

