# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

import os

INFLUXDB_CONN = {
    'host': os.environ['INFLUXDB_HOST'],
    "port": os.environ["INFLUXDB_PORT"],
    "user": os.environ["INFLUXDB_USER"],
    "password": os.environ["INFLUXDB_PWD"]
}

