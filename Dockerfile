FROM ubuntu:20.04

# Avoiding user interaction with tzdata
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y tzdata && \
    ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata

RUN apt-get update ; \
    apt-get install -y python3.9 python3-pip

# 部署代码
ADD . /home/cats_tradeserver_py
RUN cd /home/cats_tradeserver_py \
    && pip3 install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# envs
ENV PYTHONPATH=/home/cats_tradeserver_py/:$PYTHONPATH

# Define working directory.
WORKDIR /home/cats_tradeserver_py

# 交易配置
ENV TRADE_ORDER_INTERVAL=0
ENV TRADE_QUERY_INTERVAL=10
ENV TRADE_CANCEL_INTERVAL=0.5
ENV TRADE_MAX_ORDER_LIFE=600
ENV TRADE_HOSTNAME="tradeserver-cats-test"

# 股票账户
ENV STOCK_REDIS_CONNECTOR_SUB_CH='cats-connector-sub|xd'
ENV STOCK_REDIS_CONNECTOR_PUB_CH='cats-connector-pub|xd'
ENV TRADE_STOCK_ACCOUNTID="1648039391"

# 日志
ENV LOG_LEVEL="INFO"