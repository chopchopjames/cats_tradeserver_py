FROM registry.cn-shanghai.aliyuncs.com/trade/qmt_tradeserver_py:base1

# 部署代码
ADD . /home/cats_tradeserver_py
RUN cd /home/cats_tradeserver_py \
    && pip3 install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# envs
ENV PYTHONPATH=/home/cats_tradeserver_py/:$PYTHONPATH

# Define working directory.
WORKDIR /home/cats_tradeserver_py

# 日志
ENV LOG_LEVEL="INFO"