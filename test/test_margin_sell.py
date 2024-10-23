# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""

# -*- coding: utf-8 -*-


import time
import zmq
import uuid
import queue
import threading

from datetime import datetime

from xtrade_essential.proto import trade_pb2

HEAD = {'instrument': trade_pb2.ReqMessage.Head.Value('QUERY_INSTRUMENT'),
        'account': trade_pb2.ReqMessage.Head.Value('QUERY_ACCOUNT'),
        'order': trade_pb2.ReqMessage.Head.Value('QUERY_ORDER'),
        'trade': trade_pb2.ReqMessage.Head.Value('QUERY_TRADE'),
        'position': trade_pb2.ReqMessage.Head.Value('QUERY_POSITION')}

ORDER_TYPES = {'limit': trade_pb2.OrderType.Value('LIMIT')}

ORDER_ACTIONS = {'buy': trade_pb2.OrderAction.Value('BUY'),
                 'sell': trade_pb2.OrderAction.Value('SELL')}


def getReqSock(addr, port):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    conn_str = "tcp://{}:{}".format(addr, port)
    socket.connect(conn_str)
    return socket


def getSubSock(addr, port, pattern_str=''):
    addr_str = '{0}:{1}'.format(addr, port)
    context = zmq.Context()
    sock = context.socket(zmq.SUB)
    sock.setsockopt_string(zmq.SUBSCRIBE, pattern_str)
    sock.connect('tcp://%s' % (addr_str))
    return sock


def subMonitor(msg_q, zmq_socket):
    while 1:
        msg_q.put(zmq_socket.recv())


def sendQryAndWaitForReply(addr, req_port, resp_port, req, timeout):
    req.timestamp = datetime.now().timestamp()
    msg_queue = queue.Queue()
    req_sock = getReqSock(addr, req_port)
    resp_sock = getSubSock(addr, resp_port)
    sub_monitor = threading.Thread(target=subMonitor,
                                   args=(msg_queue, resp_sock,))
    sub_monitor.start()
    t1 = time.time()
    req_sock.send(req.SerializeToString())

    while time.time() - t1 < timeout:
        try:
            resp_str = msg_queue.get(True, timeout)
            resp = trade_pb2.RespMessage()
            resp.ParseFromString(resp_str)

            print(resp)
            print('took {} seconds'.format(time.time() - t1))

        except queue.Empty:
            pass


def test_qry(addr, req_port, resp_port, timeout, qry_type):
    req = trade_pb2.ReqMessage()
    req.head = HEAD[qry_type]
    return sendQryAndWaitForReply(addr, req_port, resp_port, req, timeout)


def insert_limit_order(addr, req_port, resp_port, ticker, limit_price, quantity, action, type_, timeout):
    req = trade_pb2.ReqMessage()
    req.head = trade_pb2.ReqMessage.Head.Value('INSERT_ORDER')
    req.req_id = str(uuid.uuid4())
    req.logic_id = 'test'
    req.insert_order.ticker = ticker
    req.insert_order.type = type_
    req.insert_order.action = action
    req.insert_order.quantity = quantity
    req.insert_order.limit_price = limit_price
    sendQryAndWaitForReply(addr, req_port, resp_port, req, timeout)

    return req.req_id


def cancel_order(addr, req_port, resp_port, order_id, timeout):
    cancel_req = trade_pb2.ReqMessage()
    cancel_req.head = trade_pb2.ReqMessage.Head.Value('ALTER_ORDER')
    cancel_req.logic_id = 'test'
    cancel_req.cancel_order.order_req_id = order_id
    sendQryAndWaitForReply(addr, req_port, resp_port, cancel_req, timeout)


def batch_cancel(addr, req_port, resp_port, order_id_list, timeout):
    batch_cancel_req = trade_pb2.ReqMessage()
    batch_cancel_req.head = trade_pb2.ReqMessage.Head.Value('BATCH_CANCEL')
    for order_id in order_id_list:
        req = batch_cancel_req.batch_cancel.cancel_req.add()
        req.order_req_id = order_id

    sendQryAndWaitForReply(addr, req_port, resp_port, batch_cancel_req, timeout)


def test_margin():
    """
    测试场景:
    [x] 资金查询-0仓位
    [x] 资金查询-有仓位
    [x] 持仓查询-0持仓
    [x] 持仓查询-有持仓
    [x] 买入撤单
    [] 卖出撤单
    [] 买入成交
    [] 卖出成交
    [] 融资买入
    [] 融券卖出
    [] 批量报单
    [] 批量撤单
    [x] 启动撤单
    """

    host = "localhost"
    req_port = 53001
    resp_port = 53002

    print(f'{host}:{req_port}:{resp_port}')

    # 持仓查询
    print(10*'*', 'query position', 10*'*')
    req = trade_pb2.ReqMessage()
    req.head = trade_pb2.ReqMessage.Head.Value('QUERY_POSITION')
    resp = sendQryAndWaitForReply(host, req_port, resp_port, req, 5)
    print(resp)

    # 报单
    print(10*'*', 'send order', 10*'*')
    ticker = '510500.SH'
    limit_price = 4.63
    quantity = 100
    action = trade_pb2.OrderAction.Value('SELL')
    type_ = trade_pb2.OrderType.Value('LIMIT')

    order_id = insert_limit_order(host, req_port, resp_port,
                                  ticker, limit_price, quantity,
                                  action, type_, 5)


    # 撤单
    print(10*'*', 'cancel order', 10*'*')
    time.sleep(3)
    cancel_order(host, req_port, resp_port, order_id, 3)

    # # 查询
    # print(10*'*', 'query position', 10*'*')
    # req = trade_pb2.ReqMessage()
    # req.head = trade_pb2.ReqMessage.Head.Value('QUERY_POSITION')
    # resp = sendQryAndWaitForReply(host, req_port, resp_port, req, 5)
    # print(resp)


if __name__ == '__main__':
    test_margin()

