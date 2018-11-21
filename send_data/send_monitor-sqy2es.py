#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向es中写入sqy业务的仿真数据
数据直接构造,不用从别的地方下载

*/1 * * * *  python /home/wenyuan/es_replay/send_data/send_monitor-sqy2es.py >/dev/null 2>&1
"""
import copy
import time
import random
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# ----------- 需要修改的参数 -----------
es_host = '192.168.10.201'
token = '4a859fff6e5c4521aab187eee1cfceb8'
appname = 'monitor'
doc_type = 'sqy'
index_name = 'cc-{appname}-{doc_type}-{token}-{suffix}'.format(
    appname=appname,
    doc_type=doc_type,
    token=token,
    suffix=time.strftime('%Y.%m.%d')
)
# ------------------------------------

doc_template = {
    'appname': 'monitor-sqy',
    '@timestamp': '2018-05-16T09:58:33+08:00',  # 要变
    'type': 'sqy',
    'guid': '4a859fff6e5c4521aab187eee1cfceb8',
    'sqy': {  # 要变
        'node_ip': '',
        'node_name': '',
        'cpu_utilization': 0,
        'mem_utilization': 0,
        'root_disk_utilization': 0,
        'res_time': '200ms',
        'processing_speed': '-',  # per second
        'survive_time': '60天16小时20分钟'
    }
}


def execute_task(node_list):
    doc_list = make_data(node_list)
    send_data2es(doc_list)


def send_data2es(current_doc_list):
    actions = []
    for doc in current_doc_list:
        actions.append({
            '_op_type': 'index',
            '_index': index_name,
            '_type': doc_type,
            '_source': doc
        })
    try:
        es = Elasticsearch(es_host)
        success, failed = helpers.bulk(client=es, actions=actions)
        print('success: %s, failed: %s' % (success, failed))
    except TransportError as e:
        if isinstance(e, ConnectionTimeout):
            print('Read timed out!')
        elif isinstance(e, ConnectionError):
            print('Elasticsearch connection refused')
        else:
            print('System err')


def make_data(node_list):
    current_timestamp = time.strftime('%Y-%m-%dT%H:%M:%S+08:00')
    doc_list = []
    for idx, node in enumerate(node_list):
        node_ip = node['node_ip']
        node_name = node['node_name']
        if time.strftime('%H') in ['9', '10', '11', '14', '15', '16']:
            cpu_utilization = round(random.uniform(60, 70), 2)
            mem_utilization = round(random.uniform(50, 70), 2)
            res_time = round(random.uniform(200, 400), 2)
        elif time.strftime('%H') in ['0', '1', '2', '3', '4', '5', '6']:
            cpu_utilization = round(random.uniform(20, 30), 2)
            mem_utilization = round(random.uniform(30, 40), 2)
            res_time = round(random.uniform(100, 200), 2)
        else:
            cpu_utilization = round(random.uniform(30, 60), 2)
            mem_utilization = round(random.uniform(30, 50), 2)
            res_time = round(random.uniform(200, 300), 2)
        # 根据节点名特殊处理响应时间/处理速度
        if node_name in ['heka_log_processor', 'fair']:
            processing_speed = str(int(random.uniform(1, 2) * 1000)) + '/s'
            res_time = '-'
        else:
            processing_speed = '-'
            res_time = str(res_time) + 'ms'
        # 根据节点名特殊处理磁盘使用率
        if node_name in ['heka_log_processor', 'fair']:
            root_disk_utilization = 30 + int(time.strftime('%H'))
        elif node_name in ['es1', 'es2', 'es3', 'es4', 'es5', 'es6']:
            root_disk_utilization = 40 + int(time.strftime('%d'))
        else:
            root_disk_utilization = 32
        # 给某一个es告警
        if node_name in ['es5']:
            cpu_utilization = round(random.uniform(80, 90), 2)
            mem_utilization = round(random.uniform(80, 90), 2)
        survive_time = get_time_span()
        doc = copy.deepcopy(doc_template)
        doc['@timestamp'] = current_timestamp
        doc['sqy'] = {  # 要变
            'node_ip': node_ip,
            'node_name': node_name,
            'cpu_utilization': cpu_utilization,
            'mem_utilization': mem_utilization,
            'root_disk_utilization': root_disk_utilization,
            'res_time': res_time,
            'processing_speed': processing_speed,
            'survive_time': survive_time
        }
        doc_list.append(doc)
    return doc_list


def get_time_span():
    start = datetime.strptime('2018-05-01 8:00:12', '%Y-%m-%d %H:%M:%S')
    now = datetime.now()
    delta = now - start
    hour = delta.seconds / 60 / 60
    minute = (delta.seconds - hour * 60 * 60) / 60
    seconds = delta.seconds - hour * 60 * 60 - minute * 60
    return '%s天%s小时%s分钟%s秒' % (delta.days, hour, minute, seconds)


if __name__ == "__main__":
    node_list = [
        {'node_ip': '192.168.10.110', 'node_name': 'heka'},
        {'node_ip': '192.168.10.111', 'node_name': 'dawn'},
        {'node_ip': '192.168.10.112', 'node_name': 'kafka'},
        {'node_ip': '192.168.10.113', 'node_name': 'heka_log_processor'},
        {'node_ip': '192.168.10.114', 'node_name': 'fair'},
        {'node_ip': '192.168.10.115', 'node_name': 'es1'},
        {'node_ip': '192.168.10.116', 'node_name': 'es2'},
        {'node_ip': '192.168.10.117', 'node_name': 'es3'},
        {'node_ip': '192.168.10.118', 'node_name': 'es4'},
        {'node_ip': '192.168.10.119', 'node_name': 'es5'},
        {'node_ip': '192.168.10.120', 'node_name': 'es6'},
        {'node_ip': '192.168.10.121', 'node_name': 'grafen'},
        {'node_ip': '192.168.10.122', 'node_name': 'apollo'}
    ]
    execute_task(node_list)
