#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向es中回放yxlink-vul_list数据
模板数据.txt文件统一都放在send_data/data目录下
每天晚上23:50分执行
50   23 * * *   python /home/wenyuan/es_replay/send_data/send_yxlink-vul_list2es.py >/dev/null 2>&1
"""
import os
import json
import time
import random
from functools import reduce
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# ----------- 需要修改的参数 -----------
es_host = '192.168.10.201'
token = '4a859fff6e5c4521aab187eee1cfceb8'
appname = 'yxlink'
doc_type = 'vul_list'
index_name = 'cc-{appname}-{doc_type}-{token}-{suffix}'.format(
    appname=appname,
    doc_type=doc_type,
    token=token,
    suffix=time.strftime('%Y.%m.%d')
)
data_file_name_list = [
    '201-yxlink-20181020.txt',
    '201-yxlink-20181021.txt',
    '201-yxlink-20181022.txt',
    '201-yxlink-20181023.txt',
    '201-yxlink-20181024.txt',
    '201-yxlink-20181025.txt',
    '201-yxlink-20181026.txt',
]
data_file_name = random.choice(data_file_name_list)
request_body_size = 100
# ------------------------------------

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, 'data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
DATA_FILE_PATH = os.path.join(DATA_DIR, data_file_name)


def execute_task():
    with open(DATA_FILE_PATH, 'r') as f:
        doc_list = []
        line = f.readline()
        while line:
            line = line.strip('\n')
            doc_content = json.loads(line, encoding='utf-8')
            doc_list.append(doc_content)
            if len(doc_list) >= request_body_size:
                current_doc_list = make_data(doc_list)
                send_data2es(current_doc_list)
                doc_list = []
            line = f.readline()
        if doc_list:
            current_doc_list = make_data(doc_list)
            send_data2es(current_doc_list)


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


def make_data(doc_list):
    current_doc_list = doc_list
    return current_doc_list


if __name__ == "__main__":
    execute_task()
