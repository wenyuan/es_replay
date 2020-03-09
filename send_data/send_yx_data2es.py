#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向es中写入snmp仿真数据
数据模板来源(.txt文件)必须要是经过fiter_data过滤后的
因为有多个数据源,.txt文件统一都放在send_data/data目录下

*/1 * * * *  cd /home/wenyuan/es_replay/send_data/ && python send_yx_data2es.py >/dev/null 2>&1
"""
import os
import sys
import json
import time
import random
from functools import reduce
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *

# ----------- 需要修改的参数 -----------
es = Elasticsearch('192.168.10.201')
token = '4a859fff6e5c4521aab187eee1cfceb8'
index_name = 'cc-yxlink-vul_list-4a859fff6e5c4521aab187eee1cfceb8-' + time.strftime('%Y.%m.%d')
data_type = 'vul_list'
# data_file_name = '201-yxlink-20181024'
request_body_size = 100
# ------------------------------------

reload(sys)
sys.setdefaultencoding('utf-8')

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, 'data')
data_file_names = os.listdir(DATA_DIR)
# print data_file_names
data_file_names = filter(lambda x: 'yxlink' in x, data_file_names)
index = time.localtime()[6]
print index
data_file_name = data_file_names[index]
print data_file_name
# print time.localtime()[6]
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
# DATA_FILE_PATH = os.path.join(DATA_DIR, data_file_name + '.txt')
DATA_FILE_PATH = os.path.join(DATA_DIR, data_file_name)


class SendSnmpData2Es(object):

    def __init__(self):
        pass

    def execute_task(self):
        with open(DATA_FILE_PATH, 'r') as f:
            doc_list = []
            line = f.readline()
            while line:
                try:
                    line = line.strip('\n')
                    doc_content = json.loads(line, encoding='utf-8')
                    doc_list.append(doc_content)
                    if len(doc_list) >= request_body_size:
                        current_doc_list = self.make_data(doc_list)
                        self.send_data2es(current_doc_list)
                        doc_list = []
                    line = f.readline()
                except Exception as e:
                    line = f.readline()
            if doc_list:
                current_doc_list = self.make_data(doc_list)
                self.send_data2es(current_doc_list)

    @staticmethod
    def send_data2es(current_doc_list):
        body = []
        for doc in current_doc_list:
            body.append({
                "_index": index_name,
                "_type": data_type,
                "_source": doc
            })
        try:
            success, failed = helpers.bulk(es, body)
            print('success: %s, failed: %s' % (success, failed))
        except TransportError as e:
            if isinstance(e, ConnectionTimeout):
                print('read timed out!')
            elif isinstance(e, ConnectionError):
                print('elasticsearch connection refused!')
            else:
                print('system err')

    @staticmethod
    def make_data(doc_list):
        current_doc_list = []
        t = time.time()
        timestamp_in_seconds = int(t)
        timestamp_in_millisecond = int(round(t * 1000))
        for doc in doc_list:
            doc['guid'] = token
            doc['@timestamp'] = timestamp_in_millisecond
            current_doc_list.append(doc)
        return current_doc_list


if __name__ == "__main__":
    send_snmp_data2es = SendSnmpData2Es()
    send_snmp_data2es.execute_task()
