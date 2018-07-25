#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向dawn中发送snmp仿真数据
数据模板来源(json文件)必须要是经过fiter_data过滤后的(数据形式为[{},{},{}])
因为有多个数据源,json文件统一都放在send_data/json目录下
"""
import os
import sys
import json
import time
import random
import requests
from functools import reduce

# ----------- 需要修改的参数 -----------
dawn_host = '192.168.10.197'
dawn_port = '8359'
data_source = 'gossip'
token = 'internal'
json_file_name = 'snmp.data'
# ------------------------------------

reload(sys)
sys.setdefaultencoding('utf-8')

DAWN_URL = "http://{host}:{port}/{data_source}/token/{token}".format(host=dawn_host, port=dawn_port, data_source=data_source, token=token)
CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
JSON_PATH = os.path.join(CURRENT_DIR, 'jsons')
if not os.path.exists(JSON_PATH):
    os.makedirs(JSON_PATH)
JSON_FILE_PATH = os.path.join(JSON_PATH, json_file_name + '.json')


class SendSnmpData2Dawn(object):

    def __init__(self):
        pass

    def send_data2dawn(self):
        doc_list = self.read_jsonfile()
        current_doc_list = self.make_data(doc_list)
        response = requests.post(DAWN_URL, json=current_doc_list)
        if response.status_code == 200:
            print('success: %s' % (response.json()['took']))
        else:
            print('failed: %s' % (response.json()))

    @staticmethod
    def read_jsonfile():
        with open(JSON_FILE_PATH, 'r') as load_f:
            doc_list = json.load(load_f, encoding=None)
            print('finish reading, doc count: %s' % (len(doc_list)))
            return doc_list

    @staticmethod
    def make_data(doc_list):
        current_doc_list = []
        t = time.time()
        timestamp_in_seconds = int(t)
        timestamp_in_millisecond = int(round(t * 1000))
        for doc in doc_list:
            doc['guid'] = "internal"
            doc['@timestamp'] = timestamp_in_millisecond
            doc['dawn_ts'] = timestamp_in_millisecond * 1000
            # todo...符合24h内正态分布,先ugly design
            if time.strftime('%H') in ['9', '10', '11', '14', '15', '16']:
                cpu_utilization = round(random.uniform(30, 50), 2)
                mem_utilization = round(random.uniform(30, 80), 2)
            elif time.strftime('%H') in ['0', '1', '2', '3', '4', '5', '6']:
                cpu_utilization = round(random.uniform(10, 30), 2)
                mem_utilization = round(random.uniform(10, 30), 2)
            else:
                cpu_utilization = round(random.uniform(30, 60), 2)
                mem_utilization = round(random.uniform(30, 40), 2)
            doc['snmp']['cpuUtilization'] = cpu_utilization
            doc['snmp']['memUtilization'] = mem_utilization

            current_doc_list.append(doc)
        return current_doc_list


if __name__ == "__main__":
    send_snmp_data2dawn = SendSnmpData2Dawn()
    send_snmp_data2dawn.send_data2dawn()
