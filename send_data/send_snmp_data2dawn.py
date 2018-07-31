#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向dawn中写入snmp仿真数据
数据模板来源(.txt文件)必须要是经过fiter_data过滤后的
因为有多个数据源,.txt文件统一都放在send_data/data目录下
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
token = '4a859fff6e5c4521aab187eee1cfceb8'
data_file_name = 'snmp.data'
request_body_size = 100
# ------------------------------------

reload(sys)
sys.setdefaultencoding('utf-8')

DAWN_URL = "http://{host}:{port}/{data_source}/token/{token}".format(host=dawn_host, port=dawn_port,
                                                                     data_source=data_source, token=token)
CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, 'data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
DATA_FILE_PATH = os.path.join(DATA_DIR, data_file_name + '.txt')


class SendSnmpData2Dawn(object):

    def __init__(self):
        pass

    def execute_task(self):
        with open(DATA_FILE_PATH, 'r') as f:
            doc_list = []
            line = f.readline()
            while line:
                line = line.strip('\n')
                doc_content = json.loads(line, encoding='utf-8')
                doc_list.append(doc_content)
                if len(doc_list) >= request_body_size:
                    current_doc_list = self.make_data(doc_list)
                    self.send_data2dawn(current_doc_list)
                    doc_list = []
                line = f.readline()
            if doc_list:
                current_doc_list = self.make_data(doc_list)
                self.send_data2dawn(current_doc_list)

    @staticmethod
    def send_data2dawn(current_doc_list):
        response = requests.post(DAWN_URL, json=current_doc_list)
        if response.status_code == 200:
            print('success: %s' % (response.json()['took']))
        else:
            print('failed: %s' % (response.json()))

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
            if_table_stats = doc['snmp']['ifTableStats']
            current_if_table_stats = []
            for each_if_stats in if_table_stats:
                if time.strftime('%H') in ['9', '10', '11', '14', '15', '16']:
                    in_bytes = int(random.uniform(5000, 8000))
                    out_bytes = int(random.uniform(30000, 50000))
                    in_pkts = int(random.uniform(80, 150))
                    out_pkts = int(random.uniform(200, 500))
                elif time.strftime('%H') in ['0', '1', '2', '3', '4', '5', '6']:
                    in_bytes = int(random.uniform(100, 500))
                    out_bytes = int(random.uniform(800, 1500))
                    in_pkts = int(random.uniform(10, 20))
                    out_pkts = int(random.uniform(10, 20))
                else:
                    in_bytes = int(random.uniform(3000, 5000))
                    out_bytes = int(random.uniform(10000, 20000))
                    in_pkts = int(random.uniform(50, 80))
                    out_pkts = int(random.uniform(10, 30))
                each_if_stats['ifInOctets'] = in_bytes
                each_if_stats['ifOutOctets'] = out_bytes
                each_if_stats['ifInNUcastPkts'] = in_pkts
                each_if_stats['ifOutUcastPkts'] = out_pkts
                current_if_table_stats.append(each_if_stats)

            current_doc_list.append(doc['snmp'])
        return current_doc_list


if __name__ == "__main__":
    send_snmp_data2dawn = SendSnmpData2Dawn()
    send_snmp_data2dawn.execute_task()
