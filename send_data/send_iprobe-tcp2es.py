#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向es中写入iprobe仿真数据,主要补充了ipv6字段
数据模板来源(.txt文件)必须要是经过filter_data过滤后的
因为有多个数据源,.txt文件统一都放在send_data/data目录下

*/1 * * * *  cd /home/wenyuan/es_replay/send_data/ && python send_iprobe-tcp2es.py >/dev/null 2>&1
"""
import os
import json
import time
import random
from functools import reduce
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
import sys
import faker
import pypinyin

from utils.isp2en import isp2en

reload(sys)
sys.setdefaultencoding('utf-8')
f = faker.Faker(locale='zh_CN')

# ----------- 需要修改的参数 -----------
es_host = '192.168.10.201'
token = '4a859fff6e5c4521aab187eee1cfceb8'
appname = 'iprobe'
doc_type = 'tcp'
index_name = 'cc-{appname}-{doc_type}-{token}-{suffix}'.format(
    appname=appname,
    doc_type=doc_type,
    token=token,
    suffix=time.strftime('%Y.%m.%d')
)
data_file_name = 'iprobe-tcp.txt'
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
    current_doc_list = []
    t = time.time()
    timestamp_in_seconds = int(t)
    timestamp_in_millisecond = int(round(t * 1000))
    for doc in doc_list:
        doc['guid'] = token
        doc['@timestamp'] = timestamp_in_millisecond
        doc['dawn_ts'] = timestamp_in_millisecond * 1000
        # 插入ipv6字段
        doc['tcp']['src_ipv6'] = f.ipv6()
        doc['tcp']['dst_ipv6'] = f.ipv6()
        # 中文转英文
        src_country = doc['tcp']['src_ip'].get('country', None)
        src_province = doc['tcp']['src_ip'].get('province', None)
        src_city = doc['tcp']['src_ip'].get('city', None)
        src_isp = doc['tcp']['src_ip'].get('isp', None)
        if src_country:
            doc['tcp']['src_ip']['country'] = pinyin(src_country)
        if src_province:
            doc['tcp']['src_ip']['province'] = pinyin(src_province)
        if src_city:
            doc['tcp']['src_ip']['city'] = pinyin(src_city)
        if src_isp:
            doc['tcp']['src_ip']['isp'] = isp2en(src_isp)

        dst_country = doc['tcp']['dst_ip'].get('country', None)
        dst_province = doc['tcp']['dst_ip'].get('province', None)
        dst_city = doc['tcp']['dst_ip'].get('city', None)
        dst_isp = doc['tcp']['dst_ip'].get('isp', None)
        if dst_country:
            doc['tcp']['dst_ip']['country'] = pinyin(dst_country)
        if dst_province:
            doc['tcp']['dst_ip']['province'] = pinyin(dst_province)
        if dst_city:
            doc['tcp']['dst_ip']['city'] = pinyin(dst_city)
        if dst_isp:
            doc['tcp']['dst_ip']['isp'] = isp2en(dst_isp)

        current_doc_list.append(doc)
    return current_doc_list


def pinyin(word):
    if isinstance(word, str):
        word = unicode(word, 'utf-8')
    s = ''
    for i in pypinyin.pinyin(word, style=pypinyin.NORMAL):
        s += '.'.join(i)
    return s


if __name__ == "__main__":
    execute_task()
