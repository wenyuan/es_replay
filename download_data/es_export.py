#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从es中导出一部分数据
按行导出,每一行为一个doc的数据
开发测试用,如果数据量很大或者涉及到深度分页,请采用scroll-api
"""
import os
import json
from functools import reduce
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# ----------- 需要修改的参数 -----------
es = Elasticsearch('127.0.0.1')
index_name = 'cc-gossip-internal-2018.07.31'
output_file_name = 'snmp'
doc_from = 0
doc_size = 5000
# ------------------------------------

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
OUTPUT_FILE_PATH = os.path.join(CURRENT_DIR, output_file_name + '.txt')


class EsExport(object):

    def __init__(self):
        pass

    def export_es_data(self):
        query_body = {
            "query": {
                "match_all": {}
            },
            "from": doc_from,
            "size": doc_size
        }
        try:
            print('query from es, please wait...')
            response = es.search(index=index_name,
                                 body=query_body,
                                 request_timeout=300)
            print('hits: %s' % (response['hits']['total']))
            print('used_time(ms): %s' % (response['took']))
            if response['hits']['total'] > 0:
                doc_hits = response['hits']['hits']
                process_bar = ShowProcess(len(doc_hits))
                print('start to write to file:')
                for hit in doc_hits:
                    line = json.dumps(hit['_source'], encoding="utf-8", ensure_ascii=False)
                    self.write2file(line + '\n')
                    process_bar.show_process()
                process_bar.close('success in finishing... %s' % OUTPUT_FILE_PATH)
        except TransportError as e:
            if isinstance(e, ConnectionTimeout):
                print('read timed out!')
            elif isinstance(e, ConnectionError):
                print('elasticsearch connection refused!')
            else:
                print('system err')
        except Exception as e:
            print(e)

    @staticmethod
    def write2file(content):
        with open(OUTPUT_FILE_PATH, "a") as f:
            f.write(content)


class ShowProcess(object):
    """
    显示处理进度的类
    调用该类相关函数即可实现处理进度的显示
    """
    i = 0  # 当前的处理进度
    max_steps = 0  # 总共需要处理的次数
    max_arrow = 50  # 进度条的长度

    # 初始化函数，需要知道总共的处理次数
    def __init__(self, max_steps):
        self.max_steps = max_steps
        self.i = 0

    # 显示函数，根据当前的处理进度i显示进度
    # 效果为[>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>]100.00%
    def show_process(self, i=None):
        if i is not None:
            self.i = i
        else:
            self.i += 1
        num_arrow = int(self.i * self.max_arrow / self.max_steps)  # 计算显示多少个'>'
        num_line = self.max_arrow - num_arrow  # 计算显示多少个'-'
        percent = self.i * 100.0 / self.max_steps  # 计算完成进度，格式为xx.xx%
        process_bar = '[' + '>' * num_arrow + '-' * num_line + ']' \
                      + '%.2f' % percent + '%' + '\r'  # 带输出的字符串，'\r'表示不换行回到最左边
        sys.stdout.write(process_bar)  # 这两句打印字符到终端
        sys.stdout.flush()

    def close(self, words='done'):
        print('')
        print(words)
        self.i = 0


if __name__ == "__main__":
    es_export = EsExport()
    es_export.export_es_data()
