#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
过滤从es中导出的数据(存在json文件中数据形式必须为[{},{},{}])
因为从es中导出的数据可能是重复的
对于snmp数据,以字段MachineIP作为唯一关键字,去除重复MachineIP的数据
筛选过后,再次以[{},{},{}]形式转成字符串后写入json文件
"""
import os
import sys
import json
from functools import reduce

# ----------- 需要修改的参数 -----------
src_json_file_name = 'snmp'
dst_json_file_name = 'snmp.data'
# ------------------------------------

reload(sys)
sys.setdefaultencoding('utf-8')

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
SRC_JSON_FILE_PATH = os.path.join(CURRENT_DIR, src_json_file_name + '.json')
DST_JSON_FILE_PATH = os.path.join(CURRENT_DIR, dst_json_file_name + '.json')


class FilterSnmpData(object):

    def __init__(self):
        pass

    def filter_data(self):
        doc_list = self.read_jsonfile()
        filtered_doc_list = self.clean_data(doc_list)
        self.write_jsonfile(filtered_doc_list)

    @staticmethod
    def read_jsonfile():
        with open(SRC_JSON_FILE_PATH, 'r') as load_f:
            doc_list = json.load(load_f, encoding=None)
            print('finish reading, doc count: %s' % (len(doc_list)))
            return doc_list

    @staticmethod
    def clean_data(doc_list):
        filtered_doc_list = []
        machine_ip_list = []
        for doc in doc_list:
            if doc['snmp']['MachineIP'] in machine_ip_list:
                continue
            filtered_doc_list.append(doc)
            machine_ip_list.append(doc['snmp']['MachineIP'])
        print('finish cleaning, now doc count: %s' % (len(filtered_doc_list)))
        return filtered_doc_list

    @staticmethod
    def write_jsonfile(content):
        with open(DST_JSON_FILE_PATH, "w") as f:
            json.dump(content, f, encoding="UTF-8", ensure_ascii=False)
            print('success in finishing... %s' % DST_JSON_FILE_PATH)


if __name__ == "__main__":
    filter_snmp_data = FilterSnmpData()
    filter_snmp_data.filter_data()
