#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
过滤通过download_data/es_export.py脚本从es中导出的数据
因为从es中导出的数据可能是重复的
对于snmp数据,以字段MachineIP作为唯一关键字,去除重复MachineIP的数据
筛选过后,再次按行导出,每一行为一个doc的数据
"""
import os
import sys
import json
from functools import reduce

# ----------- 需要修改的参数 -----------
src_text_file_name = 'snmp'
dst_text_file_name = 'snmp.data'
# ------------------------------------

reload(sys)
sys.setdefaultencoding('utf-8')

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
SRC_TEXT_FILE_PATH = os.path.join(CURRENT_DIR, src_text_file_name + '.txt')
DST_TEXT_FILE_PATH = os.path.join(CURRENT_DIR, dst_text_file_name + '.txt')


class FilterSnmpData(object):

    def __init__(self):
        pass

    def filter_data(self):
        with open(SRC_TEXT_FILE_PATH, 'r') as f:
            before_doc_count = 0
            now_doc_count = 0
            machine_ip_list = []
            line = f.readline()
            while line:
                before_doc_count += 1
                line = line.strip('\n')
                doc_content = json.loads(line, encoding='utf-8')
                machine_ip = doc_content['snmp']['MachineIP']
                if machine_ip in machine_ip_list:
                    print('%s existed' % machine_ip)
                    line = f.readline()
                    continue
                self.write2file(line + '\n')
                machine_ip_list.append(machine_ip)
                now_doc_count += 1
                line = f.readline()
            print('\nMachineIP: %s' % machine_ip_list)
            print('result: before doc count: %s, now doc count: %s' % (before_doc_count, now_doc_count))
            print('success in finishing... %s' % DST_TEXT_FILE_PATH)

    @staticmethod
    def write2file(content):
        with open(DST_TEXT_FILE_PATH, "a") as f:
            f.write(content)


if __name__ == "__main__":
    filter_snmp_data = FilterSnmpData()
    filter_snmp_data.filter_data()
