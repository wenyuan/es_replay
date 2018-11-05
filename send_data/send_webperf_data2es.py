#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向es中写入sqy业务的仿真数据
数据直接构造,不用从别的地方下载

*/1 * * * *  python /home/wenyuan/es_replay/send_data/send_webperf_data2es.py >/dev/null 2>&1
"""
import copy
import time
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# ----------- 需要修改的参数 -----------
es = Elasticsearch('192.168.10.201')
token = '4a859fff6e5c4521aab187eee1cfceb8'
index_name = 'sc-diagnosis-polling_result-4a859fff6e5c4521aab187eee1cfceb8-' + time.strftime('%Y.%m.%d')
data_type = 'polling_result'
# ------------------------------------

doc_template = {
    'appname': 'diagnosis',
    '@timestamp': '1541403395000',  # 要变
    'type': 'polling_result',
    'org': '4a859fff6e5c4521aab187eee1cfceb8',
    'polling_result': {  # 要变
        'type': 'web_perf',
        'polling_agent': {
            "status": "finished",
            "reason": "",
            "guid": "6531d90ffad34ef59dd9ee3a213b8be3"
        },
        "web_perf": [  # nested结构
            {
                "code": 200,
                "name": """\u767e\u5ea6""",
                "url": "www.baidu.com",
                "dom_download_time": 35,
                "dom_ready_time": 1380,
                "white_screen_time": 1380,
                "tcp_time": 435,
                "guid": "5ef6868010b84d2094368bde1a915b41",
                "load_time": 1410,
                "dns_time": 7,
                "ttfb": 10,
                "page_download_time": 8
            }, {
                "code": 200,
                "name": """\u817e\u8baf""",
                "url": "www.qq.com",
                "dom_download_time": 35,
                "dom_ready_time": 1380,
                "white_screen_time": 1380,
                "tcp_time": 435,
                "guid": "489juq4010b84d2094368bde1a915b41",
                "load_time": 1410,
                "dns_time": 7,
                "ttfb": 10,
                "page_download_time": 8
            }
        ]
    }
}


class SendWebperfData2Es(object):

    def __init__(self):
        pass

    def execute_task(self, node_list):
        doc_list = self.make_data(node_list)
        self.send_data2es(doc_list)

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
    def make_data(polling_agent_guid_list):
        current_timestamp = time.strftime('%Y-%m-%dT%H:%M:%S+08:00')
        doc_list = []
        for idx, polling_agent_guid in enumerate(polling_agent_guid_list):
            doc = copy.deepcopy(doc_template)
            doc['@timestamp'] = current_timestamp
            doc['polling_result']['polling_agent']['guid'] = polling_agent_guid
            doc_list.append(doc)
        return doc_list


if __name__ == "__main__":
    send_webperf_data2es = SendWebperfData2Es()
    polling_agent_guid_list = [
        '6531d90ffad34ef59dd9ee3a213b8be3',
        '6450f13e07814ad0bdaca0f599557315',
        '49160eccc4e54a1d94616712684a1d17',
        'f4f92eff9a6b4cc9bf3c92b564e2b4bd',
        '1fbc6ca790b94e89ade013a972d592ab',
    ]
    send_webperf_data2es.execute_task(polling_agent_guid_list)
