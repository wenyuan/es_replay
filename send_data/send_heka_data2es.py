#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向es中写入heka_dawn的仿真数据
数据直接构造,不用从别的地方下载

*/1 * * * *  python /home/wenyuan/es_replay/send_data/send_heka_data2es.py >/dev/null 2>&1
"""
import copy
import time
import random
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# ----------- 需要修改的参数 -----------
es = Elasticsearch('192.168.10.201')
token = '4a859fff6e5c4521aab187eee1cfceb8'
index_name = 'cc-heka_dawn-4a859fff6e5c4521aab187eee1cfceb8-' + time.strftime('%Y.%m.%d')
data_type = 'log'
# ------------------------------------

doc_template = {
    'type': 'log',
    '@timestamp': '2018-05-16T09:58:33+08:00',
    'appname': 'clearpass',
    'host': '202.120.144.4',
    'guid': '4a859fff6e5c4521aab187eee1cfceb8',
    'topic': 'heka',
    'dawn_ts': 1534334722763000,
    'log': {
        'log_raw': '202.120.144.4 <143>2018-08-15T20:05:21.986750+08:00 2018-08-15 20: 04:44,691 202.120.144.4 DHU-Tenable 29791 1 0 Common.Username=1185088,Common.Service=dhu-1x-service,Common.Roles=[User Authenticated], dhu-yjs,Common.Host-MAC-Address=940e6bb77c89,RADIUS.Acct-Framed-IP-Address=10.202.52.1,Common.NAS-IP-Address=192.168.44.11,Common.Request-Timestamp=2018-08-15 20:03:01+08',
        'source': 'clearpass',
        'serverity': 'debug',
        'Serverity': 7,
        'Facilty': 'local1',
        'facilty': 23,
        'devAddress': '202.120.144.4',
        'srcAddress': '192.168.44.11',
        'ueMac': 'b8:27:eb:f3:b1:58',
        'ueName': 'OPPO-R15',
        'userName': '1185088',
        'ueId': '00:37:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'
    }
}


class SendHekaData2Es(object):

    def __init__(self):
        pass

    def execute_task(self, data_list):
        doc_list = self.make_data(data_list)
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
    def make_data(data_list):
        dawn_ts = int(round(time.time() * 1000000))
        current_timestamp = time.strftime('%Y-%m-%dT%H:%M:%S+08:00')
        doc_list = []
        for data in data_list:
            public_ip = data['public_ip']
            internal_ip = data['internal_ip']
            doc = copy.deepcopy(doc_template)
            doc['@timestamp'] = current_timestamp
            doc['host'] = public_ip
            doc['dawn_ts'] = dawn_ts
            raw = '202.120.144.4 <143>2018-08-15T20:05:21.986750+08:00 2018-08-15 20: 04:44,691 202.120.144.4 DHU-Tenable 29791 1 0 Common.Username=1185088,Common.Service=dhu-1x-service,Common.Roles=[User Authenticated], dhu-yjs,Common.Host-MAC-Address=940e6bb77c89,RADIUS.Acct-Framed-IP-Address=10.202.52.1,Common.NAS-IP-Address=192.168.44.11,Common.Request-Timestamp=2018-08-15 20:03:01+08'
            doc['log'] = {
                'log_raw': raw.replace('202.120.144.4', public_ip).replace('2018-08-15T20:05:21.986750+08:00', current_timestamp).replace('1185088', data['userName']).replace('192.168.44.11', internal_ip),
                'source': random.choice(['clearpass', 'firewall', 'dhcpd']),
                'serverity': random.choice(['debug', 'info', 'info', 'info', 'info', 'info', 'info', 'notice', 'warning', 'err', 'crit', 'alert', 'emerg']),
                'Serverity': random.choice([1, 2, 3, 4, 5, 6, 7]),
                'Facilty': 'local1',
                'facilty': 23,
                'devAddress': public_ip,
                'ip': internal_ip,
                'srcAddress': internal_ip,
                'ueMac': data['ueMac'],
                'ueName': data['ueName'],
                'userName': data['userName'],
                'ueId': data['ueId']
            }
            doc_list.append(doc)
        return doc_list


if __name__ == "__main__":
    send_heka_data2es = SendHekaData2Es()
    data_list = [
        {'public_ip': '182.88.128.71', 'internal_ip': '192.168.10.110', 'ueMac': 'CD:8D:09:9A:C2:C4', 'ueName': 'OPPO-R15', 'userName': '1185088', 'ueId': '00:37:30:62:61:2e:32:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '122.142.234.141', 'internal_ip': '192.168.10.111', 'ueMac': 'F9:04:F3:DC:68:D4', 'ueName': 'IPHONE-4', 'userName': '12138', 'ueId': '00:37:30:62:61:2e:65:66:35:61:44:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '118.190.95.35', 'internal_ip': '192.168.10.112', 'ueMac': '83:15:40:28:4A:CE', 'ueName': 'IPHONE-4S', 'userName': '9527', 'ueId': '00:55:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '118.190.95.43	', 'internal_ip': '192.168.10.113', 'ueMac': 'D2:50:1B:E9:E9:95', 'ueName': 'IPHONE-5', 'userName': '6666', 'ueId': '00:78:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '219.141.153.41', 'internal_ip': '192.168.10.114', 'ueMac': '23:6C:21:ED:BC:79', 'ueName': 'IPHONE-5S', 'userName': '23333', 'ueId': '00:da:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '106.75.9.39', 'internal_ip': '192.168.10.115', 'ueMac': '98:F4:52:04:E8:7E', 'ueName': 'IPHONE-6', 'userName': '13345', 'ueId': '00:dd:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '122.241.74.205', 'internal_ip': '192.168.10.116', 'ueMac': '09:6B:A0:AF:53:23', 'ueName': 'IPHONE-6S', 'userName': '66223', 'ueId': '00:jy:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '175.155.24.2', 'internal_ip': '192.168.10.117', 'ueMac': '19:B8:14:DF:CE:99', 'ueName': 'IPHONE-7', 'userName': '334234', 'ueId': '00:12:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '221.234.194.50', 'internal_ip': '192.168.10.118', 'ueMac': 'B7:4E:4C:8C:C7:09', 'ueName': 'IPHONE-8', 'userName': '56456', 'ueId': '00:k3:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '111.155.116.239', 'internal_ip': '192.168.10.119', 'ueMac': '65:80:FF:EA:7B:C6', 'ueName': 'IPHONE-X', 'userName': '132432', 'ueId': '00:32:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '121.31.154.124', 'internal_ip': '192.168.10.120', 'ueMac': '78:A2:92:31:C6:AF', 'ueName': 'XIAOMI-6S', 'userName': '312456', 'ueId': '00:1g:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '175.148.77.28', 'internal_ip': '192.168.10.121', 'ueMac': '92:1D:2C:A7:5F:AD', 'ueName': 'XIAOMI-8', 'userName': '453654312', 'ueId': '00:cz:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'},
        {'public_ip': '175.155.24.48', 'internal_ip': '192.168.10.122', 'ueMac': '92:6B:7C:ED:9A:FE', 'ueName': 'MAC-OS', 'userName': '13243565', 'ueId': '00:hy:30:62:61:2e:65:66:35:61:2e:62:66:36:65:2d:56:6c:61:6e'}
    ]
    send_heka_data2es.execute_task(data_list)
