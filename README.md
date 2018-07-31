# ES Replay
> 内部开发测试用。
> 数据读取、去重、仿真数据构造、写入。

## 项目依赖
* ubuntu14.04
* python2.7
* elasticsearch 5.x

## 项目结构
```
.
├── download_data      // 从es中导出数据,生成.json文件
├── filter_data        // 处理上面得到的.json文件，去重后再次生成.json文件
├── send_data          // 读取上面生成的.json文件，构造仿真数据，写入dawn或es
|
```

## 用法
* 首次运行
```
step1 安装依赖
		pip install -r requirements.txt
step2 从es中导出数据
		python es_export.py
step3 清洗数据: 将上面导出的.json文件移动到filter_data目录
		python filter_xxx_data.py
step3 仿真数据构造和写入： 将上面过滤后导出的.json文件移动到send_data/json目录
		python send_xxx_data2dawn.py
		或python send_xxx_data2es.py
```
* 仿真数据持续生成(定时任务)
```
方案一：借助linux定时任务,频率最大为1min/次
		crontab -e
		# 编辑任务
		PATH=/usr/sbin:/usr/bin:/sbin:/bin
		*/1 * * * *  python send_xxx_data2dawn.py >/dev/null 2>&1
		# 重启crontab
		sudo /etc/init.d/cron restart
方案二：修改send_xxx_data2dawn.py和send_xxx_data2es.py
		通过while循环和延时来控, 然后交给corntab或者supervisord托管
```

## 扩展数据源
* 按照<font color=#0099ff>filter_data</font>目录下的<font color=orange>filter_snmp_data.py</font>来编写数据去重逻辑
* 按照<font color=#0099ff>send_data</font>目录下的<font color=orange>send_snmp_data2dawn.py</font>和<font color=orange>send_snmp_data2es.py</font>来编写数据构造和写入逻辑

## 提交记录
* 2018.07.31
  * 修改文件处理方式，减少内存开销
  * 增加业务仿真数据
* 2018.07.25
  * 初次提交
  * snmp数据的处理
