# -*- coding: utf-8 -*-
from os import path as _opath

from complexconfig.config import Config as _Config
from complexconfig.config import EmptyConfig as _EmptyConfig
from complexconfig.parser.properties_parser import PropertiesParser as _PropertiesParser
from complexconfig.loader.file_loader import FileLoader as _FileLoader
from complexconfig.configcontainer import configcontainer as _cc

from gevent.queue import Queue
"""
for _ in dir(settings):
    if not _.startswith("_"):
         print _, " : ", getattr(settings, _)
"""

# =============== Fundenmental Settings ===============
_global_config_fn = "/etc/nebula/nebula.conf"
_self_config_fn = "/etc/nebula/db_writer/"

_basedir = _opath.abspath(_opath.dirname(__file__))

config_scope = "local"
global_config = None

if _opath.exists(_global_config_fn):
    _loader = _FileLoader("loader", _global_config_fn)
    _parser = _PropertiesParser("parser")
    global_config = _Config(_loader, _parser)
    global_config.load_config(sync=True)
    config_scope = "global"
    
if config_scope == "local":
    global_config = _EmptyConfig()

_cc.set_config("nebula", global_config)
_config = _cc.get_config("nebula")

# =================== Loadding Settings ===================
DEBUG = False

Base_Path = _basedir

# event buffer queue
Record_Queue = Queue(maxsize=10000)
Record_Put_Timeout = 3 # if put block for 3s, it's timeout
Record_Get_Empty_Sleep = 3 # if get Empty Exception, Sleep 3s
Concurrency = 20 # 数据库连接池大小
Total_Transactions = 20

Test_Count = 10000 # number to test insert

Auth_Code = "196ca0c6b74ad61597e3357261e80caf"

LogPath_Format = '%Y%m%d%H'

Notice_Expire_Days = 10 # 默认风险名单保留10天

Working_TS = None # 正在处理的data目录的时间, 主要为了离线跨小时的统计

Working_DAY = None # 正在处理的data目录的日期, 主要为了记录aerospike时的key

WebUI_Address = _config.get_string("webui_address", "127.0.0.1")
WebUI_Port = _config.get_int("webui_port", 9001)

Redis_Host = _config.get_string('redis_host', "redis")
Redis_Port = _config.get_int('redis_port', 6379)

Influxdb_Url = _config.get_string('influxdb_url',"http://127.0.0.1:8086/")
Babel_Mode = _config.get_string('babel_server', "redis")
Persist_Path = _config.get_string('persist_path', "./")

Nebula_Node_Count = _config.get_int("nebula_node_count", 1)

Rmq_Username = _config.get_string('rmq_username', 'guest')
Rmq_Password = _config.get_string('rmq_password', 'guest')
Rmq_Host = _config.get_string('rmq_host', "127.0.0.1")
Rmq_Port = _config.get_int('rmq_port', 5672)

MySQL_Host = _config.get_string("mysql_host", "mysql")
MySQL_Port = _config.get_int("mysql_port", 3306)
MySQL_User = _config.get_string("mysql_user", 'nebula')
MySQL_Passwd = _config.get_string("mysql_passwd", "threathunter")
Nebula_DB = _config.get_string("nebula_data_db", "nebula")

persist_path = _config.get_string('persist_path', "./")

# 当前小时计算模块是否启用，不启用的话，就省去查询相关接口的开销
Enable_Online = _config.get_boolean("nebula.online.slot.enable", True)

# Metrics configs
Metrics_Server = _config.get_string('metrics_server', "redis")
metrics_dict = {
    "app": "nebula_query_web",
    "redis": {
        "type": "redis",
        "host": Redis_Host,
        "port": Redis_Port
    },
    "influxdb": {
    "type": "influxdb",
    "url": Influxdb_Url,
    "username": "test",
    "password": "test"
    },
    "server": Metrics_Server
}

_Babel_Setting_Fns = [
    "NoticeService_redis.service", "NoticeService_rmq.service",
    "IncidentService_redis.service", "IncidentService_rmq.service", 
]

try:
    for fn in _Babel_Setting_Fns:
        with open(_opath.join(_self_config_fn, fn), 'r') as f:
            globals()[ fn.split(".")[0] ] = ''.join(f.readlines())
except IOError as e:
    print "!!!! Babel 配置缺失，无法启动!!!"
    import sys
    sys.exit(-1)
