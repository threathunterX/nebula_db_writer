# -*- coding: utf-8 -*-
import json, time, sys, logging
import urllib2, functools
from os import path as opath
from datetime import timedelta, datetime

from threathunter_common.redis.redisctx import RedisCtx

import gevent
from sqlalchemy import create_engine

import settings
# 请求nebula web API
#Auth_Code = None
Strategies_Weigh = None

DEBUG_PREFIX = '==============='

def get_last_hour(f='%Y%m%d%H'):
    """
    获取上个小时的时间字符串, ex. 2015040915 
    """
    n = datetime.now()
    td = timedelta(seconds=3600)
    last_hour_n = n - td
    return last_hour_n.strftime(f)

#Last_Hour_Path = opath.join(settings.persist_path, get_last_hour())

def parse_host_url_path(url):
    if url.find('/') == -1:
        # ex. 183.131.68.9:8080, auth.maplestory.nexon.com:443
        host = url
        url_path = ''
    else:
        if url.startswith('http') or url.startswith('https'):
            # 有协议的, 需要扩充
            segs = url.split('/', 3)
            host = '/'.join(segs[:3])
            url_path = segs[-1]
        else:
            host, url_path = url.split('/', 1)
    return host, url_path

def get_auth_code():
    """
    请求nebula web API所需的auth code
    Returns:
    """
    return settings.Auth_Code

#@catch_latency("查询策略权重")
def get_strategies_weigh():
    """
    请求nebula web API获取strategies,包含每个策略的场景、分数、标签等数据
    Returns:
    """

    global Strategies_Weigh

    if Strategies_Weigh is None:
        Strategies_Weigh = dict()
    
    auth_code = get_auth_code()
    url = 'http://{}:{}/nebula/strategyweigh?auth={}'.format(
        settings.WebUI_Address, settings.WebUI_Port, auth_code)
    res = json.loads(urllib2.urlopen(url).read())
    if res.get('msg', '') == 'ok':
        strategies = res.get('values', [])
        for strategy in strategies:
            if not strategy:
                continue
            name = strategy.pop('name')
            Strategies_Weigh[name] = strategy

def strategies_weigh_worker():
    while True:
        get_strategies_weigh()
        gevent.sleep(60)
        
def init_env(logger_name, only_babel=False):
    logger = logging.getLogger(logger_name)
    logger.debug("=================== Enter Debug Level.=====================")
    # 配置redis
    RedisCtx.get_instance().host = settings.Redis_Host
    RedisCtx.get_instance().port = settings.Redis_Port

    # 初始化 metrics 服务
    try:
        from threathunter_common.metrics.metricsagent import MetricsAgent
    except ImportError:
        logger.error(u"from threathunter_common.metrics.metricsagent import MetricsAgent 失败")
        sys.exit(-1)

    MetricsAgent.get_instance().initialize_by_dict(settings.metrics_dict)
    logger.info(u"成功初始化metrics服务: {}.".format(MetricsAgent.get_instance().m))

    if not only_babel:
        # 初始化mysql 连接
        uri = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (settings.MySQL_User, settings.MySQL_Passwd, settings.MySQL_Host, settings.MySQL_Port, settings.Nebula_DB)
        logger.debug("Connect Url: %s", uri)
        settings.db = create_engine(uri, pool_size=settings.Concurrency, max_overflow=10, pool_recycle=14400)#,echo="debug")
    return logger

def profile_cron_job(func):
    @functools.wraps
    def wrapper(*args, **kwargs):
        args[0]._profile_start_time = time.time()
        return func(*args, **kwargs)
        args[0]._profile_end_time = time.time()
    return wrapper