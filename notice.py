# -*- coding: utf-8 -*-
import time
import logging
import json
from datetime import datetime

import gevent
from gevent.queue import Empty

from threathunter_common.metrics.metricsrecorder import MetricsRecorder
from threathunter_common.metrics.redismetrics import RedisMetrics
from threathunter_common.util import millis_now
import redis

import settings
from model import Notice

logger = logging.getLogger("nebula.dbwriter.notice")

Incident_Metrics_Recorder = MetricsRecorder("store.notice",
                                            expire=86400* 60, #2month
                                            interval=300, #5min to merge
                                            type="sum",
                                            db="nebula.offline")

Not_Null_Cols = ("strategy_name",)

def add_success_metrics():
    add_incident_metrics("success")
    
def add_receive_metrics():
    add_incident_metrics("receive")

def add_fail_metrics():
    add_incident_metrics("fail")

def add_incident_metrics(status):
    Incident_Metrics_Recorder.record(1, {"status":status})
    
def get_tip(name, values):
    # @todo 以往生成tip说明是需要拿strategy、counter name这些的.
    return unicode(name).encode('utf8')

def gen_notice(event):
    """
    Input: event(Event)
    Ouput: notice(dict)
    """
    if not event:
        return
    prop = event.property_values
    notice = {
        "timestamp": event.timestamp,
        "key": event.key,
        "scene_name": prop.get("sceneName", ""),
        "checkpoints": prop.get("checkpoints", ""),
        "check_type": prop.get("checkType", ""),
        "strategy_name": prop.get("strategyName", ""),
        "decision": prop.get("decision", ""),
        "risk_score": prop.get("riskScore", ""),
        "expire": prop.get("expire", ""),
        "remark": prop.get("remark", ""),
        "variable_values": prop.get("variableValues", ""),
        "test": prop.get("test", 1),
        "geo_province": prop.get("geo_province", ""),
        "geo_city": prop.get("geo_city", ""),
        "tip": get_tip(prop.get("strategyName", ""), prop.get("triggerValues", dict())),
        "uri_stem": prop.get("triggerValues", {}).get("page", ""),
        "trigger_event": json.dumps(prop.get("triggerValues", {}))
    }
    if not notice.get("geo_province", ""):
        notice["geo_province"] = "unknown"
    if not notice.get("geo_city", ""):
        notice["geo_city"] = "unknown"
    if not notice.get("uri_stem", ""):
        notice["uri_stem"] = prop.get("triggerValues", {}).get("propertyValues", {}).get("page", "")
        if not notice.get("uri_stem", ""):
            notice["uri_stem"] = "unknown"
    if any( notice.get(k) is None for k in Not_Null_Cols):
        null_cols = []
        for k in Not_Null_Cols:
            if notice.get(k) is None:
                null_cols.append(k)
        logger.warn("One notice can't store, Because of null columns: %s", ",".join(null_cols))
        return None
        
    if notice["test"] in ("True", True, 1, "true"):
        notice["test"] = 1
    else:
        notice["test"] = 0

    for k,v in notice.items():
        if isinstance(v, unicode):
            notice[k] = unicode(v).encode('utf8')
    logger.debug("return notice: %s" % notice)
    return notice
    
def notice_worker(db):
    conn = db.connect()
    redisMetrics = RedisMetrics(settings.Redis_Host, settings.Redis_Port)
    noticeRedis = redis.Redis(host=settings.Redis_Host,
                              port=settings.Redis_Port,
                              password=settings.Redis_Password)
    try:
        while True:
            try:
                event = settings.Record_Queue.get_nowait()
                add_receive_metrics()
                notice = gen_notice(event)
                if notice is None:
                    add_fail_metrics()
                    continue
            except Empty:
                gevent.sleep(settings.Record_Get_Empty_Sleep)
                continue

            try:
                noticeTimestamp = notice.get("timestamp", '')
                noticeData = json.dumps(notice)
                noticeRedis.publish("nebula.realtime.notice", noticeData)
                logger.info("publish to redis (nebula.realtime.notice): {}".format(noticeData))
                redisMetrics.add_metrics("db.write", "notice", {"app": 'db.write.notice'},
                                                        noticeData, 1800, noticeTimestamp)
            except Exception as e0:
                logger.error("notice: %s" % e0)

            tran = conn.begin()
            try:
                conn.execute(Notice.__table__.insert(), **notice)
                tran.commit()
            except Exception as e:
                tran.rollback()
                add_fail_metrics()
                logger.exception(e.message)
                continue
            add_success_metrics()
    except Exception as e:
        logger.exception(e.message)
    finally:
        conn.close()

class NoticeCleanCronJob(object):
    def __init__(self):
        self.deadline = millis_now() - (settings.Notice_Expire_Days * 86400 *1000)
        
    def start(self):
        logger.info("timestamp %s 之前 既 %s的notice将会删除.", self.deadline, datetime.fromtimestamp(self.deadline/1000.0))
        try:
            conn = settings.db.connect()
            try:
                tran = conn.begin()
                conn.execute(Notice.__table__.delete().where(Notice.timestamp <= self.deadline))
                tran.commit()
            except Exception as e:
                tran.rollback()
                logger.exception(e.message)
                raise RuntimeError, u"Delete existing notice where timestmap <= %s fail!" % self.deadline
        finally:
            conn.close()
