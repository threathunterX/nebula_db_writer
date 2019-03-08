# -*- coding: utf-8 -*-
import time
import logging
import json
from datetime import datetime

import gevent
from gevent.queue import Empty

from threathunter_common.util import ip_match, millis_now
from threathunter_common.metrics.metricsrecorder import MetricsRecorder

import utils
import settings
from model import Incident

logger = logging.getLogger("nebula.dbwriter.incident")

Test_Get_Event_Count = 0
Test_Get_All_Event_Time = None
Test_Save_All_Time = None

Not_Null_Cols = ("ip", "start_time", "strategies", "hit_tags", "risk_score", "uri_stems",\
                 "hosts", "dids", "associated_users")

Incident_Metrics_Recorder = MetricsRecorder("store.incident",
                                            expire=86400* 60, #2month
                                            interval=300, #5min to merge
                                            type="sum",
                                            db="nebula.offline")

def add_success_metrics():
    add_incident_metrics("success")
    
def add_receive_metrics():
    add_incident_metrics("receive")

def add_fail_metrics():
    add_incident_metrics("fail")

def add_incident_metrics(status):
    Incident_Metrics_Recorder.record(1, {"status":status})


def transfer_dict_to_traditional(origin):
    result = {}
    for item in origin:
        result[item['key']] = item['value']
    return result
    

def compute_risk_score(strategies, incident_count):
    """
    根据策略权重计算风险值
    每个场景下的所有策略计算平均值
    风险值为所有场景中的最大平均值
    """
    risk_scores = list()

    for category, category_strategies in strategies.items():
        category_score = 0

        # 根据权重计算场景总分
        for strategy, count in category_strategies.items():
            strategy_score = utils.Strategies_Weigh.get(strategy, {}).get('score', 0)
            category_score += strategy_score * count

        risk_scores.append(int(category_score/incident_count))

    return max(risk_scores) if risk_scores else 0

def gen_incident(event):
    """
    Input: event(Event)
    Ouput: incident(dict)
    """
    ip = event.key
    # ip incident事件数
    ip_incident_count = event.property_values.get('ip__visit_incident_count__1h__slot', 0)
    if not(ip_match(ip, check_public=False) and ip_incident_count):
        logger.error("ip %s not valid or incident count is %s",ip, ip_incident_count)
        return None

    incident = dict()
    incident["ip"]= ip
    incident["associated_events"]= json.dumps(list()),
    incident["start_time"] = event.property_values.get('ip__visit_incident_first_timestamp__1h__slot',0)
    incident["strategies"]= event.property_values.get('ip_scene_strategy__visit_incident_group_count__1h__slot',{})
    incident['hit_tags'] = event.property_values.get('ip_tag__visit_incident_count_top20__1h__slot', [])
    incident['risk_score'] = event.property_values.get("ip__visit_incident_score__1h__slot",0)

    #    incident['risk_score'] = compute_risk_score(incident['strategies'], ip_incident_count)
    incident['uri_stems'] = event.property_values.get('ip_page__visit_dynamic_count_top20__1h__slot', [])
    incident['hosts'] = {}
    tmp_d = dict()
    for d in incident['uri_stems']:
        host, _ = utils.parse_host_url_path(d.get("key"))
        if incident['hosts'].get(host, None):
            incident['hosts'][host] += d.get("value", 0)
        else:
            incident['hosts'][host] = d.get("value", 0)
    incident['hosts'] = [dict(key=k, value=v) for k,v in tmp_d.iteritems()]
    incident['hosts'].sort(key=lambda x:x['value'],reverse=True)
    incident['most_visited'] = incident["uri_stems"][0].get("key","")
    incident['peak'] = event.property_values.get('ip__visit_incident_max_rate__1h__slot',0)
    incident['dids'] = event.property_values.get('ip_did__visit_dynamic_count_top20__1h__slot', [])
    incident['associated_users'] = event.property_values.get('ip_uid__visit_dynamic_count_top20__1h__slot', [])
    incident['users_count'] = event.property_values.get('ip__visit_dynamic_distinct_count_uid__1h__slot', 0)

    incident['associated_orders'] = dict()
    incident['status'] = 0
    incident['last_modified'] = int(time.time() * 1000) # millsseconds of now
    logger.debug("incident: %s" % incident)
    incident["associated_events"] = json.dumps(incident["associated_events"])
    incident["strategies"] = json.dumps(incident["strategies"])
    incident["hit_tags"] = json.dumps(incident["hit_tags"])
    incident["uri_stems"] = json.dumps(incident["uri_stems"])
    incident["hosts"] = json.dumps(incident["hosts"])
    incident["dids"] = json.dumps(incident["dids"])
    incident["associated_users"] = json.dumps(incident["associated_users"])
    incident["associated_orders"] = json.dumps(incident["associated_orders"])
    
    if any( incident.get(k) is None for k in Not_Null_Cols):
        null_cols = []
        for k in Not_Null_Cols:
            if incident.get(k) is None:
                null_cols.append(k)
        logger.warn("One incident can't store, Because of null columns: %s", ",".join(null_cols))
        return None
    logger.debug("return incident: %s" % incident)
    return incident

def profile():
    global Test_Get_Event_Count, Test_Get_All_Event_Time
    Test_Get_Event_Count += 1
    if Test_Get_Event_Count == settings.Test_Count:
        Test_Get_All_Event_Time = datetime.fromtimestamp(time.time())
        print u"当所有内容被分配完成: %s" % Test_Get_All_Event_Time

def check_all_save_to_mysql_time():
    global Test_Save_All_Time, Test_Get_Event_Count
    while Test_Get_Event_Count != settings.Test_Count:
        gevent.sleep(1)
    while True:
        count = settings.db.execute(Incident.__table__.count()).fetchone()
        if count and count[0] != settings.Test_Count:
            gevent.sleep(1)
        else:
            Test_Save_All_Time = datetime.fromtimestamp(time.time())
            print u"当所有内容存入数据库的大致时间是: %s" % Test_Save_All_Time
            return

def incident_worker(db):
    conn = db.connect()
    try:
        while True:
            try:
                event = settings.Record_Queue.get_nowait()
                add_receive_metrics()
#                profile()
                incident = gen_incident(event)
                if incident is None:
                    add_fail_metrics()
                    continue
            except Empty:
                gevent.sleep(settings.Record_Get_Empty_Sleep)
                continue
            tran = conn.begin()
            try:
                conn.execute(Incident.__table__.insert(), **incident)
                tran.commit()
                #logger.debug("result:%s", result.is_insert)
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

class IncidentCleanCronJob(object):
    def __init__(self):
        self.deadline = millis_now() - (settings.Notice_Expire_Days * 86400 *1000)

    def start(self):
        logger.info("timestamp %s 之前 既 %s的notice将会删除.", self.deadline, datetime.fromtimestamp(self.deadline/1000.0))
        try:
            conn = settings.db.connect()
            try:
                tran = conn.begin()
                conn.execute(Incident.__table__.delete().where(Incident.start_time <= self.deadline))
                tran.commit()
            except Exception as e:
                tran.rollback()
                logger.exception(e.message)
                raise RuntimeError, u"Delete existing notice where timestmap <= %s fail!" % self.deadline
        finally:
            conn.close()
        