# -*- coding: utf-8 -*-
import logging, json
from datetime import datetime

from sqlalchemy import select, func

from threathunter_common.util import millis_now

import utils
import settings
from model import Notice, Notice_Stat

logger = logging.getLogger("nebula.dbwriter.notice_stat")

class NoticeStatCronJob(object):
    def __init__(self, ):
        """
        Notice_Query_Stmt:
        SELECT notice.key, notice.check_type, notice.strategy_name, notice.scene_name, notice.decision, notice.test, notice.geo_city, notice.uri_stem, count(*) AS count_1
FROM notice
WHERE notice.timestamp >= :timestamp_1 AND notice.timestamp < :timestamp_2 GROUP BY notice.key, notice.check_type, notice.strategy_name, notice.scene_name, notice.decision, notice.test, notice.geo_city, notice.uri_stem
        """

        self.from_time = int(settings.Working_TS) * 1000
        self.end_time = self.from_time + 60 * 60 * 1000
        self.Notice_Query_Stmt = select([ Notice.key, Notice.check_type, \
                                          Notice.strategy_name, Notice.scene_name, \
                                          Notice.decision, Notice.test, \
                                          Notice.trigger_event, Notice.timestamp,\
                                          Notice.geo_city, Notice.uri_stem, func.count()]).\
            where(Notice.timestamp >= self.from_time).where(Notice.timestamp < self.end_time).\
            group_by(Notice.key, Notice.check_type, Notice.strategy_name, \
                     Notice.scene_name, Notice.decision, Notice.test, \
                     Notice.geo_city, Notice.uri_stem)
        
#    @utils.profile_cron_job
    def start(self):
        # 默认取上个小时的notice来丢到callback里面去
        # 如果是重复跑了同一个小时，删除之前新增的notice_stat
        try:
            conn = settings.db.connect()
            try:
                tran = conn.begin()
                conn.execute(Notice_Stat.__table__.delete().where(Notice_Stat.tsHour == self.from_time))
                tran.commit()
            except Exception as e:
                tran.rollback()
                logger.exception(e.message)
                raise RuntimeError, u"Delete existing notice tag where timestmap is %s fail, can not continue!" % self.from_time
            res = conn.execute(self.Notice_Query_Stmt)
            notice_stats_gen = ( _ for _ in self.gen_notice_stat(res.fetchall()) if _)
            notice_stats = list(notice_stats_gen)
            logger.debug("generated notice_stat %s" % len(notice_stats))
            try:
                tran = conn.begin()
                result = conn.execute(Notice_Stat.__table__.insert(), notice_stats)
                tran.commit()
                #logger.debug("result:%s", result.is_insert)
            except Exception as e:
                tran.rollback()
                logger.exception(e.message)
                raise RuntimeError, u"Insert Notice_stat where timestmap is %s fail, can not continue!" % self.from_time
        finally:
            conn.close()
        
    @staticmethod
    def gen_notice_stat(metas):
        for _ in metas:
            meta = dict(_)
            meta["count"] = meta.pop("count_1")
            meta["tsHour"] = int(settings.Working_TS) * 1000
            meta["last_modified"] = millis_now()
            # get trigger event
            # add col pop trigger event
            e = json.loads(meta.pop("trigger_event"))
            meta["ip"] = e.get("c_ip", "")
            meta["uid"] = e.get("uid", "")
            meta["did"] = e.get("did", "")
            strategy_name = meta["strategy_name"]
            if utils.Strategies_Weigh.has_key(strategy_name):
                tags = utils.Strategies_Weigh.get(strategy_name, {}).get("tags", [])
                for tag in tags:
                    meta["tag"] = unicode(tag).encode('utf8')
                    yield meta

class NoticeStatCleanCronJob(object):
    def __init__(self):
        self.deadline = millis_now() - (settings.Notice_Expire_Days * 86400 *1000)
        
    def start(self):
        logger.info("timestamp %s 之前 既 %s的notice将会删除.", self.deadline, datetime.fromtimestamp(self.deadline/1000.0))
        try:
            conn = settings.db.connect()
            try:
                tran = conn.begin()
                conn.execute(Notice_Stat.__table__.delete().where(Notice_Stat.timestamp <= self.deadline))
                tran.commit()
            except Exception as e:
                tran.rollback()
                logger.exception(e.message)
                raise RuntimeError, u"Delete existing notice where timestmap <= %s fail!" % self.deadline
        finally:
            conn.close()
