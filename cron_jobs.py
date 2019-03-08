# -*- coding: utf-8 -*-
import sys
import time, traceback
from datetime import datetime
import logging

import utils
import settings
from notice_stat import NoticeStatCronJob, NoticeStatCleanCronJob
from notice import NoticeCleanCronJob
from incident import IncidentCleanCronJob

from threathunter_common.metrics.metricsrecorder import MetricsRecorder

import gevent
import click

logger = None

@click.group()
@click.option('--debug', '-d', is_flag=True, help="debug switch")
def CronJobFactory(debug):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
@CronJobFactory.command()
@click.option("--timestamp", default=None, help=u"字符串制定将哪个小时的风险名单转成notice_stat ex.2017010112")
def notice_stat(timestamp):
    global logger
    logger_name = "nebula.notice_stat.writer"
    job_name = "generate notice stat"
    error_type = None
    start_time = time.time()
    # 获取需要转换notice -> notice_stat 的时间戳, 如果不指定，默认转换的notice的时间范围是上个小时内
    if not timestamp:
        timestamp = utils.get_last_hour()
    t = datetime.strptime(timestamp, settings.LogPath_Format)
    settings.Working_TS = time.mktime((t.year, t.month, t.day, t.hour, t.minute, t.second, 0, 0, 0))
    settings.Working_DAY = int(time.mktime((t.year, t.month, t.day, 0, 0, 0, 0, 0, 0)))
    click.echo(u"所使用的工作小时的时间戳是:%s, 既:%s" %
               (settings.Working_TS, datetime.fromtimestamp(settings.Working_TS)))
    click.echo(u"所处的日期是%s, 既:%s" %
               (settings.Working_DAY, datetime.fromtimestamp(settings.Working_DAY*1.0)))
    
    try:
        logger = utils.init_env(logger_name)
        notice_stat_recorder = MetricsRecorder("cronjob.notice_stat",
                                               expire=86400* 60, #2month
                                               interval=300, #5min to merge
                                               type="sum",
                                               db="nebula.offline")
        utils.get_strategies_weigh()
        cj = NoticeStatCronJob()
        notice_stat_recorder.record(1, {"status":"run", "workingts":settings.Working_TS})
        cj.start()
        status = "success"
    except Exception as e:
        logger.exception(traceback.format_exc())
        status = "fail"
        error_type = e.message
    finally:
        costs = (time.time() - start_time)/ 60.0
        logger.info("Cronjob(%s) working ts: %s has been %s, costs: %s min.", job_name, settings.Working_TS, status, costs)
        notice_stat_recorder.record(1, {"status":status,
                                        "workingts":settings.Working_TS,
                                        "error_type":error_type})
        # wait for metrics to write.
        gevent.sleep(60)
#    logger.info(u"Notice_Stat cronjob costs: %s s" % (cj._profile_end_time - cj._profile_start_time))

@CronJobFactory.command()
def notice_clean():
    global logger
    logger_name = "nebula.notice.cleaner"
    start_time = time.time()
    error_type = None
    job_name = "Clean table notice"
    try:
        logger = utils.init_env(logger_name)
        notice_clean_recorder = MetricsRecorder("cronjob.notice_clean",
                                               expire=86400* 60, #2month
                                               interval=300, #5min to merge
                                               type="sum",
                                               db="nebula.offline")
        cj = NoticeCleanCronJob()
        notice_clean_recorder.record(1, {"status":"run"})
        cj.start()
        
        status = "success"
    except Exception as e:
        logger.exception(e.message)
        status = "fail"
        error_type = e.message
    finally:
        costs = (time.time() - start_time)/ 60.0
        logger.info("Cronjob(%s) start at %s has been %s , costs %s min.", job_name, start_time, status, costs)
        notice_clean_recorder.record(1, {"status":status,
                                         "error_type":error_type,
                                         "costs": costs})
        # wait for metrics to write.
        gevent.sleep(60)

@CronJobFactory.command()
def notice_stat_clean():
    global logger
    logger_name = "nebula.notice_stat.cleaner"
    start_time = time.time()
    error_type = None
    job_name = "Clean table notice_stat"
    try:
        logger = utils.init_env(logger_name)
        notice_stat_clean_recorder = MetricsRecorder("cronjob.notice_stat_clean",
                                               expire=86400* 60, #2month
                                               interval=300, #5min to merge
                                               type="sum",
                                               db="nebula.offline")
        cj = NoticeStatCleanCronJob()
        notice_stat_clean_recorder.record(1, {"status":"run"})
        cj.start()
        status = "success"
    except Exception as e:
        logger.exception(e.message)
        status = "fail"
        error_type = e.message
    finally:
        costs = (time.time() - start_time)/ 60.0
        print "Cronjob(%s) start at %s has been %s , costs %s min." % (job_name, start_time, status, costs)
        logger.info("Cronjob(%s) start at %s has been %s , costs %s min.", job_name, start_time, status, costs)
        notice_stat_clean_recorder.record(1, {"status":status,
                                         "error_type":error_type,
                                         "costs": costs})
        # wait for metrics to write.
        gevent.sleep(60)
    
@CronJobFactory.command()
def incident_clean():
    global logger
    logger_name = "nebula.incident.cleaner"
    start_time = time.time()
    error_type = None
    job_name = "Clean table risk_incident"
    try:
        logger = utils.init_env(logger_name)
        incident_clean_recorder = MetricsRecorder("cronjob.incident_clean",
                                               expire=86400* 60, #2month
                                               interval=300, #5min to merge
                                               type="sum",
                                               db="nebula.offline")
        cj = IncidentCleanCronJob()
        incident_clean_recorder.record(1, {"status":"run"})
        cj.start()
        status = "success"
    except Exception as e:
        logger.exception(e.message)
        status = "fail"
        error_type = e.message
    finally:
        costs = (time.time() - start_time)/ 60.0
        logger.info("Cronjob(%s) start at %s has been %s , costs %s min.", job_name, start_time, status, costs)
        incident_clean_recorder.record(1, {"status":status,
                                         "error_type":error_type,
                                         "costs": costs})
        # wait for metrics to write.
        gevent.sleep(60)
    print "Cronjob(%s) start at %s has been %s , costs %s min." % (job_name, start_time, status, costs)

if __name__ == '__main__':
    CronJobFactory()
