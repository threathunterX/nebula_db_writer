# -*- coding: utf-8 -*-
import logging, json, time, sys
from datetime import datetime
from subprocess import PIPE, Popen
import codecs

import click

import settings
import utils
from model import Notice

logger = None
is_debug = False

@click.group()
@click.option('--debug', '-d', is_flag=True, help="debug switch")
@click.option('--num', default=1000, help="test data number")
def TestCronJobFactory(debug, num):
    global is_debug
    if debug:
        is_debug = True
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig()

    settings.Test_Count = num
    
@TestCronJobFactory.command()
@click.option("--timestamp", default=None, help=u"字符串制定将哪个小时的风险名单转成notice_stat ex.2017010112")
def notice_stat(timestamp):
    global logger
    
    logger = utils.init_env("database.test.notice_stat")
    # gen test notice
    notices = []
    with codecs.open("tests/notice_data_tem.json", encoding='utf-8') as f:
        dt = json.load(f)
        if not timestamp:
            timestamp = utils.get_last_hour()
            
        t = datetime.strptime(timestamp, settings.LogPath_Format)
        settings.Working_TS = time.mktime((t.year, t.month, t.day, t.hour, t.minute, t.second, 0, 0, 0))
        while len(notices) < settings.Test_Count:
            for _ in dt:
                _["last_modified"] = _["timestamp"] = int(settings.Working_TS) * 1000
                _["id"] = None
                notices.append(_)
    # insert test notice data into mysql
    conn = settings.db.connect()
    try:
        tran = conn.begin()
        conn.execute(Notice.__table__.insert(), notices)
        tran.commit()
    except Exception as e:
        tran.rollback()
        logger.exception(e.message)
        sys.exit(-1)
    
    # run cron job
    cmd = ["python cron_jobs.py ",]
    if is_debug:
        cmd.append("--debug ")
    cmd.append("notice_stat ")
    if timestamp:
        cmd.append("--timestamp %s " % timestamp)
    logger.debug("input cmd: %s" % ''.join(cmd))
    p = Popen(''.join(cmd), shell=True, stdout=PIPE, stderr=PIPE)
    sout, serr = p.communicate()
#    print p.returncode, sout, serr
    if p.returncode != 0:
        logger.error(serr)
    print sout


if __name__ == '__main__':
    TestCronJobFactory()