# -*- coding: utf-8 -*-
import greenify;greenify.greenify()
from gevent import monkey;monkey.patch_all()

import time
from datetime import datetime
import logging

from gevent.queue import Full
from gevent.pool import Pool
import click

import utils
import babel
import settings
from incident import incident_worker, check_all_save_to_mysql_time
from notice import notice_worker

logger = None

Record_Put_Timeout_Counts = 0
Test_Start_Getting_Event_Time = None
Test_Received_All_Time = None
Test_Received_Event_Count = 0

def profile():
    global Test_Start_Getting_Event_Time, Test_Received_Event_Count, Test_Received_All_Time
    if Test_Received_Event_Count == 0:
        Test_Start_Getting_Event_Time = datetime.fromtimestamp(time.time())
        print u"开始接收到数据的时间是 %s" % Test_Start_Getting_Event_Time
    Test_Received_Event_Count += 1
    if Test_Received_Event_Count == settings.Test_Count:
        Test_Received_All_Time = datetime.fromtimestamp(time.time())
        print u"接收完全部Event的时间是 %s" % Test_Received_All_Time   

def process_notify(event):
    # put event into Queue.
    global Record_Put_Timeout_Counts
#    profile()
    # if block, what will happen? lots process_notify hang here?
    try:
        settings.Record_Queue.put(event, timeout=settings.Record_Put_Timeout)
    except Full:
        logger.error("Record Queue for insert to mysql is timeout for %ss." % settings.Record_Put_Timeout)
        Record_Put_Timeout_Counts += 1

class WorkerGenerator(object):
    def __init__(self, server, db, bg_tasks=None):
        self.server = server
        self.db = db
        self.bg_tasks = bg_tasks
        self.pool = Pool(settings.Concurrency + (len(bg_tasks) if bg_tasks else 0))

    def join(self):
        for _ in xrange(settings.Total_Transactions):
            g = self.pool.spawn(self.worker_gen(self.server), self.db)
            g.link_exception(self.task_exception_handler)
        if self.bg_tasks:
            for _ in self.bg_tasks:
                g = self.pool.spawn(_)
                g.link_exception(self.task_exception_handler)
        self.pool.join()

    @staticmethod
    def task_exception_handler(greenlet):
        logging.warn('{0}'.format(greenlet.exception))

    @staticmethod
    def worker_gen(server):
        if server.server_name == "incident":
            return incident_worker
        elif server.server_name == "notice":
            return notice_worker
        # @todo more workers.

class IncidentServer(object):
    def __init__(self):
        self.server_name = "incident"
        self.server = babel.get_incident_notify_server()

    def start(self):
        self.server.start(func=process_notify)

class NoticeServer(object):
    def __init__(self):
        self.server_name = "notice"
        self.server = babel.get_notice_notify_server()
    def start(self):
        self.server.start(func=process_notify)

@click.group()
@click.option('--debug', '-d', is_flag=True, help="debug switch")
def DBWriterManager(debug):
    global is_debug
    if debug:
        is_debug = True
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig()

@DBWriterManager.command()
def incident():
    global logger
    logger = utils.init_env("nebula.incident.writer")
    server = IncidentServer()
    start_server(server)

@DBWriterManager.command()
def notice():
    global logger
    logger = utils.init_env("nebula.notice.writer")
    server = NoticeServer()
    start_server(server)
    
def start_server(server, bg_tasks=None):
    # 启动持久化server
    server.start()
    click.echo(u"%s Server started." % server.server_name)
    # spawn workers to listen records to write
    default_tasks = [ utils.strategies_weigh_worker, ]#check_all_save_to_mysql_time]
    if bg_tasks is None:
        bg_tasks = default_tasks
    else:
        bg_tasks.extend(default_tasks)
    workers = WorkerGenerator(server, settings.db, bg_tasks)
    workers.join()
    
if __name__ == '__main__':
    DBWriterManager()