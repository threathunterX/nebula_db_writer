# -*- coding: utf-8 -*-

import logging, time
from datetime import datetime

import click

from threathunter_common.event import Event
from threathunter_common.util import millis_now

import utils
from settings import settings
from babel import get_incident_notify_client

logger = None
is_debug = False

@click.group()
@click.option('--debug', '-d', is_flag=True, help="debug switch")
@click.option('--num', default=1000, help="test data number")
def TestBabelClientManager(debug, num):
    global is_debug
    if debug:
        is_debug = True
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig()

    settings.Test_Count = num

@TestBabelClientManager.command()
def incident():
    global logger
    logger = utils.init_env("test.babel_client.incident", only_babel=True)
    babel_client = get_incident_notify_client()
    c = IncidentClient(babel_client)
    c.run()

class TestBabelClient(object):
    def __init__(self, babel_client):
        self.babel_client = babel_client
        self.server_name = None
    def start(self):
        self.babel_client.start()
    def event_generator(self):
        pass
    def generate_events(self):
        pass
    def send(self):
        events = self.generate_events()
        logger.debug(u"测试开始的时间: %s" % datetime.fromtimestamp(time.time()))
        res = [ self.babel_client.send(e, e.key, timeout=5) for e in events ] # why notify not work
        logger.debug(u"所有事件发送完毕的时间是: %s" % datetime.fromtimestamp(time.time()))
        assert all(_[0] for _ in res)
        click.echo(u"%s 个 %s 所需的babel event已经成功发送" % (settings.Test_Count, self.server_name))
    def __del__(self):
        self.babel_client.close()
    def close(self):
        self.babel_client.close()
    def run(self):
        self.start()
        self.send()
        self.close()
        
class IncidentClient(TestBabelClient):
    def __init__(self, *args, **kwargs):
        TestBabelClient.__init__(self,*args, **kwargs)
        self.server_name = "incident"

    def event_generator(self):
        properties = {
            'ip__visit__incident_count__1h__slot': 2,
            'ip__visit__incident_min_timestamp__1h__slot':0,
            'ip__visit__scene_incident_count_strategy__1h__slot':{},
            'ip__visit__tag_incident_count__1h__slot':{},
            'ip__visit__page_incident_count__1h__slot':{},
            'ip__visit__incident_max_rate__1h__slot':{},
            'ip__visit__did_incident_count__1h__slot':{},
            'ip__visit__user_incident_count__1h__slot':{},
            'ip__visit__incident_distinct_user__1h__slot':[],
        }
        for _ in xrange(settings.Test_Count):
            yield Event("nebula",
                        "incident_add",
                        "5.5.5.5",
                        millis_now(),
                        properties)

    def generate_events(self):
        return list(self.event_generator())
    
if __name__ == '__main__':
    TestBabelClientManager()