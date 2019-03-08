# -*- coding: utf-8 -*-

import settings

from babel_python.serviceclient_async import ServiceClient
from babel_python.servicemeta import ServiceMeta
from babel_python.serviceserver_async import ServiceServer
from threathunter_common.util import millis_now

# default mode
mode = settings.Babel_Mode
amqp_url = 'amqp://%s:%s@%s:%s/' % (settings.Rmq_Username, settings.Rmq_Password, settings.Rmq_Host, settings.Rmq_Port)

def get_client(redis_conf, rmq_conf):
    conf = rmq_conf
    if mode == "redis":
        conf = redis_conf

    meta = ServiceMeta.from_json(conf)
    client = ServiceClient(meta) if mode == "redis" else ServiceClient(meta, amqp_url=amqp_url, client_id="")    
    client.start()
    return wrap_client(client)


successing_timeout = 0
frozen_until = 0

def wrap_client(client):
    old_send = client.send

    def send(request, key, block=True, timeout=10):
        global successing_timeout, frozen_until

        now = millis_now()
        if now < frozen_until:
            print "frozen now"
            return False, None

        result = old_send(request, key, block, timeout)
        if result[0]:
            # success
            successing_timeout = 0
            frozen_until = 0
        else:
            # timeout
            successing_timeout += 1
            if successing_timeout >= 3:
                frozen_until = now + 30 * 1000 # block 30 seconds
                successing_timeout = 0
        return result

    client.send = send
    return client

def set_mode(m):
    if m != "redis" and m != "rabbitmq":
        raise RuntimeError("invalid babel mode")

    global mode
    mode = m

def get_server(redis_conf, rmq_conf):
    conf = rmq_conf
    if mode == "redis":
        conf = redis_conf

    meta = ServiceMeta.from_json(conf)
    server = ServiceServer(meta) if mode == "redis" else ServiceServer(meta, amqp_url=amqp_url, server_id="")
    return server

def get_incident_notify_client():
    return get_client(settings.IncidentService_redis,
                      settings.IncidentService_rmq)

def get_incident_notify_server():
    return get_server(settings.IncidentService_redis,
                      settings.IncidentService_rmq)

def get_notice_notify_server():
    return get_server(settings.NoticeService_redis,
                      settings.NoticeService_rmq)
    