# -*- coding: utf-8 -*-
import traceback
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import json

from model import Notice, Incident

uri = "mysql+pymysql://root:ThreathunterNebula@localhost:3306/nebula_data?charset=utf8"
db = create_engine(uri, pool_size=10, max_overflow=10)
conn = db.connect()
tran = conn.begin()
try:
    res = conn.execute(select([Notice]).limit(200))
    c = []
    for _ in res.fetchall():
        d = dict(_.items())
        d["timestamp"] = None
        #d["start_time"] = None
        d["last_modified"] = None
        c.append(d)
    tran.commit()
    print json.dumps(c)
except:
    tran.rollback()
    traceback.print_exc()
finally:
    conn.close()