# -*- coding: utf-8 -*-
import traceback
from sqlalchemy import create_engine
uri = "mysql+pymysql://root:peslihui@localhost:3306/mysql_drivers_test"
db = create_engine(uri, pool_size=10, max_overflow=10)
conn = db.connect()
tran = conn.begin()
sql_tem = "insert into risk_incident values(null, '%s', '%s', %d, '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', %d, %d)"
try:
    conn.execute(sql_tem % ("1.1.1.1", "aevents", 1471586420, 'strateges', "tags", 1, "uris", "hosts", "most_visit", "peak", "dids", "users", 1, "orders", 0, 1471586450))
    tran.commit()
except:
    tran.rollback()
    traceback.print_exc()
finally:
    conn.close()