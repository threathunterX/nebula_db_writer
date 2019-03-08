# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import Integer, VARCHAR, CHAR, BLOB, String, BigInteger

BaseModel = declarative_base()

class Notice(BaseModel):
    __tablename__ = 'notice'

    id = Column(Integer, primary_key=True)
    timestamp = Column(BigInteger, index=True)
    key = Column(VARCHAR(512), index=True)
    strategy_name = Column(CHAR(100))
    scene_name = Column(CHAR(100))
    checkpoints = Column(CHAR(100))
    check_type = Column(CHAR(100))
    decision = Column(CHAR(100))
    risk_score = Column(Integer)
    expire = Column(Integer)
    remark = Column(VARCHAR(1000))
    last_modified = Column(BigInteger)
    variable_values = Column(BLOB)
    geo_province = Column(CHAR(100))
    geo_city = Column(CHAR(100))
    test = Column(Integer)
    tip = Column(String(1024))
    uri_stem = Column(String(1024))
    trigger_event = Column(BLOB)


class Notice_Stat(BaseModel):
    __tablename__ = "notice_stat"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(BigInteger, index=True)
    tsHour = Column(BigInteger)
    key = Column(VARCHAR(512), index=True)
    check_type = Column(CHAR(100))
    strategy_name = Column(CHAR(100))
    scene_name = Column(CHAR(100))
    decision = Column(CHAR(100))
    test = Column(Integer)
    tag = Column(CHAR(100))
    geo_city = Column(CHAR(100))
    uri_stem = Column(String(1024))
    ip = Column(CHAR(20))
    uid = Column(VARCHAR(512))
    did = Column(VARCHAR(512))
    count = Column(Integer)
    last_modified = Column(BigInteger)
    
class Incident(BaseModel):
    __tablename__ = "risk_incident"
    
    id = Column(Integer, primary_key=True)
    ip = Column(CHAR(20), nullable=False)
    start_time = Column(BigInteger, nullable=False)
    strategies = Column(VARCHAR(1000), nullable=False)
    hit_tags = Column(VARCHAR(1000), nullable=False)
    risk_score = Column(Integer, nullable=False)
    uri_stems = Column(VARCHAR(2000), nullable=False)
    hosts = Column(VARCHAR(1000), nullable=False)
    most_visited = Column(VARCHAR(1000))
    peak = Column(CHAR(20))
    dids = Column(BLOB, nullable=False)
    associated_users = Column(BLOB, nullable=False)
    associated_orders = Column(VARCHAR(1000))
    users_count = Column(Integer)
    status = Column(Integer, default=0)
    last_modified = Column(BigInteger)