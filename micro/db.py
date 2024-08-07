# -*- coding: utf-8 -*

import psycopg2.extras
import psycopg2
import logging
import time
import json
import os

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "database")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

##
#

class Database:

    def __init__(self):
        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        )

        logging.debug("database connected")

        return self.conn
    
    def close(self):
        if self.cur is not None:
            self.cur.close()
            
        if self.conn is not None:
            self.conn.close()
            
        logging.debug("database connection closed")
    
    def cursor(self):
        if self.conn is None:
            self.connect()
        self.cur = self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        return self.cur
    
    def execute(self, query, params=None):
        logging.debug("query: %s", query)
        self.cur.execute(query, params)
        return self.cur

##
#

db = Database()

def connect(handle):
    def decorator(*args, **kwargs):
        res = None
        try:
            logging.debug("connecting")
            with db.connect():
                with db.cursor():
                    res = handle(*args, **kwargs)
        finally:
            db.close()
        return None

    return decorator

##
#

def query(sql, params):
    return db.execute(sql, params)

##
#

def listen(channel):
    def decorator(handle):

        logging.debug("listen on %s", channel)
        
        try:
            db = Database()

            conn = db.connect()
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
            cursor = conn.cursor()
            cursor.execute(f"LISTEN {channel};")

            while True:

                conn.poll()
                
                for notify in conn.notifies:
                    try:
                        logging.debug("notify: %s", notify)
                        payload = json.loads(notify.payload)
                        handle(payload.get('event'), payload.get('message'))
                    except Exception as ex:
                        logging.error(ex)
        
                conn.notifies.clear()
                time.sleep(1)
                    
        finally:
            logging.info("database connection closed")
            cursor.close()
            conn.close()
        
    return decorator
