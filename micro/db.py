# -*- coding: utf-8 -*

import psycopg2.extras
import psycopg2
import logging
import asyncio
import os

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "database")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

##
#

class Database:

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
        if hasattr(self, "conn"):
            self.conn.close()
        logging.debug("database connection closed")
    
    def cursor(self):
        self.cur = self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        return self.cur
    
    def execute(self, query, params=None):
        logging.debug("query: %s", query)
        self.cur.execute(query, params)
        return self.cur

    def listen(self, channel, handle):

        logging.debug("listen on %s", channel)
        
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur.execute(f"LISTEN {channel};")
        self.conn.poll()
        
        for notify in self.conn.notifies:
            try:
                logging.debug("notify: %s", notify)
                payload = notify.payload
                handle(payload['event'], payload['message'])
            except Exception as ex:
                logging.error(ex)

        self.conn.notifies.clear()

##
#

def connect(handle):
    def decorator(*args, **kwargs):
        try:
            logging.debug("connecting")
            with db.connect():
                with db.cursor():
                    handle(db, *args, **kwargs)
        finally:
            db.close()

    return decorator

##
#

def query(handle):
    def decorator(sql, params=None):
        try:
            logging.debug("connecting")
            with db.connect():
                with db.cursor():
                    res = db.execute(sql, params)
                    handle(res)
        finally:
            db.close()

    return decorator

##
#

def listen(channel):
    def decorator(handle):

        try:
            db = Database()

            def wrapper():
                db.listen(channel, handle)
            
            with db.connect() as conn:
                with db.cursor():
                    loop = asyncio.get_event_loop()
                    loop.add_reader(conn, wrapper)
                    loop.run_forever()
        finally:
            db.close()
        
    return decorator
