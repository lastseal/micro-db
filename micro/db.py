# -*- coding: utf-8 -*

import psycopg2.extras
import psycopg2
import logging
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

    def listen(self, channel)
        logging.debug("listen on %s", channel)
        self.cur.execute(f"LISTEN {channel};")

        def wrapper():
            self.conn.poll()
            for notify in self.conn.notifies:
                try:
                    logging.debug("notify: %s", notify)
                    handle(notify.payload)
                except Exception as ex:
                    logging.error(ex)

            self.conn.notifies.clear()

##
#
    
db = Database()

def connect(handle):
    def decorator(*args, **kwargs):
        try:
            logging.debug("connecting")
            with db.connect():
                with db.cursor():
                    handle(*args, **kwargs)
        finally:
            db.close()

    return decorator

##
#

def query(sql, params=None):
    return db.execute(sql, params)
