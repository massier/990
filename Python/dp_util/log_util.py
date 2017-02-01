import os
import pymysql.cursors
import dp_util.gen_util as gen_util
import warnings
import logging


class DBLogger(logging.Handler):
    def __init__(self, connection):
        logging.Handler.__init__(self)
        self.db_connection = connection

    def emit(self, record):
        self.formatter.format(record)
        timestamp = gen_util.make_datestamp()
        log_insert_cursor = self.db_connection.cursor()
        log_insert_query = "INSERT INTO irs.log (date, type, note) VALUES (%s, %s, %s)"
        try:
            log_insert_cursor.execute(log_insert_query, (timestamp, record.levelname, record.message,))
        except Exception as err:
            print("Error in logging: %s", (err,))
            self.db_connection.rollback()
        else:
            self.db_connection.commit()
        log_insert_cursor.close()

