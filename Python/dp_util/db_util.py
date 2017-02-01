import os
import pymysql.cursors
import logging
import dp_util.log_util as log_util
import warnings


# TODO: use warnings module

def init_logging():
    log_connection = make_test_connection()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('main_logger')
    logger.setLevel(logging.INFO)
    db_handler = log_util.DBLogger(log_connection)
    db_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    db_handler.setFormatter(formatter)
    logger.addHandler(db_handler)
    logging.getLogger('py.warnings').addHandler(db_handler)
    logging.captureWarnings(True)


def make_test_connection():
    user = os.environ['DATAPULL_USER']
    pwd = os.environ['DATAPULL_PASS']
    hostname = 'irs.cqd2sjht0tbl.us-west-2.rds.amazonaws.com'
    cnx = pymysql.connect(user=user, password=pwd, host=hostname, db='irs', cursorclass=pymysql.cursors.DictCursor)
    return cnx


def force_warning(connection):
    warning_cursor = connection.cursor()
    warning_query = "SIGNAL SQLSTATE '01234' SET MESSAGE_TEXT = 'Test warning'";
    warning_cursor.execute(warning_query)


def insert_into_table(table_name, list_of_dict, connection, cursor_in = None):
    insert_cursor = cursor_in if cursor_in is not None else connection.cursor()
    for row in list_of_dict:
        key_string = ""
        val_string = ""
        vals = []

        #populate
        for key, elem in row.items():
            key_string += "`"+key + "`, "
            val_string += "%s, "
            vals.append(elem)

        insert_query = "insert into irs.`"+table_name+"` (" + key_string[:-2] + ") VALUES (" + val_string[:-2] + ")"
        insert_cursor.execute(insert_query, vals)


def get_columns(table_name, connection):
    desc_cursor = connection.cursor()
    desc_query = "Desc irs." + table_name
    desc_cursor.execute(desc_query)
    desc_rows = desc_cursor.fetchall()
    field_list = []
    for row in desc_rows:
        if row['Extra'] != 'auto_increment':
            field_list.append(row['Field'])
    return field_list
