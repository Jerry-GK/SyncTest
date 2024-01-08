import mysql.connector
import threading
import time
import random
import matplotlib.pyplot as plt
import csv

# global variables
DB_NAME = "test_last"
TABLE_NAME = "test_delta"
SNAPSHOT_SIZE = 20 * 10000

MYSQL_USER = "root"
MYSQL_PASSWORD = "Jk37373737"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306

FOXLAKE_USER = "foxlake_root"
FOXLAKE_PASSWORD = "foxlake2023"
FOXLAKE_HOST = "127.0.0.1"
FOXLAKE_PORT = 11288
FOXLAKE_STORAGE_NAME = "storage_sync_lag_validate"
# FOXLAKE_STORAGE_URI = f'''s3://foxlake/test_sync/{FOXLAKE_STORAGE_NAME}'''
FOXLAKE_STORAGE_URI = f'''minio://foxlakebucket/{FOXLAKE_STORAGE_NAME}'''
# FOXLAKE_STORAGE_ENDPOINT = "s3.cn-northwest-1.amazonaws.com.cn"
FOXLAKE_STORAGE_ENDPOINT = "127.0.0.1:9000"
# FOXLAKE_STORAGE_ID = "AKIAWSVSB2URE6ZU6R5Q"
FOXLAKE_STORAGE_ID = "ROOTUSER"
# FOXLAKE_STORAGE_KEY = "VjCReaHxWO6xtHmMF9P53xkIsddpwYp0wIzS9ArA"
FOXLAKE_STORAGE_KEY = "CHANGEME123"
FOXLAKE_STORAGE_CREDENTIALS = f'''(ACCESS_KEY_ID='{FOXLAKE_STORAGE_ID}' SECRET_ACCESS_KEY='{FOXLAKE_STORAGE_KEY}')'''
FOXLAKE_DATASOURCE = f'mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME}'
FOXLAKE_ENGINE = f'''columnar@{FOXLAKE_STORAGE_NAME}'''

start_time = time.time()
conn_lock = threading.Lock()

def format_cur_time():
    global start_time
    seconds = time.time() - start_time
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours):02d}.{int(minutes):02d}.{int(seconds):02d}"

def test_insert():
    print(f'[{format_cur_time()}]: Connect to foxlake')
    conn_lock.acquire()
    foxlake_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    foxlake_cursor = foxlake_conn.cursor()

    # print(f'''[{format_cur_time()}]: Drop database '{DB_NAME}' in foxlake''')
    # foxlake_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME}')
    # print(f'''[{format_cur_time()}]: Create database '{DB_NAME}' in foxlake''')
    # foxlake_cursor.execute(f'Create DATABASE IF NOT EXISTS {DB_NAME}')
    foxlake_cursor.execute(f'Use {DB_NAME}')
    print(f'''[{format_cur_time()}]: Create table '{TABLE_NAME}' in foxlake''')
    foxlake_cursor.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (id BIGINT PRIMARY KEY, name VARCHAR(255), price FLOAT, create_time TIMESTAMP)")
    
    sql = doInsertSql()
    print(f'''[{format_cur_time()}]: Insert {SNAPSHOT_SIZE} rows...''')
    # time.sleep(5)
    start = time.time()
    foxlake_cursor.execute(sql)
    end = time.time()
    print(f'''Insert {SNAPSHOT_SIZE} rows, duration: {end - start}s''')

    conn_lock.acquire()
    foxlake_conn.close()
    conn_lock.release()

    print(f'[{format_cur_time()}]: Passed Test!')

def doInsertSql():
    sql = "INSERT INTO `" + DB_NAME + "`.`" + TABLE_NAME + "` (id, name, price, create_time) VALUES "
    id = 0
    for i in range(0, SNAPSHOT_SIZE):
        id = id + 1
        if i > 0:
            sql += ", " 
        sql += f'''({id}, 'name', {i / 5}, '2023-08-18 12:34:56')'''
    return sql


# main
test_insert()


