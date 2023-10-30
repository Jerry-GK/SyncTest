import mysql.connector
import threading
import time
import random

"""
    Unsupported types:
    BIT(N): FoxLake has some problems reading them 
    FLOAT(M, D): In kafka it's INT64, but in foxlake it is INT32, the same as ordinary FLOAT
    TIMESTAMP: issues related to timezone
    SPARTIAL TYPES: Foxlake does not support them.

    (BLOB, Set and JSON type is not tested for returning types not matched)
"""

# global variables
DB_NAME = "syncdb_test_type_1"
TABLE_NAME = "TEST_TYPE"
SNAPSHOT_SIZE = 100
DML_THREAD_NUM = 5
DML_NUM_PER_THREAD = SNAPSHOT_SIZE // 10

MYSQL_USER = "root"
MYSQL_PASSWORD = "Jk37373737"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306

FOXLAKE_USER = "foxlake_root"
FOXLAKE_PASSWORD = "foxlake2023"
FOXLAKE_HOST = "127.0.0.1"
FOXLAKE_PORT = 11288
FOXLAKE_STORAGE_NAME = "storage_sync_type_1"
FOXLAKE_STORAGE_URI = f'''minio://foxlakebucket/{FOXLAKE_STORAGE_NAME}'''
FOXLAKE_STORAGE_ENDPOINT = "127.0.0.1:9000"
FOXLAKE_STORAGE_ID = "ROOTUSER"
FOXLAKE_STORAGE_KEY = "CHANGEME123"
FOXLAKE_STORAGE_CREDENTIALS = f'''(ACCESS_KEY_ID='{FOXLAKE_STORAGE_ID}' SECRET_ACCESS_KEY='{FOXLAKE_STORAGE_KEY}')'''
FOXLAKE_DATASOURCE = f'mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME}'
FOXLAKE_ENGINE = f'''columnar@{FOXLAKE_STORAGE_NAME}'''

start_time = time.time()
conn_lock = threading.Lock()
select_query = "SELECT * FROM " + DB_NAME + "." + TABLE_NAME + " ORDER BY pk"

"""
    Wait for synchronization completed.
    `timewait` should better be a little bigger than `flushInterval` in foxdt.
"""
def wait_sync(foxlake_cursor, interval: float=1, timewait=20, timeout=300):
    begin = time.time()
    prev_applied_id = 0
    while time.time() - begin < timeout:
        foxlake_cursor.execute(f'SHOW SYNCHRONIZED STATUS FROM {DB_NAME}')
        res = next(foxlake_cursor)
        if res[3] == "UP_TO_DATE" and int(res[2]) != 0 and (time.time() - begin) > timewait:
            begin = time.time()
            print(f'[{format_cur_time()}]: Wait for sync success: APPLIED_SEQUENCE_ID = ' + str(res[1]) + ", TARGET_SEQUENCE_ID = " + str(res[2]) + ", STATUS = " + str(res[3]))
            return
        if int(res[1]) != prev_applied_id:
            prev_applied_id = int(res[1])
            begin = time.time()
        time.sleep(interval)
    raise Exception(f'[{format_cur_time()}]: Wait for sync timeout after ' + str(timeout) + ": APPLIED_SEQUENCE_ID = " + str(res[1]) + ", TARGET_SEQUENCE_ID = " + str(res[2]) + ", STATUS = " + str(res[3]))

"""
    Do DML operations for at most `dml_num` times.
"""
def do_dml(dml_num, tid):
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    conn_lock.release()
    print(f'[{format_cur_time()}]: [do_dml_thread-{tid}] - DML thread start with dml_num = ' + str(dml_num))
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f'use {DB_NAME}')
    for i in range(1, dml_num+1):
        while True:
            PK = random.randint(-(10 * SNAPSHOT_SIZE), -1)
            mysql_cursor.execute(f'''INSERT IGNORE INTO {TABLE_NAME} VALUES(
                    {PK}, 
                    FALSE,
                    -128,
                    -32768, 
                    -8388608, 
                    -2147483648, 
                    -9223372036854775808,
                    -123.45, 
                    -3.14159,
                    -1.61803,
                    -1.72341,
                    'Hello', 
                    'World', 
                    'VarBinary', 
                    'TinyText', 
                    'TextData', 
                    'MediumTextData', 
                    'LongTextData', 
                    'A', 
                    2023, 
                    '2023-08-18', 
                    '12:34:56', 
                    '2023-08-18 12:34:56',
                    CURRENT_TIMESTAMP
                )''')
            if mysql_cursor.rowcount > 0:
                break
    conn_lock.acquire()
    mysql_conn.close()
    conn_lock.release()
    print(f'[{format_cur_time()}]: [do_dml_thread-{tid}] - DML thread finish')

"""
    Validate the results of the query in mysql and foxlake.
"""
def validate_results(mysql_cursor, foxlake_cursor, query, desc):
    print("\n=================Validate======================")
    print(f'[{format_cur_time()}]: Validating for ' + desc + "...")
    print(f'[{format_cur_time()}]: Validation query: ' + query)
    mysql_cursor.execute(query)
    mysql_results = mysql_cursor.fetchall()
    foxlake_cursor.execute(query)
    foxlake_results = foxlake_cursor.fetchall()
    if mysql_results == foxlake_results:
        print(f'[{format_cur_time()}]: Validation success: correct results when testing ' + desc)
    else:
        raise Exception(f'[{format_cur_time()}]: Validation failure: incorrect results when testing ' + desc)
        # raise Exception(f'[{format_cur_time()}]: Validation failure: incorrect results when testing ' + desc + ". MySQL results:", mysql_results[0], ", FoxLake results: ", foxlake_results[0])
    print("===============================================\n")

"""
    Check the synchronization progress every `interval` seconds
"""
def check_sync(interval: float=2):
    conn_lock.acquire()
    foxlake_check_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    foxlake_check_cursor = foxlake_check_conn.cursor()
    prev_applied_id = -1
    prev_target_id = -1
    while True:
        try:
            foxlake_check_cursor.execute(f'SHOW SYNCHRONIZED STATUS FROM {DB_NAME}')
        except:
            print(f'[{format_cur_time()}]: Synchronization not established yet')
            time.sleep(interval)
            continue
        res = next(foxlake_check_cursor)
        if int(res[1]) != prev_applied_id or int(res[2]) != prev_target_id:
            print(f'[{format_cur_time()}]: [check_sync_thread] - Sync Progress < applied / target > : < ' + str(res[1]) + ' / ' + str(res[2]) + ' >')
            prev_applied_id = int(res[1])
            prev_target_id = int(res[2])
        time.sleep(interval)

def format_cur_time():
    global start_time
    seconds = time.time() - start_time
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours):02d}.{int(minutes):02d}.{int(seconds):02d}"

def test_sync():
    global select_query

    print(f'[{format_cur_time()}]: Start Test!')
    print(f'[{format_cur_time()}]: Connect to mysql')
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    conn_lock.release()
    mysql_cursor = mysql_conn.cursor()
    print(f'''[{format_cur_time()}]: Drop database '{DB_NAME}' in mysql''')
    mysql_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME}')
    print(f'''[{format_cur_time()}]: Create database '{DB_NAME}' in mysql''')
    mysql_cursor.execute(f'CREATE DATABASE {DB_NAME}')
    mysql_cursor.execute(f'USE {DB_NAME}')
    print(f'''[{format_cur_time()}]: Create table '{DB_NAME}.{TABLE_NAME}' in mysql''')
    
    mysql_cursor.execute(f'''
        CREATE TABLE {TABLE_NAME} (
            PK INT,
            COL_BOOL BOOL,
            COL_TINYINT TINYINT,
            COL_SMALLINT SMALLINT,
            COL_MEDIUMINT MEDIUMINT,
            COL_INTEGER INTEGER,
            COL_BIGINT BIGINT,
            COL_REAL REAL,
            COL_FLOAT FLOAT,
            COL_DOUBLE DOUBLE,
            COL_DECIMAL DECIMAL(10,5),
            COL_CHAR CHAR(10),
            COL_VARCHAR VARCHAR(10),
            COL_VARBINARY VARBINARY(10),
            COL_TINYTEXT TINYTEXT,
            COL_TEXT TEXT,
            COL_MEDIUMTEXT MEDIUMTEXT,
            COL_LONGTEXT LONGTEXT,
            COL_ENUM ENUM('A', 'B', 'C'),
            COL_YEAR YEAR,
            COL_DATE DATE,
            COL_TIME TIME,
            COL_DATETIME DATETIME,
            COL_TIMESTAMP TIMESTAMP,
            PRIMARY KEY(PK)
    ) ''')

    print(f'[{format_cur_time()}]: Create snapshot in mysql')
    for i in range(1, SNAPSHOT_SIZE+1):
        jsonStr = '{"key":"value"}'
        mysql_cursor.execute(f'''INSERT INTO {TABLE_NAME} VALUES(
                                {i}, 
                                TRUE,
                                127,
                                32767, 
                                8388607, 
                                2147483647, 
                                9223372036854775807, 
                                123.45, 
                                3.14159, 
                                1.61803, 
                                1.72341,
                                'Hello', 
                                'World', 
                                'VarBinary', 
                                'TinyText', 
                                'TextData', 
                                'MediumTextData', 
                                'LongTextData', 
                                'A', 
                                2023, 
                                '2023-08-18', 
                                '12:34:56', 
                                '2023-08-18 12:34:56',
                                CURRENT_TIMESTAMP
                            )''')
    print(f'[{format_cur_time()}]: Finish mysql snapshot')

    print(f'[{format_cur_time()}]: Connect to foxlake')
    conn_lock.acquire()
    foxlake_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    foxlake_cursor = foxlake_conn.cursor()

    print(f'[{format_cur_time()}]: Create storage in foxlake')
    foxlake_cursor.execute(f'''
        CREATE OR REPLACE STORAGE {FOXLAKE_STORAGE_NAME}
        AT URI '{FOXLAKE_STORAGE_URI}'
        ENDPOINT = '{FOXLAKE_STORAGE_ENDPOINT}'
        CREDENTIALS {FOXLAKE_STORAGE_CREDENTIALS};
    ''')

    print(f'''[{format_cur_time()}]: Drop database '{DB_NAME}' in foxlake''')
    foxlake_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME}')

    print(f'''[{format_cur_time()}]: Create synchronized database '{DB_NAME}' in foxlake''')
    foxlake_cursor.execute(f'''
        CREATE SYNCHRONIZED DATABASE {DB_NAME}
        DATASOURCE = '{FOXLAKE_DATASOURCE}'
        ENGINE = '{FOXLAKE_ENGINE}';
    ''')

    foxlake_cursor.execute(f'use {DB_NAME}')

    check_thread = threading.Thread(target=check_sync)
    check_thread.daemon = True
    check_thread.start()

    print(f'''[{format_cur_time()}]: Wait for synchronization of snapshot...''')
    wait_sync(foxlake_cursor)
    print(f'''[{format_cur_time()}]: Wait for synchronization of snapshot done''')

    # validate snapshot
    validate_results(mysql_cursor, foxlake_cursor, select_query, "snapshot")

    # validate dml
    print(f'''[{format_cur_time()}]: Start DML threads''')
    dml_threads = []
    for i in range(DML_THREAD_NUM):
        dml_threads.append(threading.Thread(target=do_dml, args=(DML_NUM_PER_THREAD, i+1)))
        dml_threads[i].daemon = True
        dml_threads[i].start()
    for i in range(DML_THREAD_NUM):
        dml_threads[i].join()

    print(f'''[{format_cur_time()}]: Wait for synchronization of delta...''')
    wait_sync(foxlake_cursor)
    print(f'''[{format_cur_time()}]: Wait for synchronization of delta done''')

    validate_results(mysql_cursor, foxlake_cursor, select_query, "dml")

    conn_lock.acquire()
    mysql_conn.close()
    foxlake_conn.close()
    conn_lock.release()

    print(f'[{format_cur_time()}]: Passed Test!')

# main
test_sync()