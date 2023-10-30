import mysql.connector
import threading
import time
import random

# global variables
DB_NAME_1 = "syncdb_test_mi_1"
TABLE_NAME_1_1 = "test_mi_1"
TABLE_NAME_1_2 = "test_mi_2"
DB_NAME_2 = "syncdb_test_mi_2"
TABLE_NAME_2_1 = "test_mi_1"
TABLE_NAME_2_2 = "test_mi_2"
SNAPSHOT_SIZE = 10000
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
FOXLAKE_STORAGE_NAME = "storage_sync"
FOXLAKE_STORAGE_URI = f'''minio://foxlakebucket/{FOXLAKE_STORAGE_NAME}'''
FOXLAKE_STORAGE_ENDPOINT = "127.0.0.1:9000"
FOXLAKE_STORAGE_ID = "ROOTUSER"
FOXLAKE_STORAGE_KEY = "CHANGEME123"
FOXLAKE_STORAGE_CREDENTIALS = f'''(ACCESS_KEY_ID='{FOXLAKE_STORAGE_ID}' SECRET_ACCESS_KEY='{FOXLAKE_STORAGE_KEY}')'''
FOXLAKE_DATASOURCE_1 = f'mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME_1}'
FOXLAKE_DATASOURCE_2 = f'mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME_2}'
FOXLAKE_ENGINE = f'''columnar@{FOXLAKE_STORAGE_NAME}'''

start_time = time.time()
conn_lock = threading.Lock()
select_query = ""

"""
    Wait for synchronization completed.
    `timewait` should better be a little bigger than `flushInterval` in foxdt.
"""
def wait_sync(foxlake_cursor, dbName, interval: float=1, timewait=15, timeout=300):
    begin = time.time()
    prev_applied_id = 0
    while time.time() - begin < timeout:
        foxlake_cursor.execute(f'SHOW SYNCHRONIZED STATUS FROM {dbName}')
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
def do_dml(dml_num, dbName, tableName, tid):
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    conn_lock.release()
    print(f'[{format_cur_time()}]: [do_dml_thread-{tid}] - DML thread start with dml_num = ' + str(dml_num) + "db = " + dbName + ", table = " + tableName)
    mysql_cursor = mysql_conn.cursor()
    counter = [0] * 4
    for i in range(1, dml_num+1):
        while True:
            # generate a random number between 1 and 4
            pk = random.randint(1, SNAPSHOT_SIZE)
            num = random.randint(1, 10000)
            choice = random.randint(1, 4)
            if choice == 1: # insert
                mysql_cursor.execute(f'''INSERT IGNORE INTO {dbName}.{tableName} VALUES({i}, "str", {num}, "name_insert")''')
            elif choice == 2: # delete
                mysql_cursor.execute(f'''DELETE IGNORE FROM {dbName}.{tableName} WHERE pk = {pk}''')
            elif choice == 3: # update
                mysql_cursor.execute(f'''UPDATE IGNORE {dbName}.{tableName} SET num = {num}, name = "name_update" WHERE pk = {pk}''')
            elif choice == 4: # update pk
                newPk = random.randint(1, SNAPSHOT_SIZE)
                mysql_cursor.execute(f'''UPDATE IGNORE {dbName}.{tableName} SET pk = {newPk}, name = "name_update_pk" WHERE pk = {pk}''')
            if mysql_cursor.rowcount > 0:
                counter[choice-1] += 1
                break
    conn_lock.acquire()
    mysql_conn.close()
    conn_lock.release()
    print(f'[{format_cur_time()}]: [do_dml_thread-{tid}] - DML thread finish, insert({counter[0]}), delete({counter[1]}), update({counter[2]}), update_pk({counter[3]})')

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
        # raise Exception(f'[{format_cur_time()}]: Validation failure: incorrect results when testing ' + desc + ". MySQL results:", mysql_results, ", FoxLake results:", foxlake_results)
    print("===============================================\n")

"""
    Check the synchronization progress every `interval` seconds
"""
def check_sync(dbName):
    interval: float=2
    conn_lock.acquire()
    foxlake_check_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    foxlake_check_cursor = foxlake_check_conn.cursor()
    prev_applied_id = -1
    prev_target_id = -1
    while True:
        try:
            foxlake_check_cursor.execute(f'SHOW SYNCHRONIZED STATUS FROM {dbName}')
        except:
            print(f'[{format_cur_time()}]: Synchronization not established yet for ' + dbName)
            time.sleep(interval)
            continue
        res = next(foxlake_check_cursor)
        if int(res[1]) != prev_applied_id or int(res[2]) != prev_target_id:
            print(f'[{format_cur_time()}]: [check_sync_thread] - Sync Progress < applied / target > : < ' + str(res[1]) + ' / ' + str(res[2]) + ' >' + " for " + dbName)
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

    # ddl for db1
    print(f'''[{format_cur_time()}]: Drop database '{DB_NAME_1}' in mysql''')
    mysql_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME_1}')
    print(f'''[{format_cur_time()}]: Create database '{DB_NAME_1}' in mysql''')
    mysql_cursor.execute(f'CREATE DATABASE {DB_NAME_1}')
    mysql_cursor.execute(f'USE {DB_NAME_1}')
    print(f'''[{format_cur_time()}]: Create table '{DB_NAME_1}.{TABLE_NAME_1_1}' in mysql''')
    mysql_cursor.execute(f'''
        CREATE TABLE {TABLE_NAME_1_1} (
            pk INT,
            str VARCHAR(64) NOT NULL,
            num FLOAT NOT NULL,
            name TEXT NOT NULL,
            PRIMARY KEY(pk)
    ) ''')
    print(f'''[{format_cur_time()}]: Create table '{DB_NAME_1}.{TABLE_NAME_1_2}' in mysql''')
    mysql_cursor.execute(f'''
        CREATE TABLE {TABLE_NAME_1_2} (
            pk INT,
            str VARCHAR(64) NOT NULL,
            num FLOAT NOT NULL,
            name TEXT NOT NULL,
            PRIMARY KEY(pk)
    ) ''')

    # ddl for db2
    print(f'''[{format_cur_time()}]: Drop database '{DB_NAME_2}' in mysql''')
    mysql_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME_2}')
    print(f'''[{format_cur_time()}]: Create database '{DB_NAME_2}' in mysql''')
    mysql_cursor.execute(f'CREATE DATABASE {DB_NAME_2}')
    mysql_cursor.execute(f'USE {DB_NAME_2}')
    print(f'''[{format_cur_time()}]: Create table '{DB_NAME_2}.{TABLE_NAME_2_1}' in mysql''')
    mysql_cursor.execute(f'''
        CREATE TABLE {TABLE_NAME_2_1} (
            pk INT,
            str VARCHAR(64) NOT NULL,
            num FLOAT NOT NULL,
            name TEXT NOT NULL,
            PRIMARY KEY(pk)
    ) ''')
    mysql_cursor.execute(f'''
        CREATE TABLE {TABLE_NAME_2_2} (
            pk INT,
            str VARCHAR(64) NOT NULL,
            num FLOAT NOT NULL,
            name TEXT NOT NULL,
            PRIMARY KEY(pk)
    ) ''')

    # snapshots
    print(f'[{format_cur_time()}]: Create snapshot in mysql')
    for i in range(1, SNAPSHOT_SIZE+1):
        num = random.randint(1, 10000)
        mysql_cursor.execute(f'''INSERT INTO {DB_NAME_1}.{TABLE_NAME_1_1} VALUES({i}, "str", {num}, "name")''')
        num = random.randint(1, 10000)
        mysql_cursor.execute(f'''INSERT INTO {DB_NAME_1}.{TABLE_NAME_1_2} VALUES({i}, "str", {num}, "name")''')
        num = random.randint(1, 10000)
        mysql_cursor.execute(f'''INSERT INTO {DB_NAME_2}.{TABLE_NAME_2_1} VALUES({i}, "str", {num}, "name")''')
        num = random.randint(1, 10000)
        mysql_cursor.execute(f'''INSERT INTO {DB_NAME_2}.{TABLE_NAME_2_2} VALUES({i}, "str", {num}, "name")''')
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

    print(f'''[{format_cur_time()}]: Drop database '{DB_NAME_1}' in foxlake''')
    foxlake_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME_1}')
    print(f'''[{format_cur_time()}]: Drop database '{DB_NAME_2}' in foxlake''')
    foxlake_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME_2}')

    print(f'''[{format_cur_time()}]: Create synchronized database '{DB_NAME_1}' in foxlake''')
    foxlake_cursor.execute(f'''
        CREATE SYNCHRONIZED DATABASE {DB_NAME_1}
        DATASOURCE = '{FOXLAKE_DATASOURCE_1}'
        ENGINE = '{FOXLAKE_ENGINE}';
    ''')

    print(f'''[{format_cur_time()}]: Create synchronized database '{DB_NAME_2}' in foxlake''')
    foxlake_cursor.execute(f'''
        CREATE SYNCHRONIZED DATABASE {DB_NAME_2}
        DATASOURCE = '{FOXLAKE_DATASOURCE_2}'
        ENGINE = '{FOXLAKE_ENGINE}';
    ''')

    check_thread_1 = threading.Thread(target=check_sync, args=(DB_NAME_1,))
    check_thread_1.daemon = True
    check_thread_1.start()

    check_thread_2 = threading.Thread(target=check_sync, args=(DB_NAME_2,
                                                               ))
    check_thread_2.daemon = True
    check_thread_2.start()

    print(f'''[{format_cur_time()}]: Wait for synchronization of snapshot...''')
    wait_sync(foxlake_cursor, DB_NAME_1)
    wait_sync(foxlake_cursor, DB_NAME_2)
    print(f'''[{format_cur_time()}]: Wait for synchronization of snapshot done''')

    # validate snapshot
    select_query = "SELECT * FROM " + DB_NAME_1 + "." + TABLE_NAME_1_1 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "snapshot")
    select_query = "SELECT * FROM " + DB_NAME_1 + "." + TABLE_NAME_1_2 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "snapshot")
    select_query = "SELECT * FROM " + DB_NAME_2 + "." + TABLE_NAME_2_1 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "snapshot")
    select_query = "SELECT * FROM " + DB_NAME_2 + "." + TABLE_NAME_2_2 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "snapshot")

    # validate dml
    print(f'''[{format_cur_time()}]: Start DML threads''')
    dml_threads = []
    tid = 0
    for i in range(DML_THREAD_NUM):
        dml_threads.append(threading.Thread(target=do_dml, args=(DML_NUM_PER_THREAD, DB_NAME_1, TABLE_NAME_1_1, tid)))
        dml_threads[-1].daemon = True
        dml_threads[-1].start()
        tid += 1

        dml_threads.append(threading.Thread(target=do_dml, args=(DML_NUM_PER_THREAD, DB_NAME_1, TABLE_NAME_1_2, tid)))
        dml_threads[-1].daemon = True
        dml_threads[-1].start()
        tid += 1

        dml_threads.append(threading.Thread(target=do_dml, args=(DML_NUM_PER_THREAD, DB_NAME_2, TABLE_NAME_2_1, tid)))
        dml_threads[-1].daemon = True
        dml_threads[-1].start()
        tid += 1

        dml_threads.append(threading.Thread(target=do_dml, args=(DML_NUM_PER_THREAD, DB_NAME_2, TABLE_NAME_2_2, tid)))
        dml_threads[-1].daemon = True
        dml_threads[-1].start()
        tid += 1
    for i in range(len(dml_threads)):
        dml_threads[i].join()

    print(f'''[{format_cur_time()}]: Wait for synchronization of delta...''')
    wait_sync(foxlake_cursor, DB_NAME_1)
    wait_sync(foxlake_cursor, DB_NAME_2)
    print(f'''[{format_cur_time()}]: Wait for synchronization of delta done''')

    select_query = "SELECT * FROM " + DB_NAME_1 + "." + TABLE_NAME_1_1 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "dml")
    select_query = "SELECT * FROM " + DB_NAME_1 + "." + TABLE_NAME_1_2 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "dml")
    select_query = "SELECT * FROM " + DB_NAME_2 + "." + TABLE_NAME_2_1 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "dml")
    select_query = "SELECT * FROM " + DB_NAME_2 + "." + TABLE_NAME_2_2 + " ORDER BY pk"
    validate_results(mysql_cursor, foxlake_cursor, select_query, "dml")

    conn_lock.acquire()
    mysql_conn.close()
    foxlake_conn.close()
    conn_lock.release()

    print(f'[{format_cur_time()}]: Passed Test!')

# main
test_sync()