import mysql.connector
import threading
import time
import random
import multiprocessing
import matplotlib.pyplot as plt
import csv

# global variables

DB_NAME = "syncdb_test_1w"
TABLE_NAME = "test"
SNAPSHOT_SIZE = 1 * 10000
DML_THREAD_NUM = 5
DML_NUM_PER_THREAD = 10000000000
DML_TOTAL = 0
DO_SNAPSHOT_IN_MYSQL = True
DO_SNAPSHOT_IN_FOXLAKE = True
DO_DML = False
DO_VALIDATE = False
WRITE_CSV = False

MYSQL_USER = "root"
MYSQL_PASSWORD = "Jk37373737"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306

FOXLAKE_USER = "foxlake_root"
FOXLAKE_PASSWORD = "foxlake2023"
FOXLAKE_HOST = "127.0.0.1"
FOXLAKE_PORT = 11288
FOXLAKE_STORAGE_NAME = "storage_sync_lag_validate_113"
# FOXLAKE_STORAGE_URI = f'''s3://foxlake/test_sync/{FOXLAKE_STORAGE_NAME}'''
FOXLAKE_STORAGE_URI = f'''s3c://foxlakebucket/{FOXLAKE_STORAGE_NAME}'''
# FOXLAKE_STORAGE_ENDPOINT = "s3.cn-northwest-1.amazonaws.com.cn"
FOXLAKE_STORAGE_ENDPOINT = "127.0.0.1:9000"
# FOXLAKE_STORAGE_ID = "AKIAWSVSB2URE6ZU6R5Q"
FOXLAKE_STORAGE_ID = "ROOTUSER"
# FOXLAKE_STORAGE_KEY = "VjCReaHxWO6xtHmMF9P53xkIsddpwYp0wIzS9ArA"
FOXLAKE_STORAGE_KEY = "CHANGEME123"
FOXLAKE_STORAGE_CREDENTIALS = f'''(ACCESS_KEY_ID='{FOXLAKE_STORAGE_ID}' SECRET_ACCESS_KEY='{FOXLAKE_STORAGE_KEY}')'''
FOXLAKE_DATASOURCE = f'''mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME}_mysql'''
FOXLAKE_ENGINE = f'''columnar@{FOXLAKE_STORAGE_NAME}'''

start_time = time.time()
conn_lock = threading.Lock()
# select_time_query = "SELECT time FROM " + DB_NAME + "." + TABLE_NAME + " ORDER BY time DESC LIMIT 1"
select_time_query = "SELECT MAX(COL_CUR_TIME) FROM " + DB_NAME + "." + TABLE_NAME
select_bytes_query = "SHOW GLOBAL STATUS LIKE 'Bytes_received'"
select_validate_query = "SELECT * FROM " + DB_NAME + "." + TABLE_NAME + " ORDER BY pk"
select_all_query = "SELECT COUNT(*) FROM " + DB_NAME + "." + TABLE_NAME + " ORDER BY pk"

update_lock = threading.Lock()
dml_lock = threading.Lock()

MAX_LAG_WAIT = 3
VALIDATE_INTERVAL = 70
EXIT = False

short_decimal_value = 3.14159
long_decimal_value = 1234567812345678123456781234567812345678.12345678

lags = []
qpss = []
times = []
scan_time = []

def format_cur_time():
    global start_time
    seconds = time.time() - start_time
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours):02d}.{int(minutes):02d}.{int(seconds):02d}"

def get_cur_time():
    return time.time()

"""
    Wait for synchronization completed.
    `timewait` should better be a little bigger than `flushInterval` in foxdt.
"""
def wait_sync(foxlake_cursor, interval: float=1, timewait=100, timeout=300):
    global EXIT
    begin = time.time()
    prev_applied_id = 0
    while time.time() - begin < timeout:
        foxlake_cursor.execute(f'SHOW SYNCHRONIZED STATUS FROM {DB_NAME}')
        res = next(foxlake_cursor)
        if res[3] == "UP_TO_DATE" and int(res[2]) != 0 and (time.time() - begin) > timewait:
            begin = time.time()
            print(f'[{format_cur_time()}]: Wait for sync success: APPLIED_SEQUENCE_ID = ' + str(res[1]) + ", TARGET_SEQUENCE_ID = " + str(res[2]) + ", STATUS = " + str(res[3]))
            if prev_applied_id > 200:
                EXIT = True
            return
        if int(res[1]) != prev_applied_id:
            prev_applied_id = int(res[1])
            begin = time.time()
        time.sleep(interval)
    raise Exception(f'[{format_cur_time()}]: Wait for sync timeout after ' + str(timeout) + ": APPLIED_SEQUENCE_ID = " + str(res[1]) + ", TARGET_SEQUENCE_ID = " + str(res[2]) + ", STATUS = " + str(res[3]))

def check_lag(mysql_cursor, foxlake_cursor, dml_start_time, dml_start_bytes):
    check_interval = 1
    mysql_cursor.execute(f'use {DB_NAME}')
    foxlake_cursor.execute(f'use {DB_NAME}')
    first = True
    current = 0

    while True and not EXIT:
        update_lock.acquire()
        # start = time.time()
        # mysql_cursor.execute(select_time_query)
        # mysql_res = next(mysql_cursor)
        # mysql_query_time = time.time() - start
        if DO_DML:
            current = time.time()
        foxlake_cursor.execute(select_time_query)
        foxlake_res = next(foxlake_cursor)
        qstart = time.time()    
        foxlake_cursor.execute(select_all_query)
        foxlake_query_time = time.time() - qstart
        foxlake_res_all = next(foxlake_cursor)
        update_lock.release()
        # query_time_margin = foxlake_query_time - mysql_query_time
        # print(f'[{format_cur_time()}]: start = ' + str(start) + ', foxlake_res = ' + str(foxlake_res[0]))
        if foxlake_res and foxlake_res[0]:
            if first:
                first = False
                continue
            scan_time.append(foxlake_query_time)
            print(f'''[{format_cur_time()}]: DML = {DML_TOTAL}, table scan latency = {1000 * foxlake_query_time:.2f}ms (avg = {1000 * sum(scan_time) / len(scan_time):.2f}ms)''')
            lag = current - foxlake_res[0]
            lags.append(lag)
            dml_time = time.time() - dml_start_time
            qps = (int)(DML_TOTAL / dml_time)
            qpss.append(qps)
            times.append(dml_time)
            mysql_cursor.execute(select_bytes_query)
            res = next(mysql_cursor)
            bytesReceived = int(res[1]) - dml_start_bytes
            throughput = int(((bytesReceived // 1000) / dml_time))
            print(f'[{format_cur_time()}]: [check_lag_thread] - Lag = {1000*lag:.2f}ms (avg = {1000*sum(lags) / len(lags):.2f}'
                   + "ms)\tQPS = " + qps.__str__() + "\tthroughput = " + throughput.__str__() + "KB/s")

        time.sleep(check_interval)

"""
    Thread for Validate the results of the query in mysql and foxlake.
"""
def validate():
    global EXIT
    global DO_DML
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    foxlake_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    mysql_cursor = mysql_conn.cursor(buffered=True)
    foxlake_cursor = foxlake_conn.cursor(buffered=True)
    while True:
        time.sleep(VALIDATE_INTERVAL)
        dml_lock.acquire()
        DO_DML = False
        time.sleep(MAX_LAG_WAIT)
        #wait_sync(foxlake_cursor, 1, 80, 120)
        mysql_cursor.execute(select_validate_query)
        mysql_results = mysql_cursor.fetchall()
        foxlake_cursor.execute(select_validate_query)
        foxlake_results = foxlake_cursor.fetchall()
        # find the different result
        mysql_len = len(mysql_results)
        for i in range (0, mysql_len):
            if i >= len(foxlake_results):
                EXIT = True
                raise Exception(f'[{format_cur_time()}]: Validation failure: incorrect results, MySQL results len:', len(mysql_results), ", FoxLake results len: ", len(foxlake_results))
            if mysql_results[i] != foxlake_results[i]:
                EXIT = True
                raise Exception(f'[{format_cur_time()}]: Validation failure: incorrect results, MySQL results[{i}]:', mysql_results[i], f', FoxLake results[{i}]: ', foxlake_results[i])
        print(f'[{format_cur_time()}]: ================== <Validation success: correct results> ==================')
        DO_DML = True
        dml_lock.release()

"""
    Do DML operations for at most `dml_num` times.
"""

"""
    Do DML operations for at most `dml_num` times.
"""
def do_dml(dml_num, tid):
    global DML_TOTAL
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    conn_lock.release()
    print(f'[{format_cur_time()}]: [do_dml_thread-{tid}] - DML thread start with dml_num = ' + str(dml_num))
    mysql_cursor = mysql_conn.cursor(buffered=True)
    mysql_cursor.execute(f'use {DB_NAME}_mysql')
    counter = [0] * 4
    for i in range(1, dml_num+1):
        dml_lock.acquire()
        while True:
            # generate a random number between 1 and 4
            pk = random.randint(1, SNAPSHOT_SIZE+1)
            num = random.randint(1, 10000)

            choice = random.randint(1, 4)
            if choice == 1: # insert
                sign = random.randint(0, 1)
                if sign == 0:
                    mysql_cursor.execute(f'''INSERT IGNORE INTO {TABLE_NAME} VALUES(
                                            {pk}, 
                                            FALSE,
                                            -128,
                                            -32768, 
                                            -8388608, 
                                            -2147483648, 
                                            -9223372036854775808,
                                            -3.14159,
                                            -3.14159,
                                            -1.61803,
                                            {-short_decimal_value},
                                            {-long_decimal_value},
                                            -37.73,
                                            'Hello_insert', 
                                            'World_insert', 
                                            'Varbinary_insert',
                                            'TinyText_insert', 
                                            'TextData_insert', 
                                            'MediumTextData_insert', 
                                            'LongTextData_insert', 
                                            'B', 
                                            2023, 
                                            '2023-08-18', 
                                            '12:34:56', 
                                            '2023-08-18 12:34:56',
                                            '2023-08-18 12:34:56.123456',
                                            {get_cur_time()}
                                            )''')
                elif sign == 1:
                    mysql_cursor.execute(f'''INSERT IGNORE INTO {TABLE_NAME} VALUES(
                                            {-pk}, 
                                            FALSE,
                                            -128,
                                            -32768, 
                                            -8388608, 
                                            -2147483648, 
                                            -9223372036854775808,
                                            -3.14159,
                                            -3.14159,
                                            -1.61803,
                                            {-short_decimal_value},
                                            {-long_decimal_value},
                                            -37.73,
                                            'Hello_insert', 
                                            'World_insert', 
                                            'Varbinary_insert',
                                            'TinyText_insert', 
                                            'TextData_insert', 
                                            'MediumTextData_insert', 
                                            'LongTextData_insert', 
                                            'B', 
                                            2023, 
                                            '2023-08-18', 
                                            '12:34:56', 
                                            '2023-08-18 12:34:56',
                                            '2023-08-18 12:34:56.123456',
                                            {get_cur_time()}
                                         )''')
            elif choice == 2: # delete
                mysql_cursor.execute(f'''DELETE IGNORE FROM {TABLE_NAME} WHERE pk = {pk}''')
            elif choice == 3: # update
                mysql_cursor.execute(f'''UPDATE IGNORE {TABLE_NAME} SET 
                                        COL_BOOL = False,
                                        COL_TINYINT = -128,
                                        COL_SMALLINT = -32768,
                                        COL_MEDIUMINT = -8388608,
                                        COL_INTEGER = -2147483648,
                                        COL_BIGINT = -9223372036854775808,
                                        COL_FLOAT = -3.14159,
                                        COL_LIMIT_FLOAT = -3.14159,
                                        COL_DOUBLE = -1.61803,
                                        COL_SHORT_DECIMAL = {-short_decimal_value},
                                        COL_LONG_DECIMAL = {-long_decimal_value},
                                        COL_REAL = -37.73,
                                        COL_CHAR = 'Hello_update',
                                        COL_VARCHAR = 'World_update',
                                        COL_VARBINARY = 'Varbinary_update',
                                        COL_TINYTEXT = 'TinyText_update',
                                        COL_TEXT = 'TextData_update',
                                        COL_MEDIUMTEXT = 'MediumTextData_update',
                                        COL_LONGTEXT = 'LongTextData_update',
                                        COL_ENUM = 'C',
                                        COL_YEAR = 2023,
                                        COL_DATE = '2023-08-18',
                                        COL_TIME = '12:34:56',
                                        COL_DATETIME = '2023-08-18 12:34:56',
                                        COL_DATETIME_6 = '2023-08-18 12:34:56.123456',
                                        COL_CUR_TIME = {get_cur_time()}
                                     WHERE pk = {pk}''')
            elif choice == 4: # update pk
                newPk = random.randint(1, SNAPSHOT_SIZE+1)
                mysql_cursor.execute(f'''UPDATE IGNORE {TABLE_NAME} SET 
                                        PK = {newPk},
                                        COL_BOOL = False,
                                        COL_TINYINT = -128,
                                        COL_SMALLINT = -32768,
                                        COL_MEDIUMINT = -8388608,
                                        COL_INTEGER = -2147483648,
                                        COL_BIGINT = -9223372036854775808,
                                        COL_FLOAT = -3.14159,
                                        COL_LIMIT_FLOAT = -3.14159,
                                        COL_DOUBLE = -1.61803,
                                        COL_SHORT_DECIMAL = {-short_decimal_value},
                                        COL_LONG_DECIMAL = {-long_decimal_value},
                                        COL_REAL = -37.73,
                                        COL_CHAR = 'Hello_update_pk',
                                        COL_VARCHAR = 'World_update_pk',
                                        COL_VARBINARY = 'Varbinary_update_pk',
                                        COL_TINYTEXT = 'TinyText_update_pk',
                                        COL_TEXT = 'TextData_update_pk',
                                        COL_MEDIUMTEXT = 'MediumTextData_update_pk',
                                        COL_LONGTEXT = 'LongTextData_update_pk',
                                        COL_ENUM = 'C',
                                        COL_YEAR = 2023,
                                        COL_DATE = '2023-08-18',
                                        COL_TIME = '12:34:56',
                                        COL_DATETIME = '2023-08-18 12:34:56',
                                        COL_DATETIME_6 = '2023-08-18 12:34:56.123456',
                                        COL_CUR_TIME = {get_cur_time()}
                                    WHERE pk = {pk}''')
                if mysql_cursor.rowcount > 0:
                    counter[choice-1] += 1
                    DML_TOTAL += 1
            if mysql_cursor.rowcount > 0:
                counter[choice-1] += 1
                DML_TOTAL += 1
                break
        dml_lock.release()
        # time.sleep(0.01)
    conn_lock.acquire()
    mysql_conn.close()
    conn_lock.release()
    print(f'[{format_cur_time()}]: [do_dml_thread-{tid}] - DML thread finish, insert({counter[0]}), delete({counter[1]}), update({counter[2]}), update_pk({counter[3]})')

"""
    Check the synchronization progress every `interval` seconds
"""
def check_sync(interval: float=1):
    conn_lock.acquire()
    foxlake_check_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    foxlake_check_cursor = foxlake_check_conn.cursor(buffered=True)
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

def test_sync():
    print(f'[{format_cur_time()}]: Start Test!')
    print(f'[{format_cur_time()}]: Connect to mysql')
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    conn_lock.release()
    mysql_cursor = mysql_conn.cursor(buffered=True)
    # mysql_cursor.execute(f'SET @@GLOBAL.GTID_MODE = ON')

    if DO_SNAPSHOT_IN_MYSQL:
        print(f'''[{format_cur_time()}]: Drop database '{DB_NAME}'_mysql in mysql''')
        mysql_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME}_mysql')
        print(f'''[{format_cur_time()}]: Create database '{DB_NAME}'_mysql in mysql''')
        mysql_cursor.execute(f'CREATE DATABASE {DB_NAME}_mysql')
        mysql_cursor.execute(f'USE {DB_NAME}_mysql')
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
                COL_FLOAT FLOAT,
                COL_LIMIT_FLOAT FLOAT(10, 5),
                COL_DOUBLE DOUBLE,
                COL_SHORT_DECIMAL DECIMAL(10, 5),
                COL_LONG_DECIMAL DECIMAL(63, 10),
                COL_REAL REAL,
                COL_CHAR CHAR(30),
                COL_VARCHAR VARCHAR(30),
                COL_VARBINARY VARBINARY(30),
                COL_TINYTEXT TINYTEXT,
                COL_TEXT TEXT,
                COL_MEDIUMTEXT MEDIUMTEXT,
                COL_LONGTEXT LONGTEXT,
                COL_ENUM ENUM('A', 'B', 'C'),
                COL_YEAR YEAR,
                COL_DATE DATE,
                COL_TIME TIME,
                COL_DATETIME DATETIME,
                COL_DATETIME_6 DATETIME(6),
                COL_CUR_TIME DOUBLE,
                PRIMARY KEY(PK)
        ) ''')

        print(f'[{format_cur_time()}]: Create snapshot in mysql')
        for i in range(1, SNAPSHOT_SIZE+1):
            num = random.randint(1, 10000)
            mysql_cursor.execute(f'''INSERT INTO {TABLE_NAME} VALUES(
                                    {i}, 
                                    TRUE,
                                    127,
                                    32767, 
                                    8388607, 
                                    2147483647, 
                                    9223372036854775807, 
                                    3.14159, 
                                    3.14159, 
                                    1.61803, 
                                    {short_decimal_value},
                                    {long_decimal_value},
                                    37.73,
                                    'Hello', 
                                    'World', 
                                    'Varbinary',
                                    'TinyText', 
                                    'TextData', 
                                    'MediumTextData', 
                                    'LongTextData', 
                                    'A', 
                                    2023, 
                                    '2023-08-18', 
                                    '12:34:56', 
                                    '2023-08-18 12:34:56',
                                    '2023-08-18 12:34:56.123456',
                                    {get_cur_time()}
                                 )''')
        print(f'[{format_cur_time()}]: Finish mysql snapshot')

    print(f'[{format_cur_time()}]: Connect to foxlake')
    conn_lock.acquire()
    foxlake_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    foxlake_cursor = foxlake_conn.cursor(buffered=True)

    if DO_SNAPSHOT_IN_FOXLAKE:
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

    check_sync_thread = threading.Thread(target=check_sync)
    check_sync_thread.daemon = True
    check_sync_thread.start()

    wait_sync(foxlake_cursor)
    # time.sleep(5)

    mysql_cursor.execute(select_bytes_query)
    res = next(mysql_cursor)
    dml_start_bytes = int(res[1])
    check_lag_thread = threading.Thread(target=check_lag, args=(mysql_cursor, foxlake_cursor, time.time(), dml_start_bytes))
    check_lag_thread.daemon = True
    check_lag_thread.start()

    if DO_DML:
        print(f'''[{format_cur_time()}]: Start DML threads''')
        for i in range(0, DML_THREAD_NUM):
            argList = [DML_NUM_PER_THREAD, i + 1]
            p = multiprocessing.Process(target=do_dml, args=argList)
            p.start()

    if DO_VALIDATE:
        validate_thread = threading.Thread(target=validate)
        validate_thread.daemon = True
        validate_thread.start()


    time.sleep(1)
    while True:
        if EXIT:
            exit(-1)
        # Plot the data
        fig, ax1 = plt.subplots()
        ax1.plot(times, lags, 'r-', label="Lag")
        ax1.set_ylabel("Lag(s)")
        ax1.tick_params(axis='y')

        ax2 = ax1.twinx()
        ax2.plot(times, scan_time, 'b-', label="Scan Latency")
        ax2.set_ylabel("Scan Latency(s)")
        ax2.tick_params(axis='y')

        lines = [ax1.get_lines()[0], ax2.get_lines()[0]]
        ax1.legend(lines, [line.get_label() for line in lines])

        # set the x label to `time`
        ax1.set_xlabel("Time(s)")

        plt.title("Lag and Scan Latency")
        plt.show()

        if WRITE_CSV:
            csv_filename = "3ld_lag_tmp.csv"
            data = list(zip(times, lags))

            with open(csv_filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Time", "Lag"])
                writer.writerows(data)

            csv_filename = "3ld_latency_tmp2.csv"
            data = list(zip(times, scan_time))

            with open(csv_filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Time", "Scan"])
                writer.writerows(data)

        time.sleep(180)

    for i in range(DML_THREAD_NUM):
        dml_threads[i].join()

    conn_lock.acquire()
    mysql_conn.close()
    foxlake_conn.close()
    conn_lock.release()

    print(f'[{format_cur_time()}]: Passed Test!')

# main
if __name__ == '__main__':
    test_sync()