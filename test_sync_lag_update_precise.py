import mysql.connector
import threading
import time
import random
import matplotlib.pyplot as plt
import csv

# global variables
DB_NAME = "syncdb_test_5"
TABLE_NAME = "test"
SNAPSHOT_SIZE =  10 * 10000
DML_THREAD_NUM = 5
DML_NUM_PER_THREAD = 10000000000
DML_TOTAL = 0
DO_SNAPSHOT = True
DO_DML = True

MYSQL_USER = "root"
MYSQL_PASSWORD = "Jk37373737"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306

FOXLAKE_USER = "foxlake_root"
FOXLAKE_PASSWORD = "foxlake2023"
FOXLAKE_HOST = "127.0.0.1"
FOXLAKE_PORT = 11288
FOXLAKE_STORAGE_NAME = "storage_sync_lag_111"
# FOXLAKE_STORAGE_URI = f'''s3://foxlake/test_sync/{FOXLAKE_STORAGE_NAME}'''
FOXLAKE_STORAGE_URI = f'''s3c://foxlakebucket/{FOXLAKE_STORAGE_NAME}'''
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
# select_time_query = "SELECT time FROM " + DB_NAME + "." + TABLE_NAME + " ORDER BY time DESC LIMIT 1"
select_time_query = "SELECT MAX(time) FROM " + DB_NAME + "." + TABLE_NAME
select_bytes_query = "SHOW GLOBAL STATUS LIKE 'Bytes_received'"

update_lock = threading.Lock()

select_time_query_cdc = "SELECT MAX(time) FROM " + "`" + TABLE_NAME + "||cdc `" + " USING URL '" + f'''s3c://127.0.0.1:9000/foxlakebucket/{FOXLAKE_STORAGE_NAME}''' + "/" + DB_NAME +"/foxdt/' CREDENTIALS = (" + "access_key_id = '" + FOXLAKE_STORAGE_ID + "' secret_access_key = '" + FOXLAKE_STORAGE_KEY + "') FILES = ('cdc/test/log/') FILE_FORMAT = (type = 'DML_CHANGE_LOG' for_internal_sql = true)"

select_num_query = "SELECT COUNT(*) FROM " + "`" + DB_NAME + "`" + "." + "`" + TABLE_NAME + "`"

lags = []
qpss = []
times = []
scan_time = []

"""
    Wait for synchronization completed.
    `timewait` should better be a little bigger than `flushInterval` in foxdt.
"""
def wait_sync(foxlake_cursor, interval: float=1, timewait=40, timeout=300):
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

def check_lag(mysql_cursor, foxlake_cursor, dml_start_time, dml_start_bytes):
    time.sleep(10)
    check_interval = 1
    mysql_cursor.execute(f'use {DB_NAME}')
    foxlake_cursor.execute(f'use {DB_NAME}')

    while True:
        update_lock.acquire()
        start = time.time()
        mysql_cursor.execute(select_time_query)
        mysql_res = next(mysql_cursor)
        mysql_query_time = time.time() - start
        start = time.time()
        foxlake_cursor.execute(select_time_query)
        foxlake_res = next(foxlake_cursor)
        foxlake_query_time = time.time() - start
        update_lock.release()
        query_time_margin = foxlake_query_time - mysql_query_time
        print(f'''fq = {foxlake_query_time}, mq = {mysql_query_time}, margin = {query_time_margin}''')
        # print(f'[{format_cur_time()}]: mysql_res = ' + str(mysql_res[0]) + ', foxlake_res = ' + str(foxlake_res[0]))
        if mysql_res and foxlake_res and mysql_res[0] and foxlake_res[0]:
            # lag should minus 8:00:00
            lag = mysql_res[0] - foxlake_res[0]
            lags.append(lag)
            scan_time.append(foxlake_query_time)
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

def check_num(foxlake_cursor):
    time.sleep(10)
    check_interval = 3.7
    foxlake_cursor.execute(f'use {DB_NAME}')

    while True:
        foxlake_cursor.execute(select_num_query)
        foxlake_res = next(foxlake_cursor)
        if foxlake_res and foxlake_cursor != SNAPSHOT_SIZE:
            print(f'[{format_cur_time()}]: [check_num_thread] - Num = ' + str(foxlake_res[0]) + ", should be " + str(SNAPSHOT_SIZE))
            exit(-1)
        time.sleep(check_interval)

"""
    Do DML operations for at most `dml_num` times.
"""
def do_dml(dml_num, tid):
    global DML_TOTAL
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    conn_lock.release()
    print(f'[{format_cur_time()}]: [do_dml_thread-{tid}] - DML thread start with dml_num = ' + str(dml_num))
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f'use {DB_NAME}')
    counter = [0] * 4
    for i in range(1, dml_num+1):
        while True:
            # generate a random number between 1 and 4
            pk = random.randint(1, SNAPSHOT_SIZE+1)
            num = random.randint(1, 10000)
            update_lock.acquire()
            mysql_cursor.execute(f'''UPDATE IGNORE {TABLE_NAME} SET num = {num}, name = "name_update", time = {get_cur_time()} WHERE pk = {pk}''')
            update_lock.release()
            DML_TOTAL += 1
            # sleep for 0.5 ms
            time.sleep(0.0005)
            break

            # choice = random.randint(1, 4)
            # if choice == 1: # insert
            #     mysql_cursor.execute(f'''INSERT IGNORE INTO {TABLE_NAME} VALUES({i}, "str", {num}, "name_insert", CURRENT_TIMESTAMP)''')
            # elif choice == 2: # delete
            #     mysql_cursor.execute(f'''DELETE IGNORE FROM {TABLE_NAME} WHERE pk = {pk}''')
            # elif choice == 3: # update
            #     mysql_cursor.execute(f'''UPDATE IGNORE {TABLE_NAME} SET num = {num}, name = "name_update", time = CURRENT_TIMESTAMP WHERE pk = {pk}''')
            # elif choice == 4: # update pk
            #     newPk = random.randint(1, SNAPSHOT_SIZE+1)
            #     mysql_cursor.execute(f'''UPDATE IGNORE {TABLE_NAME} SET pk = {newPk}, name = "name_update_pk", time = CURRENT_TIMESTAMP WHERE pk = {pk}''')
            # if mysql_cursor.rowcount > 0:
            #     counter[choice-1] += 1
            #     DML_TOTAL += 1
            #     break
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

def get_cur_time():
    return time.time()

def test_sync():
    print(f'[{format_cur_time()}]: Start Test!')
    print(f'[{format_cur_time()}]: Connect to mysql')
    conn_lock.acquire()
    mysql_conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, port=MYSQL_PORT,  autocommit=True)
    conn_lock.release()
    mysql_cursor = mysql_conn.cursor()

    if DO_SNAPSHOT:
        print(f'''[{format_cur_time()}]: Drop database '{DB_NAME}' in mysql''')
        mysql_cursor.execute(f'DROP DATABASE IF EXISTS {DB_NAME}')
        print(f'''[{format_cur_time()}]: Create database '{DB_NAME}' in mysql''')
        mysql_cursor.execute(f'CREATE DATABASE {DB_NAME}')
        mysql_cursor.execute(f'USE {DB_NAME}')
        print(f'''[{format_cur_time()}]: Create table '{DB_NAME}.{TABLE_NAME}' in mysql''')
        mysql_cursor.execute(f'''
            CREATE TABLE {TABLE_NAME} (
                pk INT,
                str VARCHAR(64) NOT NULL,
                num FLOAT NOT NULL,
                name TEXT NOT NULL,
                time DOUBLE NOT NULL,
                PRIMARY KEY(pk),
                INDEX idx_time (time)
        ) ''')

        print(f'[{format_cur_time()}]: Create snapshot in mysql')
        for i in range(1, SNAPSHOT_SIZE+1):
            num = random.randint(1, 10000)
            mysql_cursor.execute(f'''INSERT INTO {TABLE_NAME} VALUES({i}, "str", {num}, "name", {get_cur_time()})''')
        print(f'[{format_cur_time()}]: Finish mysql snapshot')

    print(f'[{format_cur_time()}]: Connect to foxlake')
    conn_lock.acquire()
    foxlake_conn = mysql.connector.connect(user=FOXLAKE_USER, password=FOXLAKE_PASSWORD, host=FOXLAKE_HOST, port=FOXLAKE_PORT, autocommit=True)
    conn_lock.release()
    foxlake_cursor = foxlake_conn.cursor()

    if DO_SNAPSHOT:
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

    mysql_cursor.execute(select_bytes_query)
    res = next(mysql_cursor)
    dml_start_bytes = int(res[1])
    check_lag_thread = threading.Thread(target=check_lag, args=(mysql_cursor, foxlake_cursor, time.time(), dml_start_bytes))
    check_lag_thread.daemon = True
    check_lag_thread.start()

    # check_num_thread = threading.Thread(target=check_num, args=(foxlake_cursor,))
    # check_num_thread.daemon = True
    # check_num_thread.start()

    if DO_DML:
        print(f'''[{format_cur_time()}]: Start DML threads''')
        dml_threads = []
        for i in range(DML_THREAD_NUM):
            dml_threads.append(threading.Thread(target=do_dml, args=(DML_NUM_PER_THREAD, i+1)))
            dml_threads[i].daemon = True
            dml_threads[i].start()

    while True:
        time.sleep(120)
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


        # csv_filename = "imd_no_delta_1kw.csv"
        # data = list(zip(times, lags))

        # with open(csv_filename, mode='w', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerow(["Time", "Lag"])
        #     writer.writerows(data)

        # csv_filename = "scd_no_delta_scan_1kw.csv"
        # data = list(zip(times, scan_time))

        # with open(csv_filename, mode='w', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerow(["Time", "Scan"])
        #     writer.writerows(data)

    for i in range(DML_THREAD_NUM):
        dml_threads[i].join()

    conn_lock.acquire()
    mysql_conn.close()
    foxlake_conn.close()
    conn_lock.release()

    print(f'[{format_cur_time()}]: Passed Test!')

# main
test_sync()