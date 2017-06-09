import os
import subprocess
import sys
import time
from xml.dom import minidom
import sqlite3
import socket

my_name = os.uname()[1]
print(my_name)

# Create the queue table if it doesn't already exist
con = sqlite3.connect('/usr/users/abau/infrastructure/queue.db', isolation_level='EXCLUSIVE', timeout=10)
cur = con.cursor()
cur.execute('BEGIN EXCLUSIVE')
cur.execute('''
create table if not exists queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command TEXT NOT NULL,
    started INTEGER DEFAULT 0,
    started_at REAL DEFAULT -1,
    started_by TEXT DEFAULT NULL,
    finished INTEGER DEFAULT 0,
    finished_at REAL DEFAULT -1,
    success INTEGER DEFAULT -1,
    gpuid INTEGER DEFAULT -1,
    pid INTEGER DEFAULT -1
)
''')
con.commit()
con.close()

processes = []

while True:
    # Check status of existing processes
    filtered_processes = []
    for process in processes:
        # If the process terminated,
        # record so in the queue database and remove it from
        # the list in RAM.
        if process['popen'].poll() is not None:
            con = sqlite3.connect('queue.db', isolation_level='EXCLUSIVE', timeout=10)
            cur = con.cursor()
            cur.execute('BEGIN EXCLUSIVE')

            if process['popen'].returncode == 0:
                cur.execute('update queue set finished=1, finished_at=?, success=1 where id=?', (time.time(), process['id'],))
            else:
                cur.execute('update queue set finished=1, finished_at=?, success=0 where id=?', (time.time(), process['id'],))
            con.commit()
            con.close()
        else:
            filtered_processes.append(process)

    processes = filtered_processes

    # Try starting a new process
    try:
        # Check the status of all the GPUs on this machine
        gpu_info_text = subprocess.check_output(['nvidia-smi', '-q', '-x'])
        gpu_info = minidom.parseString(gpu_info_text)

        gpus = gpu_info.getElementsByTagName('gpu')

        for i, gpu in enumerate(gpus):
            # Check whether any processes are running
            # on this GPU
            gpu_processes = gpu.getElementsByTagName('processes')[0].getElementsByTagName('process_info')

            # If there are none, take it.
            if len(gpu_processes) == 0:
                # See what the next command to run is.
                con = sqlite3.connect('queue.db', isolation_level='EXCLUSIVE', timeout=10)
                cur = con.cursor()
                cur.execute('BEGIN EXCLUSIVE')

                # Pick any command that hasn't been started
                cur.execute('select * from queue where started=0')
                row = cur.fetchone()

                # If there is such a command,
                # run it.
                if row is not None:
                    log_file = open('logs/%d-%f-"%s".txt' % (row[0], time.time(), row[1]), 'wb')
                    err_file = open('err/%d-%f-"%s".txt' % (row[0], time.time(), row[1]), 'wb')
                    p = subprocess.Popen('CUDA_VISIBLE_DEVICES=%d %s' % (i, row[1]), shell=True, stdout=log_file, stderr=err_file)
                    processes.append({
                        'id': row[0],
                        'popen': p
                    })
                    log_file.close()
                    err_file.close()

                    cur.execute('update queue set started=1, started_at=?, started_by=?, gpuid=?, pid=? where id=?',
                            (time.time(), my_name, i, p.pid, row[0]))

                con.commit()
                con.close()

    except Exception as inst:
        print('Could not check for open GPUs')

    time.sleep(1)
