import os
import subprocess
import sys
import time
from xml.dom import minidom
import sqlite3
import socket

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

# See what the next command to run is.
con = sqlite3.connect('/usr/users/abau/infrastructure/queue.db')
cur = con.cursor()
cur.execute('select * from queue order by started asc, finished asc')

rows = cur.fetchall()
for row in rows:
    if row[2] == 1:
        if row[7] == 1:
            print('Finished successfully (%s %d): "%s"' % (row[4], row[8], row[1]))
        elif row[7] == 0:
            print('FAILED (%s %d): "%s"' % (row[4], row[8], row[1]))
        elif row[7] == -1:
            print('Running on %s %d, pid %d: "%s"' % (row[4], row[8], row[9], row[1]))
    else:
        print('Enqueued: %s' % (row[1],))

con.close()
