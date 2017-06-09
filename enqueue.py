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

command = sys.argv[1]

# See what the next command to run is.
con = sqlite3.connect('/usr/users/abau/infrastructure/queue.db', isolation_level='EXCLUSIVE', timeout=10)
cur = con.cursor()
cur.execute('BEGIN EXCLUSIVE')

print(command)

# Pick any command that hasn't been started
cur.execute('insert into queue (command) values (?)', (command,))

con.commit()
con.close()
