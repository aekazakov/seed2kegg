#!/usr/bin/python
import mmh3
import sqlite3

'''
This module contains general utility functions for sqlite database. 
'''

def connect_local_database(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def attach_local_database(cursor, db_file, db_alias=None):
    if db_alias is None:
        cursor.execute('ATTACH DATABASE ?', (db_file,))
    else:
        cursor.execute('ATTACH DATABASE ? AS ?', (db_file,db_alias))

def calculate_sequence_hash(seq):
    return mmh3.hash128(seq)

