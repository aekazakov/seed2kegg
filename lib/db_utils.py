#!/usr/bin/python
import mmh3
import sqlite3

'''
This module contains general utility functions for sqlite database. 
'''

def connect_local_database(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def attach_local_database(cursor, db_file, db_alias):
    cursor.execute('ATTACH DATABASE ? AS ?', (db_file,db_alias))

def calculate_sequence_hash(seq):
    return mmh3.hash128(seq)

def create_collections_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `collections` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`collection_name`	TEXT NOT NULL UNIQUE,\
	`info`	TEXT,\
	`version`	TEXT NOT NULL)')

def create_collection2function_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `collection2function` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`collection_uid`	INTEGER NOT NULL,\
	`function_uid`	INTEGER NOT NULL,\
	`source_db`	TEXT NOT NULL,\
	`name`	TEXT NOT NULL,\
	`category`	TEXT)')

def create_collection2function_index(cursor):
    cursor.execute('CREATE INDEX IF NOT EXISTS `collection2function_collectionuid_1` ON `collection2function` (`collection_uid` ASC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS `collection2function_functionuid_1` ON `collection2function` (`function_uid` ASC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS `collection2function_sourcedb_1` ON `collection2function` (`source_db` ASC)')

