#!/usr/bin/python

import sqlite3
import mmh3
from lib import db_utils

'''
This module contains all the functions needed for load UniRef data into sqlite database. 
All UniRef data are kept in uniref_proteins table in a separate database, 
because of large volume of the table.
'''


def calculate_sequence_hash(seq):
    return mmh3.hash128(seq)

def drop_tables(cursor):
    cursor.execute('DROP TABLE IF EXISTS uniref_proteins')

def drop_indices(cursor):
    cursor.execute('DROP INDEX IF EXISTS uniref_unirefid_1')
    cursor.execute('DROP INDEX IF EXISTS uniref_proteinhash')

def create_uniref_proteins_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `uniref_proteins` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`uniref_id`	TEXT NOT NULL UNIQUE,\
	`function`	TEXT,\
	`cluster_size`	INTEGER,\
	`tax`	TEXT,\
	`tax_id`	TEXT,\
	`rep_id`	TEXT,\
	`protein_hash`	TEXT)')

def create_uniref_proteins_indices(cursor):
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `uniref_unirefid_1` ON `uniref_proteins` (`uniref_id` ASC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS `uniref_proteinhash` ON `uniref_proteins` (`protein_hash` ASC)')

def import_uniref_fasta(cursor, uniref_fasta_file):
    counter = 0
    uniref_data = []
    protein_data = []
    protein_seq_lines = []

    with open(uniref_fasta_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                if counter%10000 == 0:
                    if len(uniref_data) != 0:
                        cursor.executemany('INSERT INTO uniref_proteins \
                        (uniref_id,function,cluster_size,tax,tax_id,rep_id,protein_hash) \
                        VALUES  (?, ?, ?, ?, ?, ?, ?)', uniref_data)
                        print (counter,' proteins processed')
                        uniref_data = []
                if protein_data != []:
                    hash_string = '{:032X}'.format(int(db_utils.calculate_sequence_hash("".join(protein_seq_lines))))
                    protein_data.append(hash_string)
                    uniref_data.append(protein_data)
                    protein_data = []
                    protein_seq_lines = []
                line = line[1:]
                line = line.replace("'","''")
                line, rep_id = line.split(' RepID=')
                line, tax_id = line.split(' TaxID=')
                line, tax = line.split(' Tax=')
                function, cluster_size = line.split(' n=')
                uniref_id = function.split(' ')[0]
                protein_data.extend((uniref_id,function,int(cluster_size),tax,tax_id,rep_id))
                counter += 1
            else:
                protein_seq_lines.append(line)
        f.closed
    hash_string = '{:032X}'.format(int(db_utils.calculate_sequence_hash("".join(protein_seq_lines))))
    protein_data.append(hash_string)
    uniref_data.append(protein_data)
    cursor.executemany('INSERT INTO uniref_proteins \
                (uniref_id,function,cluster_size,tax,tax_id,rep_id,protein_hash) \
                VALUES  (?, ?, ?, ?, ?, ?, ?)', uniref_data)
    print (counter,' proteins processed')
    
if __name__=='__main__':
    print ('This module should not be executed as script.')
