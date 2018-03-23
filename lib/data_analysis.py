#!/usr/bin/python
import os,sys
from collections import defaultdict
import sqlite3

'''
This module contains all the functions for data analysis. 
'''

def find_kegg2uniref_identical_mappings(cursor):
    # this function expects that kegg_data and uniref databases are available.
    # It finds UniRef proteins and KEGG genes with identical hash values
    # and stores them in the kegg2uniref_mappings table.
    kegg_sql_query = 'SELECT uid,kegg_seq_hash FROM kegg_genes'
    uniref_sql_query = 'SELECT uniref_proteins.uid,uniref_proteins.protein_hash FROM uniref_proteins'
    insert_sql_statement = 'INSERT INTO kegg2uniref_mappings \
    (kegg_uid, uniref_uid, evidence) VALUES  (?, ?, ?)'

    kegg_hashes = defaultdict(list)
    uniref2kegg_mappings = []
    
    cursor.execute(kegg_sql_query)
    for kegg_gene in cursor.fetchall():
        kegg_hashes[kegg_gene[1]].append(kegg_gene[0])
    
    cursor.execute(uniref_sql_query)
    for uniref_prot in cursor.fetchall():
        if uniref_prot[1] in kegg_hashes:
            for kegg_uid in kegg_hashes[uniref_prot[1]]:
                uniref2kegg_mappings.append((kegg_uid, uniref_prot[0], 'identical hash values'))
    if len(uniref2kegg_mappings) != 0:
        cursor.executemany(insert_sql_statement, uniref2kegg_mappings)

def find_seed2uniref_identical_mappings(cursor):
    # this function expects that seed_data and uniref databases are available.
    # It finds UniRef proteins and SEED genes with identical hash values
    # and stores them in the seed2uniref_mappings table.
    seed_sql_query = 'SELECT uid,protein_hash FROM seed_genes'
    uniref_sql_query = 'SELECT uniref_proteins.uid,uniref_proteins.protein_hash FROM uniref_proteins'
    insert_sql_statement = 'INSERT INTO seed2uniref_mappings \
    (seed_uid, uniref_uid, evidence) VALUES  (?, ?, ?)'

    seed_hashes = defaultdict(list)
    uniref2seed_mappings = []
    
    cursor.execute(seed_sql_query)
    for seed_gene in cursor.fetchall():
        seed_hashes[seed_gene[1]].append(seed_gene[0])
    
    cursor.execute(uniref_sql_query)
    for uniref_prot in cursor.fetchall():
        if uniref_prot[1] in seed_hashes:
            for seed_uid in seed_hashes[uniref_prot[1]]:
                uniref2seed_mappings.append((seed_uid, uniref_prot[0], 'identical hash values'))
    if len(uniref2seed_mappings) != 0:
        cursor.executemany(insert_sql_statement, uniref2seed_mappings)

def correct_seed_annotations(cursor, infile):
    pass

if __name__=='__main__':
    print ('This module should not be executed as script.')

