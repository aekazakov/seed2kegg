#!/usr/bin/python
import os,sys
import datetime
from collections import defaultdict
import sqlite3
from lib import seed_data_util

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

def correct_seed_annotations(cursor, infile, comment):
    changes = defaultdict(dict)
    line_number = 1 # counter is line number
    with open(infile, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue #skip header and comments
            line_number += 1
            line = line.rstrip('\n\r')
            line_tokens = line.split('\t')
            if len(line_tokens) != 8:
                raise SystemExit('Patch file validation error: unexpected number of fields in line ' + str(line_number) + ': ' + line)
            #get gene uid
            gene_uid = seed_data_util.get_gene_uid(cursor, line_tokens[1])
            #get role uids
            roles_current = line_tokens[3].split(',')
            role_uids_current = []
            roles_remove = line_tokens[6].split(',')
            role_uids_remove = []
            roles_add = line_tokens[7].split(',')
            role_uids_add = []
            if roles_current:
                for role in roles_current:
                    if role != 'No roles':
                        if (seed_data_util.check_gene_to_role_link(cursor, gene_uid, role)):
                            role_uids_current.append(seed_data_util.get_role_uid(cursor, role))
                        else:
                            raise SystemExit('Patch file validation error: gene ' + line_tokens[1] + ' is not associated with SEED role ' + role + '. Check line ' + str(line_number))
            if roles_remove:
                for role in roles_remove:
                    if role:
                        if (seed_data_util.check_gene_to_role_link(cursor, gene_uid, role)):
                            role_uids_remove.append(seed_data_util.get_role_uid(cursor, role))
                        else:
                            raise SystemExit('You are trying to remove SEED role ' + role + ', which is not assigned to gene ' + line_tokens[1] + '. Check line ' + str(line_number))
            if roles_add:
                for role in roles_add:
                    if role:
                        if (seed_data_util.check_gene_to_role_link(cursor, gene_uid, role)):
                            raise SystemExit('You are trying to add SEED role ' + role + ' to gene ' + line_tokens[1] + ', but this association already exists. Check line ' + str(line_number))
                        else:
                            role_uids_add.append(seed_data_util.get_role_uid(cursor, role))
            #store proposed changes in dictionary
            changes[line_number]['gene_uid'] = gene_uid
            changes[line_number]['current roles'] = role_uids_current
            changes[line_number]['roles_to_add'] = role_uids_add
            changes[line_number]['roles_to_remove'] = role_uids_remove
        f.closed

    for line_number in changes:
        # make changes in seed_gene2role table
        for role_uid in changes[line_number]['roles_to_remove']:
            now = datetime.datetime.now()
            # make changes in seed_gene2role_changes table
            sql_query = 'INSERT INTO seed_gene2role_changes \
            (seed_gene_uid, seed_role_uid, date, action, comment) \
            VALUES (?, ?, ?, ?, ?)'
            cursor.execute(sql_query, (changes[line_number]['gene_uid'], role_uid, now.strftime("%Y-%m-%d %H:%M"), 'deleted', comment + '. Old comment: ' + seed_data_util.get_gene2role_info(cursor, changes[line_number]['gene_uid'], role_uid)))
            # make changes in seed_gene2role table
            sql_query = 'DELETE FROM seed_gene2role \
            WHERE seed_gene2role.seed_gene_uid = ? \
            AND seed_gene2role.seed_role_uid = ?'
            cursor.execute(sql_query, (changes[line_number]['gene_uid'], role_uid))
        for role_uid in changes[line_number]['roles_to_add']:
            now = datetime.datetime.now()
            # make changes in seed_gene2role table
            sql_query = 'INSERT INTO seed_gene2role \
            (seed_gene_uid, seed_role_uid, comment) VALUES (?, ?, ?)'
            cursor.execute(sql_query, (changes[line_number]['gene_uid'], role_uid, comment + ' on ' + now.strftime("%Y-%m-%d %H:%M")))
            # make changes in seed_gene2role_changes table
            sql_query = 'INSERT INTO seed_gene2role_changes \
            (seed_gene_uid, seed_role_uid, date, action, comment) \
            VALUES (?, ?, ?, ?, ?)'
            cursor.execute(sql_query, (changes[line_number]['gene_uid'], role_uid, now.strftime("%Y-%m-%d %H:%M"), 'added', comment))

if __name__=='__main__':
    print ('This module should not be executed as script.')

