#!/usr/bin/python
import os,sys
import datetime
from collections import defaultdict
import sqlite3
from seed2kegg import seed_data_util
from seed2kegg import kegg_data_util
from seed2kegg import db_utils

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

def fill_seed2kegg_mappings_table(cursor, infile, identity_cutoff, mismatch_cutoff):
    ''' This function fills seed2kegg_mappings table from two sources.
    First, it finds all SEED genes mapped to a UniRef proteins and all KEGG
    genes mapped to that UniRef protein and writes those pairs into the 
    table.
    Second, it imports DIAMOND output for direct comparison of remaining 
    SEED and KEGG proteins.
    Databases for KEGG, SEED and UniRef must be attached.       '''
    insert_sql_statement = 'INSERT INTO seed2kegg_mappings \
    (seed_uid, kegg_uid, proxy, evidence_seed, evidence_kegg) \
    SELECT seed2uniref_mappings.seed_uid, kegg2uniref_mappings.kegg_uid, \
    "UniRef", seed2uniref_mappings.evidence, kegg2uniref_mappings.evidence \
    FROM seed2uniref_mappings \
    JOIN uniref_proteins ON seed2uniref_mappings.uniref_uid=uniref_proteins.uid \
    JOIN kegg2uniref_mappings ON uniref_proteins.uid = kegg2uniref_mappings.uniref_uid'
    cursor.execute(insert_sql_statement)
    
    kegg_sql_query = 'SELECT kegg_genes.uid FROM kegg_genes JOIN \
        kegg_genomes ON kegg_genes.kegg_genome_uid = kegg_genomes.uid \
        WHERE kegg_genomes.kegg_genome_id IS ? \
        AND kegg_genes.kegg_gene_id_primary IS ? '
    insert_sql_statement = 'INSERT INTO seed2kegg_mappings \
    (seed_uid, kegg_uid, proxy, evidence_seed, evidence_kegg) \
    VALUES (?, ?, ?, ?, ?)'
    
    seed2kegg_mappings = []
    with open(infile, 'r') as f:
        for line in f.readlines():
            line = line.rstrip('\n\r')
            try: 
                line_tokens = line.split('\t')
                kegg_unique_id = line_tokens[0]
                seed_id = line_tokens[1]
                identity = line_tokens[2]
                mismatches= line_tokens[4]
                kegg_genome,kegg_gene = kegg_unique_id.split(':')
                if float(identity) < identity_cutoff:
                    if int(mismatches) > mismatch_cutoff:
                        continue
                cursor.execute(kegg_sql_query, (kegg_genome, kegg_gene))
                data=cursor.fetchall()
                if len(data) == 1:
                    seed2kegg_mappings.append((seed_data_util.get_gene_uid(cursor, seed_id), data[0][0], 'direct comparison', 'diamond identity ' + identity + '% mismatches ' + mismatches, ''))
                elif len(data) > 1:
                    raise SystemExit('ERROR: More than one entry for gene %s from genome %s: check database integrity'%(gene_id, genome_id))
                else:
                    continue # skip this row if no such gene in the KEGG database. It may happen if this gene is from Eukaryotes.
            except  ValueError:
                print ('Error in parsing line: ' + line)
        f.closed
    if len(seed2kegg_mappings) != 0:
        cursor.executemany(insert_sql_statement, seed2kegg_mappings)
    seed_data_util.create_seed2kegg_mappings_index(cursor)

def report_database_statistics(cursor):
    #Databases for KEGG, SEED and UniRef must be attached.
    cursor.execute('SELECT COUNT(*) FROM uniref_proteins')
    print('Total number of UniRef proteins:', cursor.fetchone()[0])
    cursor.execute('SELECT COUNT(*) FROM seed_genes')
    print('Total number of SEED genes:', cursor.fetchone()[0])
    cursor.execute('SELECT COUNT(*) FROM kegg_genes')
    print('Total number of KEGG genes:', cursor.fetchone()[0])
    cursor.execute('SELECT COUNT(DISTINCT seed_uid) FROM seed2kegg_mappings')
    print('Total number of SEED genes mapped to KEGG:', cursor.fetchone()[0])
    cursor.execute('SELECT COUNT(DISTINCT kegg_uid) FROM seed2kegg_mappings')
    print('Total number of KEGG genes mapped to SEED:', cursor.fetchone()[0])
    cursor.execute('SELECT COUNT(DISTINCT seed_genes.uid) FROM seed_genes \
    JOIN seed_gene2role ON seed_genes.uid = seed_gene2role.seed_gene_uid')
    print('Total number of SEED genes assigned to functional roles:', cursor.fetchone()[0])

def export_kegg_unmapped_proteins(cursor, infile, outfile_name):
    sql_query='SELECT DISTINCT kegg_genomes.kegg_genome_id, kegg_genes.kegg_gene_id_primary \
        FROM kegg_genes JOIN kegg_genomes ON kegg_genes.kegg_genome_uid=kegg_genomes.uid \
        LEFT JOIN seed2kegg_mappings ON kegg_genes.uid=seed2kegg_mappings.kegg_uid \
        LEFT JOIN seed_genes ON seed2kegg_mappings.seed_uid=seed_genes.uid \
        WHERE seed_genes.uid IS NULL'
    cursor.execute(sql_query)
    data = cursor.fetchall()
    prot_ids = {}
    if data is None:
        return
    elif len(data) == 0:
        return
    else:
        for entry in data:
            prot_ids [entry[0] + ':' + entry[1]] = 1
    flag = False
    with open(outfile_name, 'w') as outfile:
        with open(infile, 'r') as f:
            for line in f:
                if line.startswith('>'):
                    line_tokens = line[1:].split(' ')
                    if line_tokens[0] in prot_ids:
                        flag = True
                    else:
                        flag = False
                if flag == True:
                    outfile.write(line)
            f.closed
        outfile.closed

def import_collection_tsv(cursor, infile, collection_name, info, version):
    db_utils.delete_collection(cursor, collection_name, version)

    insert_collection_sql_statement = 'INSERT INTO collections \
        (collection_name, info, version) VALUES  (?, ?, ?)'
    insert_func_sql_statement = 'INSERT INTO collection2function \
        (collection_uid, function_uid, source_db, name, category) VALUES  (?, ?, ?, ?, ?)'
    col_uid_sql_query = 'SELECT uid FROM collections WHERE \
    collection_name = ? AND version = ?'

    # check if new collection already exists
    cursor.execute(col_uid_sql_query, (collection_name, version))
    data = cursor.fetchone()
    if data is not None:
        raise SystemExit('Import ERROR: collection ' + collection_name + ' with version ' + version + ' already exists')

    counter = 0
    collection_uid = None
    cursor.execute(insert_collection_sql_statement, (collection_name, info, version))
    cursor.execute(col_uid_sql_query, (collection_name, version))
    data = cursor.fetchone()
    if data is None:
        raise SystemExit('Import unsuccessful')
    else:
        collection_uid = data[0]
    
    collection_data = []
    with open(infile, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue # skip comment lines
            line = line.rstrip('\n\r')
            line = line.replace("'", "''")
            line_tokens = line.split('\t')
            if len(line_tokens) == 5:
                function_uid=''
                if line_tokens[0] == 'SEED':
                    function_uid = seed_data_util.get_role_uid(cursor, line_tokens[1])
                elif line_tokens[0] == 'KEGG':
                    cursor.execute('SELECT uid FROM kegg_orthologs WHERE ko_id=?',(line_tokens[1],))
                    function_uid = cursor.fetchone()[0]
                collection_data.append((collection_uid,function_uid,line_tokens[0],line_tokens[2],line_tokens[3]))
            else:
                raise SystemExit ('Unexpected number of fields in line: ' + line)
        f.closed
    cursor.executemany(insert_func_sql_statement, collection_data)
    db_utils.create_collection2function_index(cursor)
    print (len(collection_data), ' functions imported into collection')

    
def make_collection_gene_list(cursor, collection_name, version):
    ret_val=defaultdict(dict) #ret_val[gene_id]['source']=source_db ret_val[gene_id]['func']=list of functions
    function_list_sql_query = 'SELECT collection2function.function_uid, collection2function.source_db FROM collection2function \
        JOIN collections ON collection2function.collection_uid=collections.uid \
        WHERE collections.collection_name=? AND collections.version=?'
    seed_genes_sql_query='SELECT seed_genes.seed_gene_id \
        FROM seed_genes \
        JOIN seed_gene2role ON seed_genes.uid=seed_gene2role.seed_gene_uid \
        JOIN seed_functional_roles ON seed_gene2role.seed_role_uid=seed_functional_roles.uid \
        WHERE seed_functional_roles.uid = ?'
    kegg_genes_sql_query='SELECT kegg_genomes.kegg_genome_id||":"||kegg_genes.kegg_gene_id_primary\
        FROM kegg_genes JOIN kegg_genomes ON kegg_genes.kegg_genome_uid=kegg_genomes.uid \
        JOIN kegg_genes2ko ON kegg_genes.uid=kegg_genes2ko.kegg_gene_uid \
        JOIN kegg_orthologs ON kegg_genes2ko.kegg_ko_uid=kegg_orthologs.uid \
        LEFT JOIN seed2kegg_mappings ON kegg_genes.uid=seed2kegg_mappings.kegg_uid \
        LEFT JOIN seed_genes ON seed2kegg_mappings.seed_uid=seed_genes.uid \
        LEFT JOIN seed_gene2role ON seed_genes.uid=seed_gene2role.seed_gene_uid \
        LEFT JOIN seed_functional_roles ON seed_gene2role.seed_role_uid=seed_functional_roles.uid \
        WHERE seed_functional_roles.uid IS NULL AND kegg_orthologs.uid=?'

    functions = defaultdict(list)
    cursor.execute(function_list_sql_query, (collection_name, version))
    function_data = cursor.fetchall()
    #print(function_data)
    if len(function_data) == 0:
        print ('Collection not found or empty')
        return ret_val
    for function_entry in function_data:
        functions [function_entry[0]] = function_entry[1]
    for function in functions:
        print('Processing function', function)
        if functions[function] == 'SEED':
            function_id = seed_data_util.get_role_id(cursor,function)
            cursor.execute(seed_genes_sql_query, (function,))
            gene_data = cursor.fetchall()
            #print(gene_data)
            if len(gene_data) > 0:
                for gene_id in gene_data:
                    if gene_id[0] in ret_val:
                        ret_val[gene_id[0]]['func'].append(function_id)
                    else:
                        ret_val[gene_id[0]]['source']='SEED'
                        ret_val[gene_id[0]]['func']=[function_id]
        elif functions[function] == 'KEGG':
            function_id = kegg_data_util.get_ko_id(cursor,function)
            cursor.execute(kegg_genes_sql_query, (function,))
            gene_data = cursor.fetchall()
            #print(gene_data)
            if len(gene_data) > 0:
                for gene_id in gene_data:
                    if gene_id[0] in ret_val:
                        ret_val[gene_id[0]]['func'].append(function_id)
                    else:
                        ret_val[gene_id[0]]['source']='KEGG'
                        ret_val[gene_id[0]]['func']=[function_id]
        else:
            raise SystemExit('Unknown data source ' + functions[function] + ' for function ' + function)
    return ret_val
    
def export_collection_proteins(gene_dict, infile, outfile):
    flag = False
    with open(outfile, 'a') as of:
        with open(infile, 'r') as f:
            for line in f:
                if line.startswith('>'):
                    line = line.rstrip()
                    line = line[1:]
                    gene_id = line.split(' ')[0]
                    if gene_id in gene_dict:
                        flag = True
                        of.write('>' + '|'.join(dict((el,0) for el in gene_dict[gene_id]['func']).keys()) + '_' + gene_id + '\n')
                    else:
                        flag = False
                else:
                    if flag == True:
                        line = line.strip()
                        of.write(line + '\n')
            f.closed
        of.closed

if __name__=='__main__':
    print ('This module should not be executed as script.')

