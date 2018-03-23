#!/usr/bin/python
import os,sys
import sqlite3
from lib import db_utils

'''
This module contains all the functions for loading SEED data into 
a local sqlite database. 

IMPORTANT: functions for data import DO NOT save changes in the
database. Call commit() on the database connection to store the
changes permanently.
'''

def drop_tables(cursor):
    # Drops all tables except seed2uniref_mappings
    print ('Dropping seed2uniref_mappings table...')
    cursor.execute('DROP TABLE IF EXISTS seed2uniref_mappings')
    print ('Dropping seed_gene2role table...')
    cursor.execute('DROP TABLE IF EXISTS seed_gene2role')
    print ('Dropping seed_gene2cluster100 table...')
    cursor.execute('DROP TABLE IF EXISTS seed_gene2cluster100')
    print ('Dropping seed_cluster100 table...')
    cursor.execute('DROP TABLE IF EXISTS seed_cluster100')
    print ('Dropping seed_genes table...')
    cursor.execute('DROP TABLE IF EXISTS seed_genes')
    print ('Dropping seed_genomes table...')
    cursor.execute('DROP TABLE IF EXISTS seed_genomes')
    print ('Dropping seed_functional_roles table...')
    cursor.execute('DROP TABLE IF EXISTS seed_functional_roles')

def drop_seed2uniref_mappings_table(cursor):
    print ('Dropping seed2uniref_mappings table...')
    cursor.execute('DROP TABLE IF EXISTS seed2uniref_mappings')

def drop_seed2uniref_mappings_indices(cursor):
    print ('Dropping seed2uniref_mappings table...')
    cursor.execute('DROP INDEX IF EXISTS seed2uniref_seedid_1')
    cursor.execute('DROP INDEX IF EXISTS seed2uniref_unirefid_1')

def drop_all_indices(cursor):
    cursor.execute('DROP INDEX IF EXISTS seed_funcroles_roleid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_genomes_genomeid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_genomes_latestversion_1')
    cursor.execute('DROP INDEX IF EXISTS seed_cluster100_clusterid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_cluster100_refgene_1')
    cursor.execute('DROP INDEX IF EXISTS seed_genes_geneid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_genes_genomeid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_gene2cluster100_clusterid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_gene2cluster100_geneid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_gene2role_geneid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_gene2role_roleid_1')

def create_seed_functional_roles_table(cursor):
    cursor.execute('CREATE TABLE "seed_functional_roles" (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`seed_role_id`	TEXT NOT NULL UNIQUE,\
	`seed_role_name`	TEXT NOT NULL UNIQUE,\
	`seed_role_category`	TEXT)')

def create_seed_genomes_table(cursor):
    cursor.execute('CREATE TABLE "seed_genomes" (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`seed_genome_id`	TEXT NOT NULL UNIQUE,\
	`tax_id`	TEXT,\
	`genome_name`	TEXT NOT NULL,\
	`lineage`	TEXT,\
	`latest_version`	INTEGER NOT NULL,\
	`kingdom`	TEXT)')

def create_seed_genes_table(cursor):
    cursor.execute('CREATE TABLE "seed_genes" (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`seed_gene_id`	TEXT NOT NULL UNIQUE,\
	`seed_genome_uid`	INTEGER NOT NULL,\
	`protein_hash`	TEXT)')

def create_seed_cluster100_table(cursor):
    cursor.execute('CREATE TABLE `seed_cluster100` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`cluster_id`	TEXT NOT NULL UNIQUE,\
	`ref_gene`	TEXT NOT NULL UNIQUE,\
	`n`	INTEGER NOT NULL)')

def create_seed_gene2cluster100_table(cursor):
    cursor.execute('CREATE TABLE `seed_gene2cluster100` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`seed_gene_uid`	INTEGER NOT NULL,\
	`seed_cluster100_uid`	INTEGER NOT NULL)')

def create_seed_gene2role_table(cursor):
    cursor.execute('CREATE TABLE "seed_gene2role" (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`seed_gene_uid`	INTEGER NOT NULL,\
	`seed_role_uid`	INTEGER NOT NULL,\
	`comment`	TEXT)')

def create_seed2uniref_mappings_table(cursor):
    cursor.execute('CREATE TABLE "seed2uniref_mappings" (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`seed_uid`	INTEGER NOT NULL,\
	`uniref_uid`	INTEGER NOT NULL,\
	`evidence`	TEXT NOT NULL)')

def create_tables(cursor):
    create_seed_functional_roles_table(cursor)
    create_seed_genomes_table(cursor)
    create_seed_genes_table(cursor)
    create_seed_gene2role_table(cursor)

def create_seed_roles_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `seed_funcroles_roleid_1` ON \
    `seed_functional_roles` (`seed_role_id` ASC)')

def create_seed_genomes_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `seed_genomes_genomeid_1` ON `seed_genomes` (`seed_genome_id` ASC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS `seed_genomes_latestversion_1` ON `seed_genomes` (`latest_version` ASC)')

def create_seed_clusters_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `seed_cluster100_clusterid_1` ON `seed_cluster100` (`cluster_id` ASC)')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `seed_cluster100_refgene_1` ON `seed_cluster100` (`ref_gene` ASC)')

def create_seed_genes_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `seed_genes_geneid_1` ON `seed_genes` (`seed_gene_id` ASC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS `seed_genes_genomeid_1` ON `seed_genes` (`seed_genome_uid` ASC)')

def create_seed_genes2clusters_index(cursor):
    cursor.execute('CREATE INDEX IF NOT EXISTS `seed_gene2cluster100_clusterid_1` ON `seed_gene2cluster100` (`seed_cluster100_uid` ASC)')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `seed_gene2cluster100_geneid_1` ON `seed_gene2cluster100` (`seed_gene_uid` ASC)')

def create_seed2uniref_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS `seed2uniref_seedid_1` ON `seed2uniref_mappings` (`seed_uid` ASC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS `seed2uniref_unirefid_1` ON `seed2uniref_mappings` (`uniref_uid` ASC)')

def create_seed_genes2roles_index(cursor):
    cursor.execute('CREATE INDEX IF NOT EXISTS `seed_gene2role_geneid_1` ON `seed_gene2role` (`seed_gene_uid` ASC)')
    cursor.execute('CREATE INDEX IF NOT EXISTS `seed_gene2role_roleid_1` ON `seed_gene2role` (`seed_role_uid` ASC)')

def import_seed_functional_roles_table(cursor, seed_roles_file):
    seed_roles_data = []
    with open(seed_roles_file, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue # skip comment lines
            line = line.rstrip()
            line = line.replace("'", "''")
            line_tokens = line.split('\t')
            if len(line_tokens) == 3:
                seed_roles_data.append((line_tokens[0],line_tokens[1]))
            else:
                print ('Unexpected number of fields in line: ', line)
                sys.exit(1)
    f.closed
    cursor.executemany('INSERT INTO seed_functional_roles(seed_role_id,seed_role_name) VALUES (?, ?)', seed_roles_data)
    create_seed_roles_index(cursor)
    print (len(seed_roles_data), 'SEED functional roles imported')

def import_seed_genomes(cursor, seed_genome_file):
    seed_genomes_data = []
    with open(seed_genome_file, 'r') as f:
        for line in f:
            line = line.rstrip()
            line = line.replace("'", "''")
            line_tokens = line.split('\t')
            if len(line_tokens) == 6:
                seed_genomes_data.append(line_tokens)
            else:
                print ('Unexpected number of fields in line: ', line)
    f.closed
    cursor.executemany('INSERT INTO seed_genomes(seed_genome_id,\
    tax_id, genome_name, lineage, kingdom, latest_version) VALUES (?, ?, ?, ?, ?, ?)', seed_genomes_data)
    create_seed_genomes_index(cursor)
    print (len(seed_genomes_data), ' SEED genomes imported')

def import_seed_genes(cursor, directory):
    genomes_sql_query = 'SELECT uid, seed_genome_id FROM seed_genomes WHERE latest_version = 1 ORDER BY seed_genome_id ASC'
    sql_query = 'SELECT uid FROM seed_genomes WHERE seed_genome_id IS ?'
    insert_sql_query = 'INSERT INTO seed_genes (seed_gene_id, \
    seed_genome_uid, protein_hash) VALUES  (?, ?, ?)'
    
    cursor.execute(genomes_sql_query)
    genomes_list = cursor.fetchall()
    
    for genome_data in genomes_list:
        genes_data = []
        gene_data = []
        protein_seq_lines = []
        print ('Importing genes from genome', genome_data[1])
        with open(os.path.join(directory,genome_data[1] + '_proteins.txt'), 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('>'):
                    if len(protein_seq_lines) != 0:
                        hash_string = '{:032X}'.format(int(db_utils.calculate_sequence_hash("".join(protein_seq_lines))))
                        gene_data.append(hash_string)
                        genes_data.append(gene_data)
                    gene_data = [line[1:], genome_data[0]]
                    protein_seq_lines = []
                else:
                    protein_seq_lines.append(line)
            f.closed
        hash_string = '{:032X}'.format(int(db_utils.calculate_sequence_hash("".join(protein_seq_lines))))
        gene_data.append(hash_string)
        genes_data.append(gene_data)
        if len(genes_data) != 0:
            cursor.executemany(insert_sql_query, genes_data)
            print (len(genes_data), 'genes imported from genome', genome_data[1])
    create_seed_genes_index(cursor)


#deprecated
def import_seed_genes_tsv(cursor, seed_gene_file):
    sql_query = 'SELECT uid FROM seed_genomes WHERE seed_genome_id IS ?'
    insert_sql_query = 'INSERT INTO seed_genes (seed_gene_id, \
    seed_genome_uid, protein_hash) VALUES  (?, ?, ?)'

    counter = 0
    genes_data = []
    with open(seed_gene_file, 'r') as f:
        for line in f:
            if counter%10000 == 0:
                if len(genes_data) != 0:
                    cursor.executemany(insert_sql_query, genes_data)
                    print (counter, 'genes processed')
                    genes_data = []
            line = line.rstrip()
            line = line.replace("'","''")
            line_tokens = line.split('\t')
            gene_id = line_tokens[0]
            genome_id = line_tokens[1]
            genome_uid = 0
            #check if genome exists in the database
            cursor.execute(sql_query,(genome_id,))
            data=cursor.fetchall()
            if len(data) == 1:
                genome_uid = data[0][0]
                if len(line_tokens) == 5:
                    protein_hash = line_tokens[4]
                    genes_data.append((gene_id, genome_uid, protein_hash))
                else:
                    genes_data.append((gene_id, genome_uid, None))
                counter += 1
            elif len(data) > 1:
                print('Multiple genome entries found for genome %s, gene %s'%(genome_id, gene_id))
                continue

        f.closed
    if len(genes_data) != 0:
        cursor.executemany(insert_sql_query, genes_data)
        print (counter, 'genes imported')
    create_seed_genes_index(cursor)

def import_seed_gene2roles_mapping(cursor, directory, comment):
    insert_sql_statement = 'INSERT INTO seed_gene2role \
        (seed_gene_uid, seed_role_uid, comment) \
        VALUES  (?, ?, ?)'
    roles_sql_query = 'SELECT uid,seed_role_id FROM seed_functional_roles ORDER BY seed_role_id ASC'
    gene_sql_query = 'SELECT uid FROM seed_genes WHERE seed_gene_id IS ?'

    cursor.execute(roles_sql_query)
    roles_list = cursor.fetchall()

    for role_data in roles_list:
        mappings_data = []
        with open(os.path.join(directory,role_data[1] + '_role.txt'), 'r') as f:
            for line in f:
                line = line.rstrip()
                cursor.execute(gene_sql_query,(line,))
                data=cursor.fetchall()
                if len(data) == 1:
                    seed_uid = data[0][0]
                    mappings_data.append((seed_uid, role_data[0], comment))
                elif len(data) > 1:
                    print('More than one entry found for SEED gene %s. Check database integrity.'%(line))
            f.closed
        if len(mappings_data) != 0: 
            cursor.executemany(insert_sql_statement, mappings_data)
            print (role_data[1], ':', len(mappings_data), 'mappings imported')
        else:
            print (role_data[1], ':', 'no mappings found')
    create_seed_genes2roles_index(cursor)

def import_seed_gene2roles_mapping_tsv(cursor, seed_gene2roles_file, comment):
    insert_sql_statement = 'INSERT INTO seed_gene2role \
        (seed_gene_uid, seed_role_uid, comment) \
        VALUES  (?, ?, ?)'
    
    gene_sql_query = 'SELECT uid FROM seed_genes WHERE seed_gene_id IS ?'
    role_sql_query = 'SELECT uid FROM seed_functional_roles WHERE seed_role_id IS ?'
    counter = 0
    mappings_data = []
    
    with open(seed_gene2roles_file, 'r') as f:
        for line in f:
            if counter%10000 == 0:
                if len(mappings_data) != 0: 
                    cursor.executemany(insert_sql_statement, mappings_data)
                    print (counter, 'mappings imported')
                    mappings_data = []
            line = line.rstrip()
            line_tokens = line.split('\t')
            if len(line_tokens) == 2:
                seed_id = line_tokens[0]
                role_id = line_tokens[1]
                # Find SEED gene uid
                cursor.execute(gene_sql_query,(seed_id,))
                data=cursor.fetchall()
                if len(data) == 1:
                    seed_uid = data[0][0]
                    # Find SEED role uid
                    cursor.execute(role_sql_query, (role_id,))
                    data=cursor.fetchall()
                    if len(data) == 1:
                        role_uid = data[0][0]
                        mappings_data.append((seed_uid, role_uid, comment))
                        counter += 1
                    elif len(data) > 1:
                        print('More than one entry found for SEED functional role %s. Check database integrity.'%(role_id))
                elif len(data) > 1:
                    print('More than one entry found for SEED gene %s. Check database integrity.'%(seed_id))
            else:
                print('Parsing error: ', line)
        f.closed
    if len(mappings_data) != 0: 
        cursor.executemany(insert_sql_statement, mappings_data)
        print (counter, 'mappings imported')
    create_seed_genes2roles_index(cursor)

def import_seed_clusters(cursor, seed_gene_file):
    insert_sql_statement = 'INSERT INTO seed_cluster100 \
            (cluster_id, ref_gene, n) VALUES  (?, ?, ?)'
    clusters_n = defaultdict(int)
    clusters_dict = {}
    with open(seed_gene_file, 'r') as f:
        for line in f:
            line = line.strip()
            line_tokens = line.split('\t')
            if len(line_tokens) > 2:
                clusters_dict[line_tokens[2]] = line_tokens[3]
                clusters_n[line_tokens[2]] += 1
        f.closed

    counter = 0
    clusters_data = []
    for cluster_id,rep_id in clusters_dict.iteritems():
        if counter%10000 == 0:
            if len(clusters_data) != 0:
                cursor.executemany(insert_sql_statement, clusters_data)
                print (counter, 'clusters imported')
                clusters_data = []
        counter += 1
        clusters_data.append((cluster_id, rep_id, clusters_n[cluster_id]))
    if len(clusters_data) != 0:
        cursor.executemany(insert_sql_statement, clusters_data)
        print (counter, 'clusters imported')
    create_seed_clusters_index(cursor)
    
    insert_sql_statement = 'INSERT INTO seed_gene2cluster100 \
        (seed_gene_uid, seed_cluster100_uid) VALUES  (?, ?)'
    geneuid_sql_query = 'SELECT uid FROM seed_genes WHERE seed_gene_id IS ?'
    clusteruid_sql_query = 'SELECT uid FROM seed_cluster100 WHERE cluster_id IS ?'
    counter = 0
    clusters_data = []
    with open(seed_gene_file, 'r') as f:
        for line in f:
            if counter%10000 == 0:
                if len(clusters_data) != 0:
                    cursor.executemany(insert_sql_statement, clusters_data)
                    print (counter + 'mappings imported')
                    clusters_data = []
            line = line.rstrip()
            line = line.replace("'","''")
            line_tokens = line.split('\t')
            if len(line_tokens) > 3:
                gene_id = line_tokens[0]
                cluster_id = line_tokens[2]
                #check if the gene exists in the database
                cursor.execute(geneuid_sql_query, (gene_id,))
                data=cursor.fetchall()
                if len(data) == 1:
                    gene_uid = data[0]
                    #check if cluster exists in the database
                    cluster_uid = 0
                    sql_query = ''
                    cursor.execute(clusteruid_sql_query,(cluster_id,))
                    data=cursor.fetchall()
                    if len(data) == 1:
                        cluster_uid = data[0]
                        clusters_data.append((gene_uid, cluster_uid))
                        counter += 1
                    elif len(data) > 1:
                        print('More than one entry found for cluster %s. Check database integrity.'%(cluster_id))
                        sys.exit(1)
                elif len(data) > 1:
                    print('More than one entry found for gene %s. Check database integrity.'%(gene_id))
                    sys.exit(1)
        f.closed
    if len(clusters_data) != 0:
        cursor.executemany(insert_sql_statement, clusters_data)
        print (counter + 'mappings imported')
    create_seed_genes2clusters_index(cursor)

def import_seed2uniref_mappings(cursor, seed_genes2uniref_file):
    uniref_sql_query = 'SELECT uid FROM uniref_proteins WHERE uniref_id IS ?'
    seed_sql_query = 'SELECT uid FROM seed_genes WHERE seed_gene_id IS ?'
    insert_sql_statement = 'INSERT INTO seed2uniref_mappings \
        (seed_uid, uniref_uid, evidence) VALUES  (?, ?, ?)'
    
    display_counter = 0
    counter = 0
    mappings_data = []
    with open(seed_genes2uniref_file, 'r') as f:
        for line in f:
            if counter%10000 == 0:
                if len(mappings_data) != 0:
                    cursor.executemany(insert_sql_statement, mappings_data)
                    print (counter, 'mappings imported')
                    mappings_data = []
            line = line.strip()
            line = line.replace("'","''")
            seed_id, uniref_id, evidence = line.split('\t')
            cursor.execute(seed_sql_query,(seed_id,))
            data=cursor.fetchall()
            if len(data) == 1:
                seed_uid = data[0][0]
                uniref_uid = 0
                cursor.execute(uniref_sql_query,(uniref_id,))
                data=cursor.fetchall()
                if len(data) == 1:
                    mappings_data.append((seed_uid, data[0][0], evidence))
                    counter += 1
                elif len(data) > 1:
                    print('More than one entry found for UniRef gene %s. Check database integrity.'%(uniref_id))
            elif len(data) > 1:
                print('More than one entry found for SEED gene %s. Check database integrity.'%(seed_id))
                
        f.closed
    if len(mappings_data) != 0:
        cursor.executemany(insert_sql_statement, mappings_data)
        print (counter, 'mappings imported')
    create_seed2uniref_index(cursor)

def load_diamond_search_results(cursor,infile, identity_cutoff, mismatch_cutoff):
    uniref_sql_query = 'SELECT uid FROM uniref_proteins WHERE uniref_id IS ?'
    seed_sql_query = 'SELECT seed_genes.uid FROM seed_genes \
        WHERE seed_genes.seed_gene_id IS ? '
    insert_sql_statement = 'INSERT INTO seed2uniref_mappings \
        (seed_uid, uniref_uid, evidence) VALUES  (?, ?, ?)'
    
    seed2uniref_mappings = []
    with open(infile, 'r') as f:
        for line in f.readlines():
            line = line.rstrip('\n\r')
            try: 
                line_tokens = line.split('\t')
                seed_id = line_tokens[0]
                uniref_id = line_tokens[1]
                identity = line_tokens[2]
                mismatches= line_tokens[4]
                if float(identity) >= identity_cutoff:
                    if int(mismatches) <= mismatch_cutoff:
                        cursor.execute(uniref_sql_query, (uniref_id,))
                        data = cursor.fetchall()
                        uniref_uid = 0
                        if len(data) == 1:
                            uniref_uid = data[0][0]
                        elif len(data) > 1:
                            print('ERROR: More than one entry for protein %s: check database integrity'%(uniref_id, ))
                            sys.exit(1)
                        else:
                            continue # skip this row if no such proteins in the UniRef database
                        cursor.execute(seed_sql_query, (seed_id, ))
                        data=cursor.fetchall()
                        if len(data) == 1:
                            seed2uniref_mappings.append((data[0][0], uniref_uid, 'diamond identity ' + identity + '% mismatches ' + mismatches))
                        elif len(data) > 1:
                            print('ERROR: More than one entry for SEED gene %s: check database integrity'%(seed_id))
                            sys.exit(1)
                        else:
                            continue # skip this row if no such genes in the KEGG database
            except  ValueError:
                print ('Error in parsing line \"s%\"'%(line,))
        f.closed
    if len(seed2uniref_mappings) != 0:
        cursor.executemany(insert_sql_statement, seed2uniref_mappings)
    create_seed2uniref_index(cursor)


if __name__=='__main__':
    print ('This module should not be executed as script.')
