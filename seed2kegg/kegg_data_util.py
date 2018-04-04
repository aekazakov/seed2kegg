#!/usr/bin/python
import os,sys
import sqlite3
from seed2kegg import db_utils

'''
This module contains all the functions for downloading KEGG data and
storing them in a local sqlite database. 
'''
def kegg_dir_is_valid(d):
    retVal = True
    if os.path.exists(d) == False:
        print (d, ' does not exists.')
        retVal = False
    if os.path.isdir(d) == False:
        print (d, ' is not a directory.')
        retVal = False
    if os.path.isfile(os.path.join(d,'kegg_ko_list.txt')) == False:
        print (os.path.join(d,'kegg_ko_list.txt'), ' not found.')
        return False
    if os.path.isfile(os.path.join(d,'kegg_genomes.txt')) == False:
        print (os.path.join(d,'kegg_genomes.txt'), ' not found.')
        retVal = False
    if os.path.isfile(os.path.join(d,'ko_proteins_nr.fasta')) == False:
        print (os.path.join(d,'ko_proteins_nr.fasta'), ' not found.')
        retVal = False
    with open(os.path.join(d,'kegg_ko_list.txt'), 'r') as f:
        for line in f:
            line = line.rstrip()
            line = line.replace("'", "''")
            line_tokens = line.split('\t')
            ko_id = line_tokens[0][3:9]
            if os.path.isfile(os.path.join(d,'ko_' + ko_id + '.txt')) == False:
                print (os.path.join(d,'ko_' + ko_id + '.txt'), ' not found.')
                retVal = False
        f.closed
    return retVal
 
def drop_kegg2uniref_mappings_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS kegg2uniref_mappings')

def drop_kegg_genes2ko_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS kegg_genes2ko')

def drop_kegg_genes_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS kegg_genes')

def drop_kegg_genomes_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS kegg_genomes')

def drop_kegg_orthologs_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS kegg_orthologs')

def drop_indices(cursor):
    cursor.execute('DROP INDEX IF EXISTS kegg_ortholog_ids_1')
    cursor.execute('DROP INDEX IF EXISTS kegg_genome_ids_1')
    cursor.execute('DROP INDEX IF EXISTS kegg_gene_genomeuid_geneidsprimary_1')
    cursor.execute('DROP INDEX IF EXISTS kegg_gene_genome_uids_1')
    cursor.execute('DROP INDEX IF EXISTS kegg_genes2ko_geneuid_1')
    cursor.execute('DROP INDEX IF EXISTS kegg_genes2ko_kouid_1')
    cursor.execute('DROP INDEX IF EXISTS kegg2uniref_keggid_1')
    cursor.execute('DROP INDEX IF EXISTS kegg2uniref_unirefid_1')

def create_kegg_orthologs_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `kegg_orthologs` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`ko_id`	TEXT NOT NULL UNIQUE,\
	`ko_name`	TEXT NOT NULL)')

def create_kegg_genomes_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `kegg_genomes` (\
    `uid` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, \
    `kegg_genome_id`TEXT NOT NULL UNIQUE, \
    `kegg_genome_name` TEXT NOT NULL, \
    `ncbi_tax` TEXT, \
    `genome_lineage` TEXT, \
    `kingdom` TEXT)')

def create_kegg_genes_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `kegg_genes` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`kegg_gene_id_primary`	TEXT NOT NULL,\
	`kegg_gene_id_full`	TEXT NOT NULL,\
	`kegg_genome_uid`	INTEGER NOT NULL,\
	`kegg_seq_hash`	TEXT, \
    UNIQUE (kegg_gene_id_full, kegg_genome_uid) ON CONFLICT REPLACE)')

def create_kegg_genes2ko_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `kegg_genes2ko` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`kegg_gene_uid`	INTEGER NOT NULL,\
	`kegg_ko_uid`	INTEGER NOT NULL)')

def create_kegg2uniref_mappings_table(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS `kegg2uniref_mappings` (\
	`uid`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,\
	`kegg_uid`	INTEGER NOT NULL,\
	`uniref_uid`	INTEGER NOT NULL,\
	`evidence`	TEXT NOT NULL)')

def create_tables(cursor):
    create_kegg_orthologs_table(cursor)
    create_kegg_genomes_table(cursor)
    create_kegg_genes_table(cursor)
    create_kegg_genes2ko_table(cursor)
    create_kegg2uniref_mappings_table(cursor)

def create_kegg_orthologs_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX `kegg_ortholog_ids_1` \
    ON `kegg_orthologs` (`ko_id` ASC)')

def create_kegg_genomes_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX `kegg_genome_ids_1` \
    ON `kegg_genomes` (`kegg_genome_id` ASC)')

def create_kegg_genes_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX `kegg_gene_genomeuid_geneidsprimary_1` \
    ON `kegg_genes` (`kegg_genome_uid` ASC, `kegg_gene_id_primary` ASC)')
    cursor.execute('CREATE INDEX `kegg_gene_genomeuids_1` \
    ON `kegg_genes` (`kegg_genome_uid` ASC)')

def create_kegg_genes2ko_index(cursor):
    cursor.execute('CREATE INDEX `kegg_genes2ko_geneuid_1` ON `kegg_genes2ko` (`kegg_gene_uid` ASC)')
    cursor.execute('CREATE INDEX `kegg_genes2ko_kouid_1` ON `kegg_genes2ko` (`kegg_ko_uid` ASC)')

def create_kegg2uniref_mappings_index(cursor):
    cursor.execute('CREATE UNIQUE INDEX `kegg2uniref_keggid_1` ON `kegg2uniref_mappings` (`kegg_uid` ASC)')
    cursor.execute('CREATE INDEX `kegg2uniref_unirefid_1` ON `kegg2uniref_mappings` (`uniref_uid` ASC)')

def get_ko_id(cursor,uid):
    cursor.execute('SELECT ko_id FROM kegg_orthologs WHERE uid=?',(uid,))
    return cursor.fetchone()[0]


def import_kegg_orthologs_list(cursor,ko_file):
    sql_query = 'INSERT INTO kegg_orthologs(ko_id,ko_name) VALUES (?, ?)'
    ko_data = []
    with open(ko_file, 'r') as f:
        for line in f:
            line = line.rstrip()
            line = line.replace("'", "''")
            ko_id, ko_name = line.split('\t')
            ko_id = ko_id[3:9]
            ko_data.append((ko_id, ko_name))
    f.closed
    cursor.executemany(sql_query, ko_data)
    create_kegg_orthologs_index(cursor)
 
def import_kegg_genomes_list (cursor, genome_file):
    sql_query = 'INSERT INTO kegg_genomes(kegg_genome_id,\
                kegg_genome_name, ncbi_tax, genome_lineage, kingdom) \
                VALUES (?, ?, ?, ?, ?)'
    kegg_genome_data = []
    with open(genome_file, 'r') as f:
        for line in f:
            line = line.rstrip()
            line = line.replace("'", "''")
            #print line
            line_tokens = line.split('\t')
            if len(line_tokens) == 5:
                kegg_genome_data.append(line_tokens)
            else:
                print ('Line \"s%\" has unexpected number of fields'%(line,))
    f.closed
    cursor.executemany(sql_query, kegg_genome_data)
    create_kegg_genomes_index(cursor)
    print (len(kegg_genome_data), ' KEGG genomes processed')

def import_kegg_genes(cursor, genes_file):
    insert_sql_query = 'INSERT INTO kegg_genes \
        (kegg_gene_id_primary,kegg_gene_id_full, kegg_genome_uid, kegg_seq_hash) \
        VALUES  (?, ?, ?, ?)'
    sql_query = 'SELECT uid FROM kegg_genomes WHERE kegg_genome_id IS ?'
    counter = 0
    protein_seq_lines = []
    genome_id = ''
    gene_id_primary = ''
    gene_id_full = ''
    gene_data = []
    batch_data = []
    with open(genes_file, 'r') as f:
        for line in f:
            line = line.rstrip()
            if line.startswith('>'):
                #calculate protein hash and store append it to gene_data
                if gene_data != []:
                    hash_string = '{:032X}'.format(int(db_utils.calculate_sequence_hash("".join(protein_seq_lines))))
                    gene_data.append(hash_string)
                    batch_data.append(gene_data)
                gene_data = []
                protein_seq_lines = []
                # load batch into database, every 10K entries
                if counter%10000 == 0:
                    if len(batch_data) != 0:
                        cursor.executemany(insert_sql_query, batch_data)
                        print (counter, ' genes processed')
                        batch_data = []
                # find genome_id, gene_id and put them into gene_data
                line = line[1:]
                line = line.replace("'","''")
                line_tokens = line.split(' ')
                genome_id,gene_id_primary = line_tokens[0].split(':')
                gene_data.extend((gene_id_primary, line))
                genome_uid = 0
                #check if the genome exists in the database
                cursor.execute(sql_query, (genome_id,))
                data=cursor.fetchone()
                if data is None:
                    print('Skipping gene %s: There is no genome named %s'%(gene_id_full,genome_id))
                    gene_data = []
                else:
                    genome_uid = data[0]
                    gene_data.append(genome_uid)
                counter += 1
            else:
                protein_seq_lines.append(line)
        f.closed
    if gene_data != []:
        hash_string = '{:032X}'.format(int(db_utils.calculate_sequence_hash("".join(protein_seq_lines))))
        gene_data.append(hash_string)
        batch_data.append(gene_data)
    if len(batch_data) != 0:
        cursor.executemany(insert_sql_query, batch_data)
    print (counter, ' genes processed')
    create_kegg_genes_index(cursor)

def import_genes2ko_mappings(cursor, kegg_directory):
    insert_sql_query = 'INSERT INTO kegg_genes2ko(\
        kegg_gene_uid,kegg_ko_uid) VALUES (?, ?)'
    gene_sql_query = 'SELECT kegg_genes.uid FROM kegg_genes JOIN \
        kegg_genomes ON kegg_genes.kegg_genome_uid = kegg_genomes.uid \
        WHERE kegg_genomes.kegg_genome_id IS ? \
        AND kegg_genes.kegg_gene_id_primary IS ? '
    ko_list_sql_query = 'SELECT uid,ko_id FROM kegg_orthologs' 
    
    gene2ko_mappings = []
    genes_flag = False
    
    cursor.execute(ko_list_sql_query)
    ko_list_data = cursor.fetchall()
    for ko_item in ko_list_data:
        with open(os.path.join(kegg_directory,'ko_' + ko_item[1] + '.txt'), 'r') as f:
            for line in f:
                if line.startswith('GENES      '):
                    genes_flag = True
                elif line.startswith('///'):
                    genes_flag = False
                elif line.startswith('REFERENCE  '):
                    genes_flag = False
                if genes_flag is True:
                    line = line.rstrip()            
                    line = line[12:]
                    try: 
                        [genome_id, genes_str] = line.split(': ')
                        genome_id = genome_id.lower()
                        genes_list = genes_str.split(' ')
                        for gene in genes_list:
                            gene_id = gene.split('(')[0]
                            cursor.execute(gene_sql_query, (genome_id, gene_id))
                            data=cursor.fetchall()
                            if len(data) == 1:
                                gene2ko_mappings.append((data[0][0], ko_item[0]))
                            elif len(data) > 1:
                                print('ERROR: More than one entry for gene %s from genome %s: check database integrity'%(gene_id, genome_id))
                                sys.exit(1)
                    except ValueError:
                        print ('Error in parsing line \"', line, '\" from ', ko_item[1])

            f.closed
        if len(gene2ko_mappings) != 0:
            cursor.executemany(insert_sql_query, gene2ko_mappings)
            gene2ko_mappings = []
        print ('KEGG Orthology group %s imported'%(ko_item[1],))
    create_kegg_genes2ko_index(cursor)

def load_diamond_search_results(cursor,infile, identity_cutoff, mismatch_cutoff):
    ''' This function reads DIAMOND ouput and selects meaningful hits
    of KEGG protein aligned to UniRef. Good hits must have identity at or 
    given threshold AND number of mismatches higher than the given 
    mismatch_cutoff. There are two cutoffs because a pair of short 
    proteins may have low overall identity but still be true orthologs.
    '''
    uniref_sql_query = 'SELECT uid FROM uniref_proteins WHERE uniref_id IS ?'
    kegg_sql_query = 'SELECT kegg_genes.uid FROM kegg_genes JOIN \
        kegg_genomes ON kegg_genes.kegg_genome_uid = kegg_genomes.uid \
        WHERE kegg_genomes.kegg_genome_id IS ? \
        AND kegg_genes.kegg_gene_id_primary IS ? '
    insert_sql_statement = 'INSERT INTO kegg2uniref_mappings \
        (kegg_uid, uniref_uid, evidence) VALUES  (?, ?, ?)'
    
    kegg2uniref_mappings = []
    with open(infile, 'r') as f:
        for line in f.readlines():
            line = line.rstrip('\n\r')
            try: 
                line_tokens = line.split('\t')
                kegg_unique_id = line_tokens[0]
                uniref_id = line_tokens[1]
                identity = line_tokens[2]
                mismatches= line_tokens[4]
                kegg_genome,kegg_gene = kegg_unique_id.split(':')
                if float(identity) < identity_cutoff:
                    if int(mismatches) > mismatch_cutoff:
                        continue # skip if identity too low AND too many mismatches
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
                cursor.execute(kegg_sql_query, (kegg_genome, kegg_gene))
                data=cursor.fetchall()
                if len(data) == 1:
                    kegg2uniref_mappings.append((data[0][0], uniref_uid, 'diamond identity ' + identity + '% mismatches ' + mismatches))
                elif len(data) > 1:
                    print('ERROR: More than one entry for gene %s from genome %s: check database integrity'%(gene_id, genome_id))
                    sys.exit(1)
                else:
                    continue # skip this row if no such genes in the KEGG database
            except  ValueError:
                print ('Error in parsing line \"s%\"'%(line,))
        f.closed
    if len(kegg2uniref_mappings) != 0:
        cursor.executemany(insert_sql_statement, kegg2uniref_mappings)
    create_kegg2uniref_mappings_index(cursor)


if __name__=='__main__':
    print ('This module should not be executed as script.')

