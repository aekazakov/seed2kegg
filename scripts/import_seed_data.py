#!/usr/bin/python
import sys
import os.path
import sqlite3
import argparse
from context import lib
from lib import db_utils
from lib import seed_data_util


def get_args():
    desc = '''This scipts imports SEED data from flat files into local
    sqlite database.
    All flat files for SEED functional roles should be in one directory.
    All FASTA files for SEED proteomes should be in one directory.
    
    The directory must contain a list of all KOs in kegg_ko_list.txt,
    a list of all KEGG genomes in kegg_genomes.txt,
    protein sequences of all KEGG proteins in ko_proteins_nr.fasta,
    and flat files of all KO entries with names like ko_K00001.txt .'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--seed_roles_dir', help='Directory with files of gene lists\
    for SEED functional roles.')
    parser.add_argument('--seed_prot_dir', help='Directory with FASTA files \
    of SEED complete proteomes.')
    parser.add_argument('--seed_genome_file', help='File with list of SEED genomes.')
    parser.add_argument('--seed_roles_file', help='File with list of \
    SEED functional roles.')
    parser.add_argument('--db', help='Output DB file name')
    parser.add_argument('--comment', help='Comment for gene to role mappings')
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    return args

   
def main():
    args = get_args()
    
    # Open database and prepare tables
    conn = db_utils.connect_local_database(args.db)
    c = conn.cursor()    
    seed_data_util.drop_tables(c)
    seed_data_util.drop_all_indices(c)
    seed_data_util.create_tables(c)

    # Import data
    seed_data_util.import_seed_functional_roles_table(c,args.seed_roles_file)
    seed_data_util.import_seed_genomes(c,args.seed_genome_file)
    seed_data_util.import_seed_genes(c,args.seed_prot_dir)
    seed_data_util.import_seed_gene2roles_mapping(c,args.seed_roles_dir,args.comment)
    
    # Save changes and close database   
    conn.commit()
    conn.close()
        
if __name__=='__main__':
    main()
