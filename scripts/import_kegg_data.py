#!/usr/bin/python
import sys
import os.path
import sqlite3
import argparse
from context import seed2kegg
from seed2kegg import db_utils
from seed2kegg import kegg_data_util


def get_args():
    desc = '''This scipts imports KEGG data from flat files into local
    sqlite database.
    All KEGG files should be in one directory.
    The directory must contain a list of all KOs in kegg_ko_list.txt,
    a list of all KEGG genomes in kegg_genomes.txt,
    protein sequences of all KEGG proteins in ko_proteins_nr.fasta,
    and flat files of all KO entries with names like ko_K00001.txt .'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--kegg_dir', help='Directory with KEGG flat files')
    parser.add_argument('--db', help='Output DB file name')
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return args

   
def main():
    args = get_args()
    
    # Check if KEGG data directory contains all required files
    if kegg_data_util.kegg_dir_is_valid(args.kegg_dir) == False:
        print ('Some required files are missing from ', args.kegg_dir, '. Data import failed.')
        sys.exit(1)
    
    conn = db_utils.connect_local_database(args.db)
    c = conn.cursor()
    
    # Prepare database
    print ('Drop genes2ko table...')
    kegg_data_util.drop_kegg_genes2ko_table(c)
    print ('Drop KEGG genes table...')
    kegg_data_util.drop_kegg_genes_table(c)
    print ('Drop KEGG genomes table...')
    kegg_data_util.drop_kegg_genomes_table(c)
    print ('Drop KEGG orthologs table...')
    kegg_data_util.drop_kegg_orthologs_table(c)
    print ('Drop database indices...')
    kegg_data_util.drop_indices(c)
    print ('Create KEGG orthologs table...')
    kegg_data_util.create_kegg_orthologs_table(c)
    print ('Create KEGG genomes table...')
    kegg_data_util.create_kegg_genomes_table(c)
    print ('Create KEGG genes table...')
    kegg_data_util.create_kegg_genes_table(c)
    print ('Create genes2ko table...')
    kegg_data_util.create_kegg_genes2ko_table(c)

    # Import data
    kegg_data_util.import_kegg_orthologs_list(c, os.path.join(args.kegg_dir,'kegg_ko_list.txt'))
    kegg_data_util.import_kegg_genomes_list(c, os.path.join(args.kegg_dir,'kegg_genomes.txt'))
    kegg_data_util.import_kegg_genes(c, os.path.join(args.kegg_dir,'ko_proteins_nr.fasta'))
    kegg_data_util.import_genes2ko_mappings(c, args.kegg_dir)
   
    conn.commit()
    conn.close()
        
if __name__=='__main__':
    main()
