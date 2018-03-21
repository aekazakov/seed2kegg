#!/usr/bin/python
import sys
import os.path
import sqlite3
import argparse
from context import lib
from lib import db_utils
from lib import uniref_data_util


def get_args():
    desc = 'This scipts imports data from UniRef FASTA file into local \
    sqlite database.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--fasta', help='UniRef FASTA file')
    parser.add_argument('--db', help='Output DB file name')
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return args

def main():
    args = get_args()
    
    conn = db_utils.connect_local_database(args.db)
    c = conn.cursor()
    
    uniref_data_util.drop_tables(c)
    uniref_data_util.drop_indices(c)
    uniref_data_util.create_uniref_proteins_table(c)
    uniref_data_util.import_uniref_fasta(c,args.fasta)
    uniref_data_util.create_uniref_proteins_indices(c)
    
    conn.commit()
    conn.close()
        
if __name__=='__main__':
    main()
