#!/usr/bin/python
import sys
import argparse
from context import lib
from lib import db_utils
from lib import data_analysis


def get_args():
    desc = '''This script creates a collection of functions.
    Databases with KEGG and SEED data would be prepared in advance.'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--seed_db', help='SEED sqlite DB path')
    parser.add_argument('--kegg_db', help='SEED sqlite DB path')
    parser.add_argument('--infile', help='List of functions for collection (tsv)')
    parser.add_argument('--name', help='Collection name')
    parser.add_argument('--ver', help='Collection version')
    parser.add_argument('--info', help='Collection info')
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return args

   
def main():
    args = get_args()

    # Open database
    conn = db_utils.connect_local_database(args.seed_db)
    c = conn.cursor()
    db_utils.attach_local_database(c, args.kegg_db, 'kegg_data')
    db_utils.create_collections_table(c)
    db_utils.create_collection2function_table(c)
    data_analysis.import_collection_tsv(c, args.infile, args.name, args.info, args.ver)
    conn.commit()
    conn.close()
    print ('done.')
        
if __name__=='__main__':
    main()
