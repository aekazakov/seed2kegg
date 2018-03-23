#!/usr/bin/python
import sys
import os.path
import sqlite3
import argparse
from context import lib
from lib import db_utils
from lib import seed_data_util
from lib import data_analysis


def get_args():
    desc = '''This script builds a table of SEED genes mapped to UniRef.
    First, it compares hash values of SEED proteins with those of UniRef 
    proteins and finds identical sequences. All found links are stored in the
    seed2uniref_mappings table.
    Second, it imports DIAMOND output from the search of SEED proteins 
    in the UniRef DB and adds high homologies into the
    seed2uniref_mappings table.'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--seed_db', help='SEED sqlite DB path')
    parser.add_argument('--uniref_db', help='UniRef sqlite DB path')
    parser.add_argument('--diamond_out', help='Diamond output file path')
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
    db_utils.attach_local_database(c, args.uniref_db, 'uniref_proteins')

    # Prepare database
    print ('Drop seed2uniref_mappings table...')
    seed_data_util.drop_seed2uniref_mappings_table(c)
    print ('Create seed2uniref_mappings table...')
    seed_data_util.create_seed2uniref_mappings_table(c)

    # Import data
    print ('Find genes with identical hashes...')
    data_analysis.find_seed2uniref_identical_mappings(c)
    print ('Get genes from DIAMOND output...')
    seed_data_util.load_diamond_search_results(c,args.diamond_out, 95.0, 5)

    # Write changes and close database
    print ('Saving database...',end='')
    conn.commit()
    conn.close()
    print ('done.')
        
if __name__=='__main__':
    main()
