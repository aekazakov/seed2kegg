#!/usr/bin/python
import sys
import os.path
import sqlite3
import argparse
from context import seed2kegg
from seed2kegg import db_utils
from seed2kegg import kegg_data_util
from seed2kegg import data_analysis


def get_args():
    desc = '''This script builds a table of KO genes mapped to UniRef.
    First, it takes hash values of KEGG genes and looks through list of UniRef 
    proteins for identical values. All found links a stored in the
    kegg2uniref_mappings table.
    Second, it imports DIAMOND output from the search of KEGG proteins 
    in the UniRef DB and adds meaningful homologies into the
    kegg2uniref_mappings table.'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--kegg_db', help='KEGG DB file path')
    parser.add_argument('--uniref_db', help='UniRef DB file path')
    parser.add_argument('--diamond_out', help='Diamond output file path')
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return args

   
def main():
    args = get_args()

    # Open database
    conn = db_utils.connect_local_database(args.kegg_db)
    c = conn.cursor()
    db_utils.attach_local_database(c, args.uniref_db, 'uniref_proteins')

    # Prepare database
    print ('Drop kegg2uniref_mappings table...')
    kegg_data_util.drop_kegg2uniref_mappings_table(c)
    print ('Create kegg2uniref_mappings table...')
    kegg_data_util.create_kegg2uniref_mappings_table(c)

    # Import data
    print ('Find genes with identical hashes...')
    data_analysis.find_kegg2uniref_identical_mappings(c)
    print ('Get genes from DIAMOND output...')
    kegg_data_util.load_diamond_search_results(c,args.diamond_out, 95.0, 5)

    # Write changes and close database
    print ('Saving database...',end='')
    conn.commit()
    conn.close()
    print ('done.')
        
if __name__=='__main__':
    main()
