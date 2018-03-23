#!/usr/bin/python
import sys
import argparse
from context import lib
from lib import db_utils
from lib import seed_data_util
from lib import data_analysis


def get_args():
    desc = '''This script builds a table of SEED genes mapped to KEGG
    genes.
    First, it finds all SEED genes mapped to a UniRef proteins and all KEGG
    genes mapped to that UniRef protein and writes those pairs into the 
    table.
    Second, it imports DIAMOND output for direct comparison of remaining 
    SEED and KEGG proteins.
    Databases for KEGG, SEED and UniRef data would be prepared in advance.'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--seed_db', help='SEED sqlite DB path')
    parser.add_argument('--kegg_db', help='SEED sqlite DB path')
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
    db_utils.attach_local_database(c, args.kegg_db, 'kegg_data')
    db_utils.attach_local_database(c, args.uniref_db, 'uniref_proteins')

    # Prepare database
    print ('Create seed2kegg_mappings table...')
    seed_data_util.create_seed2kegg_mappings_table(c)
    data_analysis.fill_seed2kegg_mappings_table(c, args.diamond_out, 95.0, 5)

    # Write changes and close database
    print ('Saving database...',end='')
    conn.commit()
    conn.close()
    print ('done.')
        
if __name__=='__main__':
    main()
