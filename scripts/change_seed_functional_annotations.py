#!/usr/bin/python
import sys
import argparse
from context import seed2kegg
from seed2kegg import db_utils
from seed2kegg import data_analysis
from seed2kegg import seed_data_util



def get_args():
    desc = '''This script changes SEED functional role assignments for 
    genes in sqlite database.
    '''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--seed_db', help='SEED sqlite DB path')
    parser.add_argument('--infile', help='File with changes')
    parser.add_argument('--comment', help='Comment for gene to role mappings')
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

    # Prepare database
    print ('Creating gene2role_changes table...')
    seed_data_util.create_gene2role_changes_table(c)

    # Import data
    print ('Verifying input file and making changes in DB...')
    data_analysis.correct_seed_annotations(c, args.infile, args.comment)
    seed_data_util.create_gene2role_changes_indices(c)

    # Write changes and close database
    print ('Saving database...',end='')
    conn.commit()
    conn.close()
    print ('done.')
        
if __name__=='__main__':
    main()
