#!/usr/bin/python
import sys
import argparse
from context import lib
from lib import db_utils
from lib import data_analysis


def get_args():
    desc = 'This script generates list of KEGG proteins not mapped to SEED DB.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--seed_db', help='SEED sqlite DB path')
    parser.add_argument('--kegg_db', help='SEED sqlite DB path')
    parser.add_argument('--infile', help='KEGG proteins in FASTA format')
    parser.add_argument('--outfile', help='Output file name')
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
    db_utils.attach_local_database(c, args.seed_db, 'seed_data')
    print ('Finding unmapped proteins...')
    data_analysis.export_kegg_unmapped_proteins(c, args.infile, args.outfile)
    conn.close()
    print ('done.')
        
if __name__=='__main__':
    main()
