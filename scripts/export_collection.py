#!/usr/bin/python
import sys
import argparse
from context import seed2kegg
from seed2kegg import db_utils
from seed2kegg import data_analysis

def get_args():
    desc = 'This script creates FASTA file with proteins from functional collection.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--seed_db', help='SEED sqlite DB path')
    parser.add_argument('--kegg_db', help='SEED sqlite DB path')
    parser.add_argument('--kegg_prots', help='KEGG proteins in FASTA format')
    parser.add_argument('--seed_prots', help='SEED proteins in FASTA format')
    parser.add_argument('--name', help='Collection name')
    parser.add_argument('--ver', help='Collection version')
    parser.add_argument('--outfile', help='Output file name')
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
    print ('Finding genes...')
    gene_collection = data_analysis.make_collection_gene_list(c, args.name, args.ver)
    print(len(gene_collection), 'genes found. Writing output...')
    data_analysis.export_collection_proteins(gene_collection,args.seed_prots,args.outfile)
    data_analysis.export_collection_proteins(gene_collection,args.kegg_prots,args.outfile)
    conn.close()
    print ('done.')
        
if __name__=='__main__':
    main()
