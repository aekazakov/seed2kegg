#!/usr/bin/python
import sqlite3
import argparse
import db_utils
import uniref_data_util

def main():

    desc = """This scipts imports data from UniRef FASTA file into local 
    sqlite database."""
    
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--fasta', help='UniRef FASTA file')
    parser.add_argument('--db', help='Output DB file name')
    
    if len(sys.argv) == 1:
        parser.print_help()
        exit(0)
    
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
