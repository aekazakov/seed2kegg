#!/usr/bin/python
import os
import sqlite3
import unittest
from context import lib
from lib import db_utils
from lib import seed_data_util
from lib import uniref_data_util
from lib import data_analysis


data_dir = '../test_data/'
test_seed_dir = os.path.join(data_dir, 'test_seed_dir')
uniref_fasta_file = os.path.join(data_dir, 'test_uniref.faa')
diamond_output = os.path.join(data_dir, 'test_diamond_output_seed.txt')
seed_db_file = os.path.join(data_dir,'test_seed_mappings.db')
uniref_db_file = os.path.join(data_dir,'test_seed_mappings_uniref.db')

class DataImportTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(seed_db_file)
        self.cursor = self.conn.cursor()
        seed_data_util.create_seed_genomes_table(self.cursor)
        seed_data_util.import_seed_genomes(self.cursor, os.path.join(data_dir, 'test_seed_genomes.txt'))
        seed_data_util.create_seed_genes_table(self.cursor)
        seed_data_util.import_seed_genes(self.cursor, test_seed_dir)
        db_utils.attach_local_database(self.cursor, uniref_db_file, 'uniref_proteins')
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)

    def test_find_seed2uniref_identical_mappings(self):
        seed_data_util.create_seed2uniref_mappings_table(self.cursor)
        data_analysis.find_seed2uniref_identical_mappings(self.cursor)
        self.cursor.execute('SELECT COUNT(*) FROM seed2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 2)
    
    def test_load_diamond_search_results(self):
        seed_data_util.create_seed2uniref_mappings_table(self.cursor)
        seed_data_util.load_diamond_search_results(self.cursor,diamond_output, 95.0, 5)
        self.cursor.execute('SELECT COUNT(*) FROM seed2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 3)

    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(seed_db_file)
        os.remove(uniref_db_file)

if __name__=='__main__':
    unittest.main()
