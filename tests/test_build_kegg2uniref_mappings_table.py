#!/usr/bin/python
import os
import sqlite3
import unittest
from context import lib
from lib import db_utils
from lib import kegg_data_util
from lib import uniref_data_util
from lib import data_analysis


data_dir = '../test_data/'
test_kegg_dir = os.path.join(data_dir, 'test_kegg_dir')
uniref_fasta_file = os.path.join(data_dir, 'test_uniref.faa')
diamond_output = os.path.join(data_dir, 'test_diamond_output_kegg.txt')
kegg_db_file = 'test_kegg_mappings.db'
uniref_db_file = 'test_kegg_mappings_uniref.db'

class DataImportTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(os.path.join(data_dir, kegg_db_file))
        self.cursor = self.conn.cursor()
        db_utils.attach_local_database(self.cursor, os.path.join(data_dir, uniref_db_file), 'uniref_proteins')
        kegg_data_util.create_kegg_genomes_table(self.cursor)
        kegg_data_util.import_kegg_genomes_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_genomes.txt'))
        kegg_data_util.create_kegg_genes_table(self.cursor)
        kegg_data_util.import_kegg_genes(self.cursor, os.path.join(test_kegg_dir, 'ko_proteins_nr.fasta'))
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)

    def test_find_kegg2uniref_identical_mappings(self):
        kegg_data_util.create_kegg2uniref_mappings_table(self.cursor)
        data_analysis.find_kegg2uniref_identical_mappings(self.cursor)
        self.cursor.execute('SELECT COUNT(*) FROM kegg2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 2)
    
    def test_load_diamond_search_results(self):
        kegg_data_util.create_kegg2uniref_mappings_table(self.cursor)
        kegg_data_util.load_diamond_search_results(self.cursor,diamond_output, 95.0, 5)
        self.cursor.execute('SELECT COUNT(*) FROM kegg2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 2)

    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(os.path.join(data_dir, kegg_db_file))
        os.remove(os.path.join(data_dir, uniref_db_file))

if __name__=='__main__':
    unittest.main()
