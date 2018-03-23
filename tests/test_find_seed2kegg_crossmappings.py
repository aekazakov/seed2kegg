#!/usr/bin/python
import os
import unittest
from context import lib
from lib import db_utils
from lib import uniref_data_util
from lib import kegg_data_util
from lib import seed_data_util
from lib import data_analysis

data_dir = '../test_data/'
test_kegg_dir = os.path.join(data_dir, 'test_kegg_dir')
uniref_fasta_file = os.path.join(data_dir, 'test_uniref.faa')
kegg_diamond_output = os.path.join(data_dir, 'test_diamond_output_kegg.txt')
seed_genome_file = os.path.join(data_dir, 'test_seed_genomes.txt')
seed_gene_dir = os.path.join(data_dir, 'test_seed_dir')
seed_diamond_output = os.path.join(data_dir, 'test_diamond_output_seed.txt')
seed2kegg_diamond_output = os.path.join(data_dir, 'test_diamond_output_seed2kegg.txt')
db_file = os.path.join(data_dir, 'test_seed.db')


data_dir = '../test_data/'


class DataUpdateTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(db_file)
        self.cursor = self.conn.cursor()

    def test_create_seed2kegg_mappings_table(self):
        seed_data_util.create_seed2kegg_mappings_table(self.cursor)
        seed_data_util.create_seed2kegg_mappings_index(self.cursor)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','seed2kegg_mappings'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed2kegg_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_fill_seed2kegg_mappings_table(self):
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        seed_data_util.create_seed2uniref_mappings_table(self.cursor)
        seed_data_util.load_diamond_search_results(self.cursor,seed_diamond_output, 95.0, 5)
        self.cursor.execute('SELECT COUNT(*) FROM seed2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 3)
        kegg_data_util.create_kegg_genomes_table(self.cursor)
        kegg_data_util.import_kegg_genomes_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_genomes.txt'))
        kegg_data_util.create_kegg_genes_table(self.cursor)
        kegg_data_util.import_kegg_genes(self.cursor, os.path.join(test_kegg_dir, 'ko_proteins_nr.fasta'))
        kegg_data_util.create_kegg2uniref_mappings_table(self.cursor)
        kegg_data_util.load_diamond_search_results(self.cursor,kegg_diamond_output, 95.0, 5)
        self.cursor.execute('SELECT COUNT(*) FROM kegg2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 2)

        seed_data_util.create_seed2kegg_mappings_table(self.cursor)
        data_analysis.fill_seed2kegg_mappings_table(self.cursor, seed2kegg_diamond_output, 95.0, 5)
        self.cursor.execute('SELECT COUNT(*) FROM seed2kegg_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 4)

    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(db_file)


if __name__=='__main__':
    unittest.main()
