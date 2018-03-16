#!/usr/bin/python
#import sqlite3
import seed2kegg
import unittest
import store_seed_data

data_dir = '../test_data/'
seed_roles_file = data_dir + 'seed_nr_roles_list.txt'
seed_genome_file = data_dir + 'test_seed_genomes.txt'
seed_gene_file = data_dir + 'test_seed_genes.txt'
uniref_gene_file = data_dir + 'test_uniref_hashes.txt'
seed_gene2roles_file = data_dir + 'test_seed_genes2roles.txt'
seed_genes2uniref_file = data_dir + 'test_seed_genes2uniref.txt'
db_file = data_dir + 'test_seed_mappings.db'

class DataImportTest(unittest.TestCase):

    def setUp(self):
        self.conn = store_seed_data.connect_local_database(db_file)
        self.cursor = self.conn.cursor()
        store_seed_data.create_tables(self.cursor)
        store_seed_data.create_uniref_proteins_indices(self.cursor)
        store_seed_data.create_seed_roles_index(self.cursor)
        store_seed_data.create_seed_genomes_index(self.cursor)
        store_seed_data.create_seed_genes2roles_index(self.cursor)
        store_seed_data.create_seed2uniref_index(self.cursor)
        self.conn.commit()

    def test_uniref_import(self):
        store_seed_data.populate_uniref_proteins_table(self.cursor, uniref_gene_file)
        self.cursor.execute('SELECT protein_hash FROM uniref_proteins WHERE uniref_id IS ?', ('UniRef100_Q46898',))
        self.assertEqual(self.cursor.fetchone()[0], u'4A9096C1420474E03863FC21CBB05CB6')
        self.cursor.execute('SELECT COUNT(*) FROM uniref_proteins')
        self.assertEqual(self.cursor.fetchone()[0], 14)

    def test_seed_role_import(self):
        store_seed_data.populate_seed_functional_roles_table(self.cursor, seed_roles_file)
        self.cursor.execute('SELECT seed_role_name FROM seed_functional_roles WHERE seed_role_id IS ?', ('11016',))
        self.assertEqual(self.cursor.fetchone()[0], u'Respiratory nitrate reductase alpha chain (EC 1.7.99.4)')
        self.cursor.execute('SELECT COUNT(*) FROM seed_functional_roles WHERE seed_role_id IS ?', ('11016',))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed_functional_roles')
        self.assertEqual(self.cursor.fetchone()[0], 12438)

    def test_seed_genome_import(self):
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        self.cursor.execute('SELECT tax_id FROM seed_genomes WHERE seed_genome_id IS ?', ('511145.12',))
        self.assertEqual(self.cursor.fetchone()[0], u'511145')
        self.cursor.execute('SELECT COUNT(*) FROM seed_genomes')
        self.assertEqual(self.cursor.fetchone()[0], 2)

    def test_seed_gene_import(self):
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        store_seed_data.populate_seed_genes_table(self.cursor, seed_gene_file)
        self.cursor.execute('SELECT COUNT(*) FROM seed_genes')
        self.assertEqual(self.cursor.fetchone()[0], 14)

    def test_seed_gene2role_import(self):
        store_seed_data.populate_seed_functional_roles_table(self.cursor, seed_roles_file)
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        store_seed_data.populate_seed_genes_table(self.cursor, seed_gene_file)
        store_seed_data.populate_seed_genes2roles_table(self.cursor, seed_gene2roles_file, 'test')
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role')
        self.assertEqual(self.cursor.fetchone()[0], 6)

    def test_seed2uniref_import(self):
        store_seed_data.populate_uniref_proteins_table(self.cursor, uniref_gene_file)
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        store_seed_data.populate_seed_genes_table(self.cursor, seed_gene_file)
        store_seed_data.populate_seed2uniref_table(self.cursor, seed_genes2uniref_file)
        self.cursor.execute('SELECT COUNT(*) FROM seed2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 14)

    def tearDown(self):
        store_seed_data.drop_tables(self.cursor)
        store_seed_data.drop_indices(self.cursor)
        self.conn.commit()
        self.conn.close()

if __name__=='__main__':
    unittest.main()
