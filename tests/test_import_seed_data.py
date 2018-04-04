#!/usr/bin/python
#import sqlite3
import os
import unittest
from context import seed2kegg
from seed2kegg import db_utils
from seed2kegg import seed_data_util
from seed2kegg import uniref_data_util
from seed2kegg import data_analysis


data_dir = '../test_data/'
seed_roles_file = os.path.join(data_dir, 'test_seed_roles_list.txt')
seed_genome_file = os.path.join(data_dir, 'test_seed_genomes.txt')
seed_gene_dir = os.path.join(data_dir, 'test_seed_dir')
seed_gene2roles_dir = os.path.join(data_dir, 'test_seed_dir')
seed_diamond_file = os.path.join(data_dir, 'test_diamond_output_seed.txt')

seed_genes2uniref_file = os.path.join(data_dir, 'test_seed_genes2uniref.txt')
db_file = os.path.join(data_dir, 'test_seed.db')
# Accessory files for testing SEED-to-UniRef mappings import
uniref_fasta_file = os.path.join(data_dir, 'test_uniref.faa')
uniref_db_file = os.path.join(data_dir, 'test_kegg_mappings_uniref.db')


class DataImportTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(db_file)
        self.cursor = self.conn.cursor()
        db_utils.attach_local_database(self.cursor, uniref_db_file, 'uniref_proteins')

    def test_create_tables(self):
        seed_data_util.create_tables(self.cursor)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','seed_functional_roles'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed_functional_roles')
        self.assertEqual(self.cursor.fetchone()[0], 0)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','seed_genomes'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed_genomes')
        self.assertEqual(self.cursor.fetchone()[0], 0)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','seed_genes'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed_genes')
        self.assertEqual(self.cursor.fetchone()[0], 0)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','seed_gene2role'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role')
        self.assertEqual(self.cursor.fetchone()[0], 0)


    def test_import_seed_functional_roles_table(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_functional_roles_table(self.cursor,seed_roles_file)
        
        self.cursor.execute('SELECT uid FROM seed_functional_roles WHERE seed_role_id IS ?', ('9724',))
        self.assertEqual(self.cursor.fetchone()[0], 6)
        self.cursor.execute('SELECT COUNT(*) FROM seed_functional_roles WHERE seed_role_id IS ?', ('9724',))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed_functional_roles')
        self.assertEqual(self.cursor.fetchone()[0], 6)

    def test_import_seed_genomes(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        
        self.cursor.execute('SELECT tax_id FROM seed_genomes WHERE seed_genome_id IS ?', ('511145.12',))
        self.assertEqual(self.cursor.fetchone()[0], u'511145')
        self.cursor.execute('SELECT COUNT(*) FROM seed_genomes')
        self.assertEqual(self.cursor.fetchone()[0], 4)

    def test_import_seed_genes(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        self.cursor.execute('SELECT COUNT(*) FROM seed_genes')
        self.assertEqual(self.cursor.fetchone()[0], 8)

    def test_import_seed_gene2roles_mapping(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_functional_roles_table(self.cursor,seed_roles_file)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        seed_data_util.import_seed_gene2roles_mapping(self.cursor, seed_gene2roles_dir, 'test')

        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role')
        self.assertEqual(self.cursor.fetchone()[0], 6)

    def test_find_seed2uniref_identical_mappings(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.create_seed2uniref_mappings_table(self.cursor)
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)

        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        data_analysis.find_seed2uniref_identical_mappings(self.cursor)
        
        self.cursor.execute('SELECT COUNT(*) FROM seed2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 2)

    def test_load_diamond_search_results(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.create_seed2uniref_mappings_table(self.cursor)
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)

        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        seed_data_util.load_diamond_search_results(self.cursor,seed_diamond_file, 95.0, 5)
        
        self.cursor.execute('SELECT COUNT(*) FROM seed2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 3)

    def test_import_seed2uniref_mappings(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.create_seed2uniref_mappings_table(self.cursor)
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)

        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        seed_data_util.import_seed2uniref_mappings(self.cursor, seed_genes2uniref_file)
        
        self.cursor.execute('SELECT COUNT(*) FROM seed2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 5)


    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(db_file)
        os.remove(uniref_db_file)


if __name__=='__main__':
    unittest.main()
