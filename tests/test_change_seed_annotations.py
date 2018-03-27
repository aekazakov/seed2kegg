#!/usr/bin/python
import os
import unittest
from context import lib
from lib import db_utils
from lib import seed_data_util
from lib import data_analysis

data_dir = '../test_data/'
seed_roles_file = os.path.join(data_dir, 'test_seed_roles_list.txt')
seed_genome_file = os.path.join(data_dir, 'test_seed_genomes.txt')
seed_gene_dir = os.path.join(data_dir, 'test_seed_dir')
seed_gene2roles_dir = os.path.join(data_dir, 'test_seed_dir')
seed_update_file = os.path.join(data_dir, 'test_seed_update.txt')
db_file = os.path.join(data_dir, 'test_seed.db')


class DataUpdateTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(db_file)
        self.cursor = self.conn.cursor()

    def test_get_role_uid(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_functional_roles_table(self.cursor,seed_roles_file)
        self.assertEqual(seed_data_util.get_role_uid(self.cursor, '105'), 1)
        self.assertEqual(seed_data_util.get_role_uid(self.cursor, '9724'), 6)

    def test_get_gene_uid(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        self.assertEqual(seed_data_util.get_gene_uid(self.cursor, 'fig|511145.12.peg.2823'), 3)

    def test_check_gene_to_role_link(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_functional_roles_table(self.cursor,seed_roles_file)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        seed_data_util.import_seed_gene2roles_mapping(self.cursor, seed_gene2roles_dir, 'test')
        self.assertTrue(seed_data_util.check_gene_to_role_link(self.cursor, seed_data_util.get_gene_uid(self.cursor, 'fig|511145.12.peg.2823'), '911'))
        self.assertFalse(seed_data_util.check_gene_to_role_link(self.cursor, seed_data_util.get_gene_uid(self.cursor, 'fig|511145.12.peg.3843'), '911'))
    
    def test_create_gene2role_changes_table(self):
        seed_data_util.create_gene2role_changes_table(self.cursor)
        seed_data_util.create_gene2role_changes_indices(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','seed_gene2role_changes'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role_changes')
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_correct_seed_annotations(self):
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_functional_roles_table(self.cursor,seed_roles_file)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        seed_data_util.import_seed_gene2roles_mapping(self.cursor, seed_gene2roles_dir, 'test')
        seed_data_util.create_gene2role_changes_table(self.cursor)
        seed_data_util.create_gene2role_changes_indices(self.cursor)

        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role_changes')
        self.assertEqual(self.cursor.fetchone()[0], 0)
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role')
        self.assertEqual(self.cursor.fetchone()[0], 6)

        data_analysis.correct_seed_annotations(self.cursor, seed_update_file, 'test')

        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role_changes')
        self.assertEqual(self.cursor.fetchone()[0], 5)
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role')
        self.assertEqual(self.cursor.fetchone()[0], 7)

    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(db_file)


if __name__=='__main__':
    unittest.main()
