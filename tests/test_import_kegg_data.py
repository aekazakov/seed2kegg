#!/usr/bin/python
import os
import sqlite3
import unittest
from context import seed2kegg
from seed2kegg import db_utils
from seed2kegg import kegg_data_util

data_dir = '../test_data/'
test_kegg_dir = '../test_data/test_kegg_dir/'
db_file = 'test_kegg.db'

class DataImportTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(os.path.join(data_dir, db_file))
        self.cursor = self.conn.cursor()
    
    def test_kegg_dir_is_valid(self):
        self.assertTrue(kegg_data_util.kegg_dir_is_valid(test_kegg_dir))

    def test_create_tables(self):
        kegg_data_util.create_tables(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_orthologs'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM kegg_orthologs')
        self.assertEqual(self.cursor.fetchone()[0], 0)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genomes'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM kegg_genomes')
        self.assertEqual(self.cursor.fetchone()[0], 0)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genes'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM kegg_genes')
        self.assertEqual(self.cursor.fetchone()[0], 0)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genes2ko'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM kegg_genes2ko')
        self.assertEqual(self.cursor.fetchone()[0], 0)

        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg2uniref_mappings'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM kegg2uniref_mappings')
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_drop_tables(self):
        kegg_data_util.create_tables(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_orthologs'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genomes'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genes'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genes2ko'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg2uniref_mappings'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        kegg_data_util.drop_kegg2uniref_mappings_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg2uniref_mappings'))
        self.assertEqual(self.cursor.fetchone()[0], 0)
        kegg_data_util.drop_kegg_genes2ko_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genes2ko'))
        self.assertEqual(self.cursor.fetchone()[0], 0)
        kegg_data_util.drop_kegg_genes_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genes'))
        self.assertEqual(self.cursor.fetchone()[0], 0)
        kegg_data_util.drop_kegg_genomes_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_genomes'))
        self.assertEqual(self.cursor.fetchone()[0], 0)
        kegg_data_util.drop_kegg_orthologs_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','kegg_orthologs'))
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_import_kegg_orthologs_list(self):
        kegg_data_util.create_kegg_orthologs_table(self.cursor)
        kegg_data_util.import_kegg_orthologs_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_ko_list.txt'))
        self.cursor.execute('SELECT COUNT(*) FROM kegg_orthologs')
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT uid FROM kegg_orthologs WHERE ko_id = ?', ('K00001',))
        self.assertEqual(self.cursor.fetchone()[0], 1)

    def test_import_kegg_genomes_list(self):
        kegg_data_util.create_kegg_genomes_table(self.cursor)
        kegg_data_util.import_kegg_genomes_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_genomes.txt'))
        self.cursor.execute('SELECT COUNT(*) FROM kegg_genomes')
        self.assertEqual(self.cursor.fetchone()[0], 4)

    def test_import_kegg_genes(self):
        kegg_data_util.create_kegg_genomes_table(self.cursor)
        kegg_data_util.import_kegg_genomes_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_genomes.txt'))
        kegg_data_util.create_kegg_genes_table(self.cursor)
        kegg_data_util.import_kegg_genes(self.cursor, os.path.join(test_kegg_dir, 'ko_proteins_nr.fasta'))
        self.cursor.execute('SELECT COUNT(*) FROM kegg_genes')
        self.assertEqual(self.cursor.fetchone()[0], 3)

    def test_import_genes2ko_mappings(self):
        kegg_data_util.create_kegg_orthologs_table(self.cursor)
        kegg_data_util.import_kegg_orthologs_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_ko_list.txt'))
        kegg_data_util.create_kegg_genomes_table(self.cursor)
        kegg_data_util.import_kegg_genomes_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_genomes.txt'))
        kegg_data_util.create_kegg_genes_table(self.cursor)
        kegg_data_util.import_kegg_genes(self.cursor, os.path.join(test_kegg_dir, 'ko_proteins_nr.fasta'))
        kegg_data_util.create_kegg_genes2ko_table(self.cursor)
        kegg_data_util.import_genes2ko_mappings(self.cursor, test_kegg_dir)
        self.cursor.execute('SELECT COUNT(*) FROM kegg_genes2ko')
        self.assertEqual(self.cursor.fetchone()[0], 3)

    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(os.path.join(data_dir, db_file))

if __name__=='__main__':
    unittest.main()
