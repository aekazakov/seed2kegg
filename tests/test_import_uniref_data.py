#!/usr/bin/python
import sqlite3
import unittest
from context import lib
from lib import db_utils
from lib import uniref_data_util

data_dir = '../test_data/'
uniref_fasta_file = data_dir + 'test_uniref.faa'
db_file = data_dir + 'test_uniref.db'

class DataImportTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(db_file)
        self.cursor = self.conn.cursor()

    def test_uniref_create_tables(self):
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','uniref_proteins'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM uniref_proteins')
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_uniref_drop_tables(self):
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.drop_tables(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','uniref_proteins'))
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_uniref_import(self):
        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)
        self.cursor.execute('SELECT protein_hash FROM uniref_proteins WHERE uniref_id = ?', ('UniRef100_Q92AT0',))
        self.assertEqual(self.cursor.fetchone()[0], u'B2E2EDF5A1AA957ADBAA08384F6BFB9D')
        self.cursor.execute('SELECT COUNT(*) FROM uniref_proteins')
        self.assertEqual(self.cursor.fetchone()[0], 7)

    def tearDown(self):
        uniref_data_util.drop_tables(self.cursor)
        uniref_data_util.drop_indices(self.cursor)
        self.conn.commit()
        self.conn.close()

if __name__=='__main__':
    unittest.main()
