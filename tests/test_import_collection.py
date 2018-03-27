#!/usr/bin/python
import os
import unittest
from context import lib
from lib import db_utils
from lib import kegg_data_util
from lib import seed_data_util
from lib import data_analysis

data_dir = '../test_data/'
test_kegg_dir = os.path.join(data_dir, 'test_kegg_dir')
seed_roles_file = os.path.join(data_dir, 'test_seed_roles_list.txt')
collection_file = os.path.join(data_dir, 'test_collection.txt')
kegg_diamond_output = os.path.join(data_dir, 'test_diamond_output_kegg.txt')
db_file = os.path.join(data_dir, 'test.db')

class DataUpdateTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(db_file)
        self.cursor = self.conn.cursor()

    def test_create_collections_table(self):
        db_utils.create_collections_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','collections'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM collections')
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_create_collection2function_table(self):
        db_utils.create_collection2function_table(self.cursor)
        self.cursor.execute('select count(*) from sqlite_master where type = ? and name = ?', ('table','collection2function'))
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM collection2function')
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_import_collection_tsv(self):
        # Load test data
        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_functional_roles_table(self.cursor,seed_roles_file)        
        self.cursor.execute('SELECT uid FROM seed_functional_roles WHERE seed_role_id IS ?', ('9724',))

        kegg_data_util.create_kegg_orthologs_table(self.cursor)
        kegg_data_util.import_kegg_orthologs_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_ko_list.txt'))
        self.cursor.execute('SELECT COUNT(*) FROM kegg_orthologs')
        self.assertEqual(self.cursor.fetchone()[0], 1)

        # Prepare database
        db_utils.create_collections_table(self.cursor)
        db_utils.create_collection2function_table(self.cursor)

        # Import data
        data_analysis.import_collection_tsv(self.cursor, collection_file, 'nitrogen_test', 'test info', '0')

        # Check results
        self.cursor.execute('SELECT COUNT(*) FROM collections')
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute('SELECT COUNT(*) FROM collection2function')
        self.assertEqual(self.cursor.fetchone()[0], 4)

    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(db_file)


if __name__=='__main__':
    unittest.main()
