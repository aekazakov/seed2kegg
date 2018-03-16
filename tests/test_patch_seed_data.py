#!/usr/bin/python
import unittest
import seed2kegg
import store_seed_data
import patch_seed_data

#ko_list_file = 'ko_list.txt'
data_dir = '../test_data/'
seed_roles_file = data_dir + 'seed_nr_roles_list.txt'
seed_genome_file = data_dir + 'test_seed_genomes.txt'
seed_gene_file = data_dir + 'test_seed_genes.txt'
seed_gene2roles_file = data_dir + 'test_seed_genes2roles.txt'
seed_patch_file = data_dir + 'test_seed_update.txt'
db_file = data_dir + 'test_seed_mappings.db'


def drop_gene2role_changes_table(cursor):
    cursor.execute('DROP TABLE IF EXISTS seed_gene2role_changes')
    cursor.execute('DROP INDEX IF EXISTS seed_gene2role_changes_geneid_1')
    cursor.execute('DROP INDEX IF EXISTS seed_gene2role_changes_roleid_1')

class DataUpdateTest(unittest.TestCase):

    def setUp(self):
        self.conn = store_seed_data.connect_local_database(db_file)
        self.cursor = self.conn.cursor()
        store_seed_data.create_tables(self.cursor)
        store_seed_data.create_uniref_proteins_indices(self.cursor)
        store_seed_data.create_seed_roles_index(self.cursor)
        store_seed_data.create_seed_genomes_index(self.cursor)
        store_seed_data.create_seed_genes2roles_index(self.cursor)
        store_seed_data.create_seed2uniref_index(self.cursor)
        patch_seed_data.create_gene2role_changes_table(self.cursor)
        patch_seed_data.create_indices(self.cursor)
        self.conn.commit()

    def test_get_role_uid(self):
        store_seed_data.populate_seed_functional_roles_table(self.cursor, seed_roles_file)
        self.assertEqual(patch_seed_data.get_role_uid(self.cursor, '1'), 2)
        self.assertEqual(patch_seed_data.get_role_uid(self.cursor, '4944'), 4945)

    def test_get_gene_uid(self):
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        store_seed_data.populate_seed_genes_table(self.cursor, seed_gene_file)
        self.assertEqual(patch_seed_data.get_gene_uid(self.cursor, 'fig|511145.6.peg.533'), 1)

    def test_check_gene_to_role_link(self):
        store_seed_data.populate_seed_functional_roles_table(self.cursor, seed_roles_file)
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        store_seed_data.populate_seed_genes_table(self.cursor, seed_gene_file)
        store_seed_data.populate_seed_genes2roles_table(self.cursor, seed_gene2roles_file, 'test')
        self.assertTrue(patch_seed_data.check_gene_to_role_link(self.cursor, 3, 4943))
        self.assertFalse(patch_seed_data.check_gene_to_role_link(self.cursor, 1, 1))

    def test_read_patch_infile(self):
        store_seed_data.populate_seed_functional_roles_table(self.cursor, seed_roles_file)
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        store_seed_data.populate_seed_genes_table(self.cursor, seed_gene_file)
        store_seed_data.populate_seed_genes2roles_table(self.cursor, seed_gene2roles_file, 'test')
        self.patch = patch_seed_data.read_patch_infile(self.cursor, seed_patch_file)
        print str(self.patch)
        self.assertEqual(len(self.patch), 3)
        self.assertEqual(self.patch[3]['gene_uid'], 1)
        self.assertEqual(len(self.patch[3]['current roles']), 0)

    def test_apply_seed_gene2role_patch(self):
        store_seed_data.populate_seed_functional_roles_table(self.cursor, seed_roles_file)
        store_seed_data.populate_seed_genomes_table(self.cursor, seed_genome_file)
        store_seed_data.populate_seed_genes_table(self.cursor, seed_gene_file)
        store_seed_data.populate_seed_genes2roles_table(self.cursor, seed_gene2roles_file, 'test')
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role_changes')
        self.assertEqual(self.cursor.fetchone()[0], 0)
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role')
        self.assertEqual(self.cursor.fetchone()[0], 6)
        self.patch = patch_seed_data.read_patch_infile(self.cursor, seed_patch_file)
        patch_seed_data.apply_seed_gene2role_patch(self.cursor, self.patch)
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role_changes')
        self.assertEqual(self.cursor.fetchone()[0], 5)
        self.cursor.execute('SELECT COUNT(*) FROM seed_gene2role')
        self.assertEqual(self.cursor.fetchone()[0], 7)

    def tearDown(self):
        store_seed_data.drop_tables(self.cursor)
        store_seed_data.drop_indices(self.cursor)
        drop_gene2role_changes_table(self.cursor)
        self.conn.commit()
        self.conn.close()

if __name__=='__main__':
    unittest.main()
