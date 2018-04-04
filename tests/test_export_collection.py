#!/usr/bin/python
import os
import unittest
from context import seed2kegg
from seed2kegg import db_utils
from seed2kegg import uniref_data_util
from seed2kegg import kegg_data_util
from seed2kegg import seed_data_util
from seed2kegg import data_analysis

data_dir = '../test_data/'
uniref_fasta_file = os.path.join(data_dir, 'test_uniref.faa')

seed_roles_file = os.path.join(data_dir, 'test_seed_roles_list.txt')
seed_genome_file = os.path.join(data_dir, 'test_seed_genomes.txt')
seed_gene_dir = os.path.join(data_dir, 'test_seed_dir')
seed_gene2roles_dir = os.path.join(data_dir, 'test_seed_dir')
seed_diamond_output = os.path.join(data_dir, 'test_diamond_output_seed.txt')

test_kegg_dir = os.path.join(data_dir, 'test_kegg_dir')
kegg_diamond_output = os.path.join(data_dir, 'test_diamond_output_kegg.txt')

seed2kegg_diamond_output = os.path.join(data_dir, 'test_diamond_output_seed2kegg.txt')
collection_file = os.path.join(data_dir, 'test_collection.txt')
db_file = os.path.join(data_dir, 'test.db')

class DataUpdateTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_utils.connect_local_database(db_file)
        self.cursor = self.conn.cursor()

        uniref_data_util.create_uniref_proteins_table(self.cursor)
        uniref_data_util.import_uniref_fasta(self.cursor, uniref_fasta_file)
        uniref_data_util.create_uniref_proteins_indices(self.cursor)

        seed_data_util.create_tables(self.cursor)
        seed_data_util.import_seed_functional_roles_table(self.cursor,seed_roles_file)
        seed_data_util.import_seed_genomes(self.cursor, seed_genome_file)
        seed_data_util.import_seed_genes(self.cursor, seed_gene_dir)
        seed_data_util.import_seed_gene2roles_mapping(self.cursor, seed_gene2roles_dir, 'test')
        seed_data_util.create_seed2uniref_mappings_table(self.cursor)
        seed_data_util.load_diamond_search_results(self.cursor,seed_diamond_output, 95.0, 5)

        kegg_data_util.create_kegg_orthologs_table(self.cursor)
        kegg_data_util.import_kegg_orthologs_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_ko_list.txt'))
        kegg_data_util.create_kegg_genomes_table(self.cursor)
        kegg_data_util.import_kegg_genomes_list(self.cursor, os.path.join(test_kegg_dir, 'kegg_genomes.txt'))
        kegg_data_util.create_kegg_genes_table(self.cursor)
        kegg_data_util.import_kegg_genes(self.cursor, os.path.join(test_kegg_dir, 'ko_proteins_nr.fasta'))
        kegg_data_util.create_kegg_genes2ko_table(self.cursor)
        kegg_data_util.import_genes2ko_mappings(self.cursor, test_kegg_dir)
        kegg_data_util.create_kegg2uniref_mappings_table(self.cursor)
        kegg_data_util.load_diamond_search_results(self.cursor,kegg_diamond_output, 95.0, 5)

        seed_data_util.create_seed2kegg_mappings_table(self.cursor)
        data_analysis.fill_seed2kegg_mappings_table(self.cursor, seed2kegg_diamond_output, 95.0, 5)

        db_utils.create_collections_table(self.cursor)
        db_utils.create_collection2function_table(self.cursor)
        data_analysis.import_collection_tsv(self.cursor, collection_file, 'nitrogen_test', 'test info', '0')

    def test_make_collection_gene_list(self):
        self.gene_collection = data_analysis.make_collection_gene_list(self.cursor,'nitrogen_test', '0')
        # Check results
#        print(self.gene_collection)
        self.assertNotEqual(len(self.gene_collection), 0)
        self.assertEqual(self.gene_collection['fig|511145.12.peg.2823']['source'], 'SEED')
        self.assertEqual(self.gene_collection['dml:Dmul_28240']['source'], 'KEGG')
        data_analysis.export_collection_proteins(self.gene_collection,os.path.join(test_kegg_dir, 'ko_proteins_nr.fasta'),os.path.join(data_dir, 'test_output.txt'))
        data_analysis.export_collection_proteins(self.gene_collection,os.path.join(seed_gene_dir, '511145.12_proteins.txt'),os.path.join(data_dir, 'test_output.txt'))
        data_analysis.export_collection_proteins(self.gene_collection,os.path.join(seed_gene_dir, '100226.15_proteins.txt'),os.path.join(data_dir, 'test_output.txt'))

    def tearDown(self):
        self.conn.commit()
        self.conn.close()
        os.remove(db_file)


if __name__=='__main__':
    unittest.main()
