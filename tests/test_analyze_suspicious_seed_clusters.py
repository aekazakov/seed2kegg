#!/usr/bin/python
#import urllib2
import seed2kegg
import unittest
import collections
import sqlite3

'''
 This script takes list of SEED protein clusters with multiple mappings to SEED functional roles.
 It checks if cluster members are actually have different roles.
 If they do, it reports them to a tsv file.
 
 This script does not make changes in the database
'''

work_dir = '../test_data/'
clusters_infile = work_dir + 'SEED_clusters_multiple_roles_patched_05032018.tsv'
db_file = '/mnt/data2/SEED/cross_mapping_project/seed_mappings.db'
clusters_outfile = work_dir + 'SEED_clusters_multiple_roles_patched_05032018_checked.tsv'


def connect_local_database(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def read_infile(cursor, infile):
    ret_val = collections.defaultdict(dict)
    with open(infile, 'r') as f:
        f.readline() #skip header
        for line in f:
            line = line.rstrip('\n\r')
            line_tokens = line.split('\t')
            if len(line_tokens) != 5:
                print 'Parsing error: ' + line
                continue
            if has_different_functional_roles(cursor, line_tokens[0]):
                ret_val[line_tokens[0]][line_tokens[3]] = 1
        f.closed
    return ret_val

def has_different_functional_roles(cursor, cluster):
    sql_query_memebers = 'SELECT seed_cluster100.n \
        FROM seed_cluster100 \
        WHERE seed_cluster100.cluster_id  = ?'
    sql_query_roles_count = 'SELECT COUNT (seed_genes.uid) \
        FROM seed_cluster100 JOIN seed_gene2cluster100 \
        ON seed_cluster100.uid=seed_gene2cluster100.seed_cluster100_uid \
        JOIN seed_genes ON seed_gene2cluster100.seed_gene_uid=seed_genes.uid \
        LEFT JOIN seed_gene2role ON seed_genes.uid=seed_gene2role.seed_gene_uid \
        LEFT JOIN seed_functional_roles ON seed_gene2role.seed_role_uid=seed_functional_roles.uid \
        WHERE seed_cluster100.cluster_id  = ? \
        GROUP BY seed_functional_roles.seed_role_id \
        ORDER BY seed_cluster100.cluster_id,seed_genes.uid,seed_functional_roles.seed_role_id ASC'
    cursor.execute(sql_query_memebers, (cluster, ))
    n = cursor.fetchone()[0]
    cursor.execute(sql_query_roles_count, (cluster, ))
    roles_count = cursor.fetchall()
    if len(roles_count) == 0:
        raise SystemExit('Role mappings not found for cluster ' + cluster)
    for count in roles_count:
        print count 
        if count[0] != n:
            return True
    return False

def analyze_clusters (cursor, clusters_roles_dict):
        sql_query = 'SELECT seed_cluster100.cluster_id, \
        seed_genes.seed_gene_id, seed_functional_roles.seed_role_id, \
        seed_functional_roles.seed_role_name, seed_genomes.genome_name, seed_genomes.latest_version \
        FROM seed_cluster100 JOIN seed_gene2cluster100 \
        ON seed_cluster100.uid=seed_gene2cluster100.seed_cluster100_uid \
        JOIN seed_genes ON seed_gene2cluster100.seed_gene_uid=seed_genes.uid \
        JOIN seed_genomes ON seed_genes.seed_genome_uid=seed_genomes.uid \
        LEFT JOIN seed_gene2role ON seed_genes.uid=seed_gene2role.seed_gene_uid \
        LEFT JOIN seed_functional_roles ON seed_gene2role.seed_role_uid=seed_functional_roles.uid \
        WHERE seed_cluster100.cluster_id = ? \
        ORDER BY seed_genes.uid,seed_functional_roles.seed_role_id ASC '
        for cluster in clusters_roles_dict:
            cursor.execute(sql_query, (cluster, ))
            clusters_roles_dict[cluster]['output'] = cursor.fetchall()

def write_output(clusters_roles_dict, outfile):
    with open(outfile, 'w') as f:
        for cluster in clusters_roles_dict:
            for entry in clusters_roles_dict[cluster]['output']:
                if entry[0] is None:
                    f.write('\t')
                else:
                    f.write(entry[0] + '\t')
                f.write(entry[1] + '\t')
                if entry[2] is None:
                    f.write('\t')
                else:
                    f.write('\t'.join(entry[2:3]))
                f.write('\t' + entry[4] + '\t' + str(entry[5]) + '\n')
        f.closed

class DataSearchTest(unittest.TestCase):
    def setUp(self):
        self.conn = connect_local_database(db_file)
        self.cursor = self.conn.cursor()
    def test_has_different_functional_roles(self):
        self.assertEqual(has_different_functional_roles(self.cursor, 4436240), False)
        self.assertEqual(has_different_functional_roles(self.cursor, 1330232), True)
    def tearDown(self):
        self.conn.close()

if __name__=='__main__':
    unittest.main()
