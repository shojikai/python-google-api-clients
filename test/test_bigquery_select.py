import os
import re
import sys
import unittest

from pprint import pprint

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients.bigquery import BigQuery

class BigQueryTest(unittest.TestCase):

    def setUp(self):
        self.project_id = os.getenv('PROJECT_ID')
        self.dataset_id = os.getenv('DATASET_ID', 'test_dataset')
        if self.project_id is None:
            print('PROJECT_ID is not defined.')
            sys.exit(1)
        self.bq = BigQuery(self.project_id)
        if self.bq.exists_dataset(self.dataset_id):
            self.bq.drop_dataset(self.dataset_id, delete_contents=True)
        self.bq.create_dataset(self.dataset_id)
        self.bq.dataset_id = self.dataset_id    # Set default datasetId

    def TearDown(self):
        self.bq.drop_dataset(self.dataset_id, delete_contents=True)

    def test_normal_page_token(self):
        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' \
            + 'FROM [publicdata:samples.shakespeare] '
        res = self.bq.select(query, max_results=1)
        self.assertEqual(10, len(res))
        pprint(res)

    def test_normal_empty(self):
        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' \
            + 'FROM [publicdata:samples.shakespeare] ' \
            + 'WHERE corpus = "hoge" '
        res = self.bq.select(query)
        self.assertEqual(0, len(res))
        pprint(res)

    def test_normal_async(self):
        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' \
            + 'FROM [publicdata:samples.shakespeare]'
        res = self.bq.select(query, async=True)
        self.assertTrue(re.match(r'job_', res))
        pprint(res)

    def test_normal(self):
        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' \
            + 'FROM [publicdata:samples.shakespeare]'
        res = self.bq.select(query)
        self.assertEqual(10, len(res))
        pprint(res)

if __name__ == '__main__':
    unittest.main()
