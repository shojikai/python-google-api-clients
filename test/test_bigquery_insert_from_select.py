import os
import re
import sys
import time
import unittest
from pprint import pprint

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients.bigquery import BigQuery
from google_api_clients.bigquery.errors import BigQueryError

class BigQueryTest(unittest.TestCase):

    def setUp(self):
        self.project_id = os.getenv('PROJECT_ID')
        self.dataset_id = os.getenv('DATASET_ID', 'test_dataset')
        self.table_id = os.getenv('TABLE_ID', 'test_table') + '_' + str(int(time.time()))
        self.view_id = os.getenv('VIEW_ID', 'test_view') + '_' + str(int(time.time()))
        if self.project_id is None:
            print('PROJECT_ID is not defined.')
            sys.exit(1)
        self.bq = BigQuery(self.project_id)
        if self.bq.exists_dataset(self.dataset_id):
            self.bq.drop_dataset(self.dataset_id, delete_contents=True)
        self.bq.create_dataset(self.dataset_id)
        self.bq.dataset_id = self.dataset_id    # Set default datasetId
        #schema = [
        #    { 'name': 'title', 'type': 'STRING', 'mode': 'REQUIRED' },
        #    { 'name': 'count', 'type': 'INTEGER', 'mode': 'REQUIRED' }
        #]
        #self.bq.create_table(self.table_id, schema=schema)

    def TearDown(self):
        self.bq.drop_table(self.table_id)
        self.bq.drop_dataset(self.dataset_id, delete_contents=True)

    def test_error_allow_large_results(self):
        schema = [
            { 'name': 'word', 'type': 'STRING', 'mode': 'REQUIRED' },
            { 'name': 'word_count', 'type': 'INTEGER', 'mode': 'REQUIRED' },
        ]
        self.bq.create_table(self.table_id, schema=schema)

        query = 'SELECT word,word_count FROM [publicdata:samples.shakespeare] LIMIT 10'

        # Cannnot append to required fields when allowLargeResults is True
        with self.assertRaises(BigQueryError):
            self.bq.insert_from_select(self.table_id, query, allow_large_results=True)

    def test_error_no_required_field(self):
        schema = [
            { 'name': 'title', 'type': 'STRING', 'mode': 'REQUIRED' },
            { 'name': 'unique_words', 'type': 'INTEGER', 'mode': 'REQUIRED' },
            { 'name': 'required_field', 'type': 'STRING', 'mode': 'REQUIRED' },
        ]
        self.bq.create_table(self.table_id, schema=schema)

        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' \
            + 'FROM [publicdata:samples.shakespeare]'

        # No Required Field
        with self.assertRaises(BigQueryError):
            self.bq.insert_from_select(self.table_id, query)

    def test_error_schema_mismatch(self):
        schema = [
            { 'name': 'title', 'type': 'STRING', 'mode': 'REQUIRED' },
            { 'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED' },
        ]
        self.bq.create_table(self.table_id, schema=schema)

        query = 'SELECT title,id FROM [publicdata:samples.wikipedia] LIMIT 10'

        # Can't change type form NULLABLE to REQUIRED
        with self.assertRaises(BigQueryError):
            self.bq.insert_from_select(self.table_id, query)

    def test_normal_allow_large_results(self):
        schema = [
            { 'name': 'word', 'type': 'STRING', 'mode': 'NULLABLE' },
            { 'name': 'word_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
        ]
        self.bq.create_table(self.table_id, schema=schema)

        query = 'SELECT word,word_count FROM [publicdata:samples.shakespeare] LIMIT 10'
        res = self.bq.insert_from_select(self.table_id, query, allow_large_results=True)
        self.assertTrue(bool(res))
        pprint(res)

        res = self.bq.dump_table(self.table_id)
        self.assertEqual(10, len(res))
        pprint(res)

    def test_normal_insert_into_exists_table(self):
        schema = [
            { 'name': 'word', 'type': 'STRING', 'mode': 'REQUIRED' },
            { 'name': 'word_count', 'type': 'INTEGER', 'mode': 'REQUIRED' },
        ]
        self.bq.create_table(self.table_id, schema=schema)

        query = 'SELECT word,word_count FROM [publicdata:samples.shakespeare] LIMIT 10'
        res = self.bq.insert_from_select(self.table_id, query)
        self.assertTrue(bool(res))
        pprint(res)

        res = self.bq.dump_table(self.table_id)
        self.assertEqual(10, len(res))
        pprint(res)

    def test_normal_with_args(self):
        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words FROM shakespeare'
        res = self.bq.insert_from_select(
            dest_project_id=self.project_id, dest_dataset_id=self.dataset_id, dest_table_id=self.table_id, query=query,
            src_project_id='publicdata', src_dataset_id='samples'
        )
        self.assertTrue(bool(res))
        pprint(res)

        res = self.bq.dump_table(self.table_id)
        self.assertEqual(10, len(res))
        pprint(res)

    def test_normal_async(self):
        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' \
            + 'FROM [publicdata:samples.shakespeare]'
        res = self.bq.insert_from_select(self.table_id, query, async=True)
        self.assertTrue(re.match(r'job_', res))
        print(res)

        self.bq.wait_job(res)

        res = self.bq.dump_table(self.table_id)
        self.assertEqual(10, len(res))
        pprint(res)

    def test_normal(self):
        query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' \
            + 'FROM [publicdata:samples.shakespeare]'
        res = self.bq.insert_from_select(self.table_id, query)
        self.assertTrue(bool(res))
        pprint(res)

        res = self.bq.dump_table(self.table_id)
        self.assertEqual(10, len(res))
        pprint(res)

if __name__ == '__main__':
    unittest.main()
