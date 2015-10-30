import os
import signal
import sys
import time
import unittest
from pprint import pprint

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/../')

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
        schema = [
            { 'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED' },
            { 'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED' },
            { 'name': 'birth', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
                { 'name': 'year', 'type': 'INTEGER', 'mode': 'REQUIRED' },
                { 'name': 'month', 'type': 'INTEGER', 'mode': 'REQUIRED' },
                { 'name': 'day', 'type': 'INTEGER', 'mode': 'REQUIRED' },
            ]},
            { 'name': 'url', 'type': 'STRING', 'mode': 'REPEATED' },
        ]
        self.bq.create_table(self.table_id, schema=schema)

    def TearDown(self):
        self.bq.drop_table(self.table_id)
        self.bq.drop_dataset(self.dataset_id, delete_contents=True)

    @staticmethod
    def sigalrm_handler(x, y):
        raise Exception("Timeout")

    def wait_insert(self):
        signal.signal(signal.SIGALRM, BigQueryTest.sigalrm_handler)
        signal.alarm(120)
        while True:
            res = self.bq.dump_table(self.table_id)
            if bool(res): 
                signal.alarm(0)
                return res
            print('sleep...')
            time.sleep(2)

    def test_error(self):
        rows = [
            { 'id': 1, 'name': 'foo' },         # normal
            { 'id': 2 },                        # missing required field
            { 'id': 'three', 'name': 'baz' },   # invalid data type
        ]
        with self.assertRaises(BigQueryError):
            self.bq.insert(self.table_id, rows, ignore_unknown_values=True)

        rows = [
            { 'id': 1, 'name': 'foo' },
            { 'id': 2, 'name': 'bar', 'unknown_fields': 'unknown_value' },
        ]
        with self.assertRaises(BigQueryError):
            self.bq.insert(self.table_id, rows, skip_invalid_rows=True)

    def test_normal_skip_invalid_rows(self):
        rows = [
            { 'id': 1, 'name': 'foo' },         # normal
            { 'id': 2 },                        # missing required field
            { 'id': 'three', 'name': 'baz' },   # invalid data type
        ]
        res = self.bq.insert(self.table_id, rows, skip_invalid_rows=True)
        self.assertTrue(bool(res))

        res = self.wait_insert()
        self.assertEqual(1, len(res))
        pprint(res)

    def test_normal_ignore_unknown_values(self):
        rows = [
            { 'id': 1, 'name': 'foo' },         # normal
            { 'id': 2, 'name': 'bar', 'unknown_field': 'unknown_value' },
        ]
        res = self.bq.insert(self.table_id, rows, ignore_unknown_values=True)
        self.assertTrue(bool(res))
        
        res = self.wait_insert()
        self.assertEqual(2, len(res))
        pprint(res)

    def test_normal(self):
        rows = [
            { 'id': 1, 'name': 'foo' },
            { 'id': 2, 'name': 'bar', 'birth': { 'year': 2015, 'month': 10, 'day': 28 } },
            { 'id': 3, 'name': 'baz', 'url': [
                'http://www.yahoo.co.jp/',
                'http://www.google.co.jp/',
            ]}
        ]
        res = self.bq.insert(self.table_id, rows)
        self.assertTrue(bool(res))

        res = self.wait_insert()
        self.assertEqual(3, len(res))
        pprint(res)

if __name__ == '__main__':
    unittest.main()
