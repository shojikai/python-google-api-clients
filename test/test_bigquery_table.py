import os
import sys
import time
import unittest
from pprint import pprint

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients.bigquery import BigQuery
from google_api_clients.bigquery.errors import Http4xxError
from google_api_clients.bigquery.errors import NotFoundError

class BigQueryTest(unittest.TestCase):

    def setUp(self):
        self.project_id = os.getenv('PROJECT_ID')
        self.dataset_id = os.getenv('DATASET_ID', 'test_dataset')
        self.table_id = os.getenv('TABLE_ID', 'test_table')
        self.view_id = os.getenv('VIEW_ID', 'test_view')
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

    def test_error(self):
        query = 'SELECT * FROM ' + self.dataset_id + '.' + self.table_id
        with self.assertRaises(NotFoundError):
            self.bq.create_view(self.view_id, query)

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

        with self.assertRaises(NotFoundError):
            self.bq.create_table(self.table_id, schema=schema, dataset_id='not_found_dataset')

        self.bq.create_table(self.table_id, schema=schema)

        # "Schema field shouldn't be used as input with a view"
        with self.assertRaises(Http4xxError):
            self.bq.create_view(self.view_id, query, schema=schema)

    def test_normal_with_args(self):
        print("exists table?")
        if self.bq.exists_table(project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.table_id):
            print("exists")

            print("drop table")
            res = self.bq.drop_table(project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.table_id)
            self.assertTrue(bool(res))
        else:
            print("no exists")

        print("create table")
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
        expiration_time = str(int(time.time()) + 86400) + '000'
        res = self.bq.create_table(project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.table_id,
            schema=schema, description='Description', expiration_time=expiration_time, friendly_name='Friendly Name')
        self.assertTrue(bool(res))

        print("info table")
        res = self.bq.info_table(project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.table_id)
        self.assertEqual('Description', res['description'])
        self.assertEqual(expiration_time, res['expirationTime'])
        self.assertEqual('Friendly Name', res['friendlyName'])
        pprint(res)

        print("create view")
        query = 'SELECT * FROM ' + self.dataset_id + '.' + self.table_id
        res = self.bq.create_view(project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.view_id, query=query)
        self.assertTrue(bool(res))

        print("show tables")
        res = self.bq.show_tables(project_id=self.project_id, dataset_id=self.dataset_id, max_results=1)
        self.assertIn(self.table_id, res)
        self.assertIn(self.view_id, res)
        print('\n'.join(res))

        print("drop view")
        res = self.bq.drop_table(self.view_id)
        self.assertFalse(bool(res))

        print("drop table")
        res = self.bq.drop_table(self.table_id)
        self.assertFalse(bool(res))

    def test_normal(self):
        print("exists table?")
        if self.bq.exists_table(self.table_id):
            print("exists")

            print("drop table")
            res = self.bq.drop_table(self.table_id)
            self.assertTrue(bool(res))
        else:
            print("no exists")

        print("create table")
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
        res = self.bq.create_table(self.table_id, schema=schema)
        self.assertTrue(bool(res))

        print("create exists table")
        res = self.bq.create_table(self.table_id, schema=schema)
        self.assertFalse(bool(res))

        print("info table")
        res = self.bq.info_table(self.table_id)
        self.assertTrue(bool(res))
        pprint(res)

        print("create view")
        query = 'SELECT * FROM ' + self.dataset_id + '.' + self.table_id
        self.bq.create_view(self.view_id, query)
        self.assertTrue(bool(res))

        print("info view")
        res = self.bq.info_table(self.view_id)
        self.assertTrue(bool(res))
        pprint(res)

        print("show tables")
        res = self.bq.show_tables()
        self.assertIn(self.table_id, res)
        self.assertIn(self.view_id, res)
        print('\n'.join(res))

        print("drop view")
        res = self.bq.drop_table(self.view_id)
        self.assertFalse(bool(res))

        print("drop table")
        res = self.bq.drop_table(self.table_id)
        self.assertFalse(bool(res))

        print("drop no exists table")
        res = self.bq.drop_table(self.table_id)
        self.assertFalse(bool(res))

if __name__ == '__main__':
    unittest.main()
