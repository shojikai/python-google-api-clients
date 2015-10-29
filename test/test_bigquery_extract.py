import os
import sys
import time
import unittest
from pprint import pprint

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients.bigquery import BigQuery

class BigQueryTest(unittest.TestCase):

    def setUp(self):
        self.project_id = os.getenv('PROJECT_ID')
        self.dataset_id = os.getenv('DATASET_ID', 'test_dataset')
        self.nested_table_id = os.getenv('TABLE_ID', 'test_table') + '_nested_' + str(int(time.time()))
        self.flat_table_id = os.getenv('TABLE_ID', 'test_table') + '_flat_' + str(int(time.time()))
        self.bucket = os.getenv('BUCKET')
        if self.project_id is None:
            print('PROJECT_ID is not defined.')
            sys.exit(1)
        if self.bucket is None:
            print('BCUKET is not defined.')
            sys.exit(1)
        self.bq = BigQuery(self.project_id)
        if self.bq.exists_dataset(self.dataset_id):
            self.bq.drop_dataset(self.dataset_id, delete_contents=True)
        self.bq.create_dataset(self.dataset_id)
        self.bq.dataset_id = self.dataset_id    # Set default datasetId

        # create & load nested table
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
        self.bq.create_table(self.nested_table_id, schema=schema)
        rows = [
            { 'id': 1, 'name': 'foo' },
            { 'id': 2, 'name': 'bar', 'birth': { 'year': 2015, 'month': 10, 'day': 28 } },
            { 'id': 3, 'name': 'baz', 'url': [
                'http://www.yahoo.co.jp/',
                'http://www.google.co.jp/',
            ]}
        ]
        self.bq.load(self.nested_table_id, rows)

        # create & load flat table
        schema = [
            { 'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED' },
            { 'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED' },
        ]
        self.bq.create_table(self.flat_table_id, schema=schema)
        rows = [
            { 'id': 1, 'name': 'foo' },
            { 'id': 2, 'name': 'bar' },
            { 'id': 3, 'name': 'baz' },
        ]
        self.bq.load(self.flat_table_id, rows)

    def TearDown(self):
        self.bq.drop_table(self.nested_table_id)
        self.bq.drop_dataset(self.dataset_id, delete_contents=True)

    def test_normal(self):
        # json
        destination_uris = [
            self.bucket + '/test-*.json'
        ]
        print("extract start (json)")
        res = self.bq.extract(self.nested_table_id, destination_uris)
        pprint(res)
        print("extract end (json)")

        # avro
        destination_uris = [
            self.bucket + '/test-*.avro'
        ]
        print("extract start (avro)")
        res = self.bq.extract(self.nested_table_id, destination_uris)
        pprint(res)
        print("extract end (avro)")

        # csv
        destination_uris = [
            self.bucket + '/test-*.csv'
        ]
        print("extract start (csv)")
        res = self.bq.extract(self.flat_table_id, destination_uris)
        pprint(res)
        print("extract end (csv)")

        # tsv
        destination_uris = [
            self.bucket + '/test-*.tsv'
        ]
        print("extract start (tsv)")
        res = self.bq.extract(self.flat_table_id, destination_uris)
        pprint(res)
        print("extract end (tsv)")

        # json + gz
        destination_uris = [
            self.bucket + '/test-*.json.gz'
        ]
        print("extract start (json+gz)")
        res = self.bq.extract(self.nested_table_id, destination_uris)
        pprint(res)
        print("extract end (json+gz)")

        # csv + gz
        destination_uris = [
            self.bucket + '/test-*.csv.gz'
        ]
        print("extract start (csv+gz)")
        res = self.bq.extract(self.flat_table_id, destination_uris)
        pprint(res)
        print("extract end (csv+gz)")

        # tsv + gz
        destination_uris = [
            self.bucket + '/test-*.tsv.gz'
        ]
        print("extract start (tsv+gz)")
        res = self.bq.extract(self.flat_table_id, destination_uris)
        pprint(res)
        print("extract end (tsv+gz)")

if __name__ == '__main__':
    unittest.main()
