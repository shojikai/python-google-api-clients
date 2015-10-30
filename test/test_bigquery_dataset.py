import os
import sys
import unittest
from pprint import pprint

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients.bigquery import BigQuery
from google_api_clients.bigquery.errors import ParameterError

class BigQueryTest(unittest.TestCase):

    def setUp(self):
        self.project_id = os.getenv('PROJECT_ID')
        self.dataset_id = os.getenv('DATASET_ID', 'test_dataset')
        if self.project_id is None:
            print('PROJECT_ID is not defined.')
            sys.exit(1)
        self.bq = BigQuery(self.project_id)

    def TearDown(self):
        pass

    def test_normal(self):
        print('exists dataset')
        if self.bq.exists_dataset(self.dataset_id):
            print('delete dataset')
            res = self.bq.drop_dataset(self.dataset_id, delete_contents=True)
            self.assertTrue(bool(res))

        print('create dataset')
        res = self.bq.create_dataset(self.dataset_id)
        self.assertTrue(bool(res))

        print('create exists dataset')
        res = self.bq.create_dataset(self.dataset_id)
        self.assertFalse(bool(res))

        print('show datasets')
        res = self.bq.show_datasets()
        self.assertIn(self.dataset_id, res)
        print('\n'.join(res))

        print('delete dataset')
        res = self.bq.drop_dataset(self.dataset_id)
        self.assertFalse(bool(res))

        print('delete no exists dataset')
        res = self.bq.drop_dataset(self.dataset_id)
        self.assertFalse(bool(res))

    def test_normal_with_args(self):
        print('exists dataset: ' + self.dataset_id)
        if self.bq.exists_dataset(self.dataset_id, project_id=self.project_id):
            print('exists')

            print('delete dataset: ' + self.dataset_id)
            res = self.bq.drop_dataset(self.dataset_id, project_id=self.project_id, delete_contents=True)
            self.assertTrue(bool(res))
        else:
            print('no exists')

        print('create dataset')
        access = [
            { 'role': 'OWNER', 'specialGroup': 'projectOwners' },
        ]
        res = self.bq.create_dataset(self.dataset_id, project_id=self.project_id, access=access,
            default_table_expiration_ms=3600000, description='Description', friendly_name='Friendly Name',
            location='EU')
        self.assertTrue(bool(res))

        print('info dataset')
        res = self.bq.info_dataset(self.dataset_id, project_id=self.project_id)
        self.assertEqual(1, len(res['access']))
        self.assertEqual('OWNER', res['access'][0]['role'])
        self.assertEqual('projectOwners', res['access'][0]['specialGroup'])
        self.assertEqual(3600000, int(res['defaultTableExpirationMs']))
        self.assertEqual('Description', res['description'])
        self.assertEqual('Friendly Name', res['friendlyName'])
        self.assertEqual('EU', res['location'])
        pprint(res)

        print('show datasets')
        res = self.bq.show_datasets(project_id=self.project_id, all=True, max_results=10)
        self.assertIn(self.dataset_id, res)
        print('\n'.join(res))

        print('delete dataset: ' + self.dataset_id)
        res = self.bq.drop_dataset(self.dataset_id, delete_contents=True)
        self.assertFalse(bool(res))

    def test_error(self):
        with self.assertRaises(TypeError):
            self.bq.create_dataset()

        with self.assertRaises(ParameterError):
            self.bq.create_dataset(dataset_id=None)

if __name__ == '__main__':
    unittest.main()
