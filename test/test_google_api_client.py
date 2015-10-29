import os
import sys
import unittest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients import GoogleApiClient
from google_api_clients.errors import MethodNameError
from google_api_clients.errors import ResourceNameError

class TestGoogleApiClient(unittest.TestCase):

    def setUp(self):
        self.bq = GoogleApiClient().auth().build('bigquery', 'v2')
        pass

    def TearDown(self):
        pass

    def test_request(self):
        res = self.bq.request('projects', 'list')
        self.assertEqual('bigquery#projectList', res['kind'])

    def test_TypeError(self):
        with self.assertRaises(TypeError):
            self.bq.request('datasets', 'list')

    def test_ResourceNameError(self):
        with self.assertRaises(ResourceNameError):
            self.bq.request('unknown_resource', 'list')

    def test_MethodNameError(self):
        with self.assertRaises(MethodNameError):
            self.bq.request('projects', 'unknown_method')

if __name__ == '__main__':
    unittest.main()
