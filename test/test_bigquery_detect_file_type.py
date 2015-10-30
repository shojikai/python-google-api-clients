import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients.bigquery import BigQuery

class BigQueryTest(unittest.TestCase):

    def setUp(self):
        self.project_id = os.getenv('PROJECT_ID')
        if self.project_id is None:
            print('PROJECT_ID is not defined.')
            sys.exit(1)
        self.bq = BigQuery(self.project_id)

    def TearDown(self):
        pass

    def test_normal(self):
        # CSV
        filename = 'filename-*.csv'
        (file_format, field_delimiter, compression) = self.bq.detect_file_format(filename)
        self.assertEqual('CSV', file_format)
        self.assertEqual(',', field_delimiter)
        self.assertEqual('NONE', compression)

        # TSV
        filename = 'filename-*.tsv'
        (file_format, field_delimiter, compression) = self.bq.detect_file_format(filename)
        self.assertEqual('CSV', file_format)
        self.assertEqual('\t', field_delimiter)
        self.assertEqual('NONE', compression)

        # Json
        filename = 'filename-*.json'
        (file_format, field_delimiter, compression) = self.bq.detect_file_format(filename)
        self.assertEqual('NEWLINE_DELIMITED_JSON', file_format)
        self.assertEqual(None, field_delimiter)
        self.assertEqual('NONE', compression)

        # CSV + GZ
        filename = 'filename-*.csv.gz'
        (file_format, field_delimiter, compression) = self.bq.detect_file_format(filename)
        self.assertEqual('CSV', file_format)
        self.assertEqual(',', field_delimiter)
        self.assertEqual('GZIP', compression)

        # TSV + GZ
        filename = 'filename-*.tsv.gz'
        (file_format, field_delimiter, compression) = self.bq.detect_file_format(filename)
        self.assertEqual('CSV', file_format)
        self.assertEqual('\t', field_delimiter)
        self.assertEqual('GZIP', compression)

        # Json + GZ
        filename = 'filename-*.json.gz'
        (file_format, field_delimiter, compression) = self.bq.detect_file_format(filename)
        self.assertEqual('NEWLINE_DELIMITED_JSON', file_format)
        self.assertEqual(None, field_delimiter)
        self.assertEqual('GZIP', compression)

if __name__ == '__main__':
    unittest.main()
