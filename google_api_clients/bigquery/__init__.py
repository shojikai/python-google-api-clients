import json
import os
import re
import signal
import time

from StringIO import StringIO
from types import DictionaryType
from types import ListType
from types import StringType

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseUpload

from .. import GoogleApiClient
from .errors import AlreadyExistsError
from .errors import BigQueryError
from .errors import DatasetIsNotEmptyError
from .errors import Http4xxError
from .errors import Http5xxError
from .errors import JobWaitTimeoutError
from .errors import LoadError
from .errors import NotFoundError
from .errors import ParameterError

class BigQuery(GoogleApiClient):

    JOB_WAIT_TIMEOUT = 600
    MAX_RESULTS = 100000

    def __init__(self, project_id, **options):
        super(BigQuery, self).__init__(project_id=project_id, **options)
        self.auth().build('bigquery', 'v2')
        self.dataset_id = options.get('dataset_id')

    def request(self, resource, method, **kwargs):
        try:
            res = super(BigQuery, self).request(resource, method, **kwargs)
            if 'insertErrors' in res:
                # tabledata.insertAll
                for error in [y for x in res['insertErrors'] for y in x['errors']]:
                    if 'message' in error:
                        if re.search(r'no such field', error['message'], re.I):
                            raise BigQueryError(error)
                        elif kwargs['body']['skipInvalidRows'] is not True:
                            raise BigQueryError(error)
                    else:
                        BigQueryError(error)
            elif 'errors' in res:
                # jobs.query
                raise BigQueryError(res['errors'])
            elif 'status' in res and 'errors' in res['status']:
                # jobs.insert
                raise BigQueryError(res['status']['errors'])
        except HttpError as e:
            if e.resp.status == 409 and re.search(r'Already Exists', str(e), re.I):
                raise AlreadyExistsError(e)
            elif e.resp.status == 404 and re.search(r'Not Found', str(e), re.I):
                raise NotFoundError(e)
            elif e.resp.status == 400 and re.search(r'Required parameter is missing', str(e), re.I):
                raise ParameterError(e)
            elif e.resp.status == 400 and re.search(r'still in use', str(e), re.I) \
                and resource == 'datasets' and method == 'delete':
                raise DatasetIsNotEmptyError(e)
            elif 400 <= e.resp.status <= 499:
                raise Http4xxError(e)
            elif 500 <= e.resp.status <= 599:
                raise Http5xxError(e)
            else:
                raise e

        return res

    def create_dataset(self, dataset_id, **options):
        kwargs = {
            'projectId': self.project_id,
            'body': {
                'access': options.get('access'),
                'datasetReference': {
                    'projectId': options.get('project_id', self.project_id),
                    'datasetId': dataset_id,
                },
                'defaultTableExpirationMs': options.get('default_table_expiration_ms'),
                'description': options.get('description'),
                'friendlyName': options.get('friendly_name'),
                'location': options.get('location', 'US'),
            }
        }
        try:
            return self.request('datasets', 'insert', **kwargs)
        except AlreadyExistsError:
            return {}

    def drop_dataset(self, dataset_id, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': dataset_id,
            'deleteContents': options.get('delete_contents', False),
        }
        try:
            return self.request('datasets', 'delete', **kwargs)
        except NotFoundError:
            return {}

    def exists_dataset(self, dataset_id, **options):
        res = self.info_dataset(dataset_id, **options)
        return bool(res)

    def info_dataset(self, dataset_id, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': dataset_id,
        }
        try:
            return self.request('datasets', 'get', **kwargs)
        except NotFoundError:
            return {}

    def show_datasets(self, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'all': options.get('all'),
            'maxResults': options.get('max_results', BigQuery.MAX_RESULTS),
            'pageToken': options.get('page_token'),
        }
        res = self.request('datasets', 'list', **kwargs)
        if 'datasets' not in res:
            return []
        ret = [dataset['datasetReference']['datasetId'] for dataset in res['datasets']]
        if 'nextPageToken' in res:
            options['page_token'] = res['nextPageToken']
            ret.extend(self.show_datasets(**options))
        return ret

    def create_table(self, table_id, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': options.get('dataset_id', self.dataset_id),
            'body': {
                'description': options.get('description'),
                'expirationTime': options.get('expiration_time'),
                'externalDataConfiguration': options.get('external_data_configuration'),
                'friendlyName': options.get('friendly_name'),
                'tableReference': {
                    'projectId': options.get('project_id', self.project_id),
                    'datasetId': options.get('dataset_id', self.dataset_id),
                    'tableId': table_id,
                }
            }
        }
        if 'schema' in options:
            kwargs['body']['schema'] = {
                'fields': options.get('schema')
            }
        if 'query' in options:
            kwargs['body']['view'] = {
                'query': options.get('query')
            }
        try:
            return self.request('tables', 'insert', **kwargs)
        except AlreadyExistsError:
            return {}

    def create_view(self, table_id, query, **options):
        return self.create_table(table_id, query=query, **options)

    def drop_table(self, table_id, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': options.get('dataset_id', self.dataset_id),
            'tableId': table_id,
        }
        try:
            return self.request('tables', 'delete', **kwargs)
        except NotFoundError:
            return {}

    def exists_table(self, table_id, **options):
        res = self.info_table(table_id, **options)
        return bool(res)

    def info_table(self, table_id, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': options.get('dataset_id', self.dataset_id),
            'tableId': table_id,
        }
        try:
            return self.request('tables', 'get', **kwargs)
        except NotFoundError:
            return {}

    def show_tables(self, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': options.get('dataset_id', self.dataset_id),
            'maxResults': options.get('max_results', BigQuery.MAX_RESULTS),
            'pageToken': options.get('page_token')
        }
        res = self.request('tables', 'list', **kwargs)
        if 'tables' not in res:
            return []
        ret = [table['tableReference']['tableId'] for table in res['tables']]
        if 'nextPageToken' in res:
            options['page_token'] = res['nextPageToken']
            ret.extend(self.show_tables(**options))
        return ret

    def insert(self, table_id, rows, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': options.get('dataset_id', self.dataset_id),
            'tableId': table_id,
            'body': {
                'rows': [ { 'json': row } for row in rows ],
                'ignoreUnknownValues': options.get('ignore_unknown_values', False),
                'skipInvalidRows': options.get('skip_invalid_rows', False),
            }
        }
        return self.request('tabledata', 'insertAll', **kwargs)

    def dump_table(self, table_id, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'datasetId': options.get('dataset_id', self.dataset_id),
            'tableId': table_id,
            'maxResults': options.get('max_results', BigQuery.MAX_RESULTS),
            'pageToken': options.get('page_token'),
            'startIndex': options.get('start_index'),
        }
        res = self.request('tabledata', 'list', **kwargs)
        if 'rows' not in res:
            return []
        ret = res['rows']
        if 'pageToken' in res:
            options['page_token'] = res['pageToken']
            options['start_index'] = options.get('start_index', 0) + len(res['rows'])
            ret.extend(self.dump_table(table_id, **options))
        return ret

    def detect_file_format(self, filename):
        file_format = None
        field_delimiter = None
        compression = 'NONE'

        if re.search(r'.+\.(?:gz)$', filename, re.I):
            compression = 'GZIP'

        if re.search(r'.+\.csv(?:\.gz)?$', filename, re.I):
            file_format = 'CSV'
            field_delimiter = ','
        elif re.search(r'.+\.tsv(?:\.gz)?$', filename, re.I):
            file_format = 'CSV'
            field_delimiter = '\t'
        elif re.search(r'.+\.json(?:\.gz)?$', filename, re.I):
            file_format = 'NEWLINE_DELIMITED_JSON'
        elif re.search(r'.+\.avro(?:\.gz)?$', filename, re.I):
            file_format = 'AVRO'
            compression = 'NONE' # compression is not supported with avro

        return (file_format, field_delimiter, compression)

    def info_job(self, job_id, **options):
        kwargs = {
            'projectId': options.get('project_id', self.project_id),
            'jobId': job_id
        }
        return self.request('jobs', 'get', **kwargs)

    def done_job(self, job_id, **options):
        res = self.info_job(job_id, **options)
        if res['status']['state'] == 'DONE':
            return True
        else:
            return False

    def wait_job(self, job_id, **options):
        timeout = options.get('timeout', BigQuery.JOB_WAIT_TIMEOUT)
        def handler(x, y):
            raise JobWaitTimeoutError('timeout: ' + str(timeout) + 'sec')
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(timeout)
        while True:
            res = self.info_job(job_id)
            if res['status']['state'] == 'DONE':
                signal.alarm(0)
                return res
            time.sleep(2)

    def load(self, table_id, data, **options):
        media_body = None
        source_uris = None
        source_format = None
        field_delimiter = None

        if type(data) is ListType:
            if type(data[0]) is DictionaryType:
                newline_delimited_json = '\n'.join([json.dumps(datum) for datum in data])
                media_body = MediaIoBaseUpload(StringIO(newline_delimited_json), mimetype='application/octet-stream')
                source_format = 'NEWLINE_DELIMITED_JSON'
            elif type(data[0]) is StringType and re.search(r'^gs://', data[0]):
                source_uris = data
                (source_format, field_delimiter, compression) = self.detect_file_format(data[0])
            elif type(data[0]) is StringType:
                media_body = MediaIoBaseUpload(StringIO('\n'.join(data)), mimetype='application/octet-stream')
                source_format = 'CSV'
            else:
                raise LoadError('Unknown data type')
        elif type(data) is StringType and re.search(r'^gs://', data):
            source_uris = [data]
            (source_format, field_delimiter, compression) = self.detect_file_format(data)
        elif type(data) is StringType and os.path.exists(data):
            media_body = MediaFileUpload(data, mimetype='application/octet-stream')
            (source_format, field_delimiter, compression) = self.detect_file_format(data)
        elif type(data) is StringType:
            media_body = MediaIoBaseUpload(StringIO(data), mimetype='application/octet-stream')
        else:
            raise LoadError('Invalid data type')

        configuration = {
            'allowJaggedRows': options.get('allow_jagged_rows'),
            'allowQuotedNewlines': options.get('allow_quoted_newlines'),
            'createDisposition': options.get('create_disposition', 'CREATE_IF_NEEDED'),
            'destinationTable': {
                'projectId': options.get('project_id', self.project_id),
                'datasetId': options.get('dataset_id', self.dataset_id),
                'tableId': table_id,
            },
            'encoding': options.get('encoding', 'UTF-8'),
            'fieldDelimiter': options.get('field_delimiter', field_delimiter),
            'ignoreUnknownValues': options.get('ignore_unknown_values', False),
            'maxBadRecords': options.get('max_bad_records'),
            'projectionFields': options.get('projection_fields'),
            'quote': options.get('quote'),
            'skipLeadingRows': options.get('skip_leading_rows'),
            'sourceFormat': options.get('source_format', source_format),
            'sourceUris': options.get('source_uris', source_uris),
            'writeDisposition': options.get('write_disposition', 'WRITE_APPEND'),
        }
        if 'schema' in options:
            configuration['schema'] = {
                'fields': options['schema']
            }

        kwargs = {
            'projectId': self.project_id,
            'body': {
                'configuration': {
                    'load': configuration
                },
                'dryRun': options.get('dry_run', False)
            },
            'media_body': media_body
        }

        res = self.request('jobs', 'insert', **kwargs)

        job_id = res['jobReference']['jobId']
        if options.get('async') is True:
            return job_id
        else:
            return self.wait_job(job_id, timeout=options.get('timeout', BigQuery.JOB_WAIT_TIMEOUT))

    def insert_from_select(self, dest_table_id, query, **options):
        configuration = {
            'allowLargeResults': options.get('allow_large_results'),
            'createDisposition': options.get('create_disposition', 'CREATE_IF_NEEDED'),
            'destinationTable': {
                'projectId': options.get('dest_project_id', self.project_id),
                'datasetId': options.get('dest_dataset_id', self.dataset_id),
                'tableId': dest_table_id,
            },
            'flattenResults': options.get('flatten_results', True),
            'priority': options.get('priority', 'INTERACIVE'),
            'query': query,
            'tableDefinitions': options.get('table_definitions'),
            'useQueryCache': options.get('use_query_cache'),
            'userDefinedFunctionResources': options.get('user_defined_function_resources'),
            'writeDisposition': options.get('write_disposition', 'WRITE_EMPTY'),
        }
        if 'src_dataset_id' in options:
            configuration['defaultDataset'] = {
                'projectId': options.get('src_project_id', self.project_id),
                'datasetId': options.get('src_dataset_id'),
            }

        kwargs = {
            'projectId': self.project_id,
            'body': {
                'configuration': {
                    'query': configuration
                },
                'dryRun': options.get('dry_run', False)
            }
        }

        res = self.request('jobs', 'insert', **kwargs)

        job_id = res['jobReference']['jobId']
        if options.get('async') is True:
            return job_id
        else:
            return self.wait_job(job_id, timeout=options.get('timeout', BigQuery.JOB_WAIT_TIMEOUT))

    def extract(self, table_id, destination_uri, **options):
        destination_uris = []
        if type(destination_uri) is ListType:
            destination_uris = destination_uri
        else:
            destination_uris.append(destination_uri)

        (destination_format, field_delimiter, compression) = self.detect_file_format(destination_uris[0])

        configuration = {
            'compression': options.get('compression', compression),
            'destinationFormat': options.get('destination_format', destination_format),
            'destinationUris': destination_uris,
            'fieldDelimiter': options.get('field_delimiter', field_delimiter),
            'printHeader': options.get('print_header', True),
            'sourceTable': {
                'projectId': options.get('project_id', self.project_id),
                'datasetId': options.get('dataset_id', self.dataset_id),
                'tableId': table_id
            }
        }

        kwargs = {
            'projectId': self.project_id,
            'body': {
                'configuration': {
                    'extract': configuration
                }
            }
        }

        res = self.request('jobs', 'insert', **kwargs)

        job_id = res['jobReference']['jobId']
        if options.get('async') is True:
            return job_id
        else:
            return self.wait_job(job_id, timeout=options.get('timeout', BigQuery.JOB_WAIT_TIMEOUT))

    def get_query_results(self, job_id, **options):
        kwargs = {
            'projectId': self.project_id,
            'jobId': job_id,
            'maxResults': options.get('max_results', BigQuery.MAX_RESULTS),
            'pageToken': options.get('page_token'),
            'startIndex': options.get('start_index'),
            'timeoutMs': options.get('timeout_ms'),
        }

        res = self.request('jobs', 'getQueryResults', **kwargs)

        job_id = res['jobReference']['jobId']
        if options.get('async') is True:
            return job_id
        elif res['jobComplete'] is False:
            res = self.wait_job(job_id, timeout=options.get('timeout', BigQuery.JOB_WAIT_TIMEOUT))

        if 'rows' not in res:
            return []
        else:
            ret = []
            for row in res['rows']:
                ret.append([column['v'] for column in row['f']])
            if 'pageToken' in res:
                options['page_token'] = res['pageToken']
                options['start_index'] = options.get('start_index', 0) + len(res['rows'])
                ret.extend(self.get_query_results(job_id, **options))
            return ret

    def select(self, query, **options):
        kwargs = {
            'projectId': self.project_id,
            'body': {
                'query': query,
                'maxResults': options.get('max_results', BigQuery.MAX_RESULTS),
                'timeoutMs': options.get('timeout_ms'),
                'dryRun': options.get('dry_run', False),
                'useQueryCache': options.get('use_query_cache', True),
            }
        }

        if 'dataset_id' in options:
            kwargs['body']['defaultDataset'] = {
                'projectId': options.get('project_id', self.project_id),
                'datasetId': options.get('dataset_id')
            }

        res = self.request('jobs', 'query', **kwargs)

        job_id = res['jobReference']['jobId']
        if options.get('async') is True:
            return job_id
        elif res['jobComplete'] is False:
            res = self.wait_job(job_id, timeout=options.get('timeout', BigQuery.JOB_WAIT_TIMEOUT))

        if 'rows' not in res:
            return []
        else:
            ret = []
            for row in res['rows']:
                ret.append([column['v'] for column in row['f']])
            if 'pageToken' in res:
                ret.extend(self.get_query_results(res['jobReference']['jobId'], page_token=res['pageToken'],
                    start_index=len(res['rows'])))
            return ret

    def query(self, query, **options):
        return self.select(query, **options)
