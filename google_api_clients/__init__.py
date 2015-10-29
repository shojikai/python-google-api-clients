import httplib2
import json
import re

from apiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from types import ListType

from .errors import MethodNameError
from .errors import ResourceNameError

class GoogleApiClient(object):

    def __init__(self, **options):
        self.service_account = options.get('service_account')
        self.private_key = options.get('private_key')
        self.project_id = options.get('project_id')

    def auth(self):
        self.credentials = GoogleCredentials.get_application_default()
        return self

    def build(self, api_name, api_version, **options):
        self.discovery_uri = 'https://www.googleapis.com/discovery/v1/apis/%s/%s/rest' % (api_name, api_version)
        (resp_headers, content) = httplib2.Http().request(self.discovery_uri)
        self.rest_description = json.loads(content)
        credentials = options.get('credentials', self.credentials)
        self.service = discovery.build(api_name, api_version, credentials=credentials)
        return self

    def request(self, resource, method, **kwargs):
        if type(resource) is not ListType:
            resources = [resource]
        else:
            resources = resource

        rest_description = self.rest_description['resources']
        service = self.service
        for i,r in enumerate(resources):
            if i != 0:
                rest_description = rest_description['resources']
            if not r in rest_description:
                raise ResourceNameError(r)
            rest_description = rest_description[r]
            service = getattr(service, r)()

        if not method in rest_description['methods']:
            raise MethodNameError(method)

        rest_description = rest_description['methods'][method]

        parameters = {}
        for parameter in rest_description['parameters']:
            if parameter in kwargs:
                parameters[parameter] = kwargs[parameter]
            #elif 'required' in rest_description['parameters'][parameter]:
            #    raise RequiredParameterIsMissingError(parameter)

        if 'body' in kwargs:
            parameters['body'] = kwargs['body']
        if 'media_body' in kwargs:
            parameters['media_body'] = kwargs['media_body']

        return getattr(service, method)(**parameters).execute()
