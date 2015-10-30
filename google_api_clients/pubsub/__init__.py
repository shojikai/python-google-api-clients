import base64
import re

from types import ListType

from googleapiclient.errors import HttpError

from .. import GoogleApiClient
from .errors import AcknowledgeError
from .errors import AlreadyExistsError
from .errors import NotFoundError

class PubSub(GoogleApiClient):

    MAX_MESSAGES = 100000

    def __init__(self, project_id, **options):
        super(PubSub, self).__init__(project_id=project_id, **options)
        self.auth().build('pubsub', 'v1')

    def request(self, resource, method, **kwargs):
        try:
            res = super(PubSub, self).request(resource, method, **kwargs)
        except HttpError as e:
            if e.resp.status == 404 and re.search(r'not found', str(e), re.I):
                raise NotFoundError(e)
            elif e.resp.status == 409 and re.search(r'already exists', str(e), re.I):
                raise AlreadyExistsError(e)
            else:
                raise e
        return res

    def info_topic(self, topic, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'topic': 'projects/' + project_id + '/topics/' + topic
        }
        try:
            return self.request(['projects', 'topics'], 'get', **kwargs)
        except NotFoundError:
            return {}

    def exists_topic(self, topic, **options):
        return bool(self.info_topic(topic, **options))

    def drop_topic(self, topic, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'topic': 'projects/' + project_id + '/topics/' + topic
        }
        try:
            return self.request(['projects', 'topics'], 'delete', **kwargs)
        except NotFoundError:
            return {}

    def create_topic(self, topic, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'name': 'projects/' + project_id + '/topics/' + topic,
            'body': {}
        }
        try:
            return self.request(['projects', 'topics'], 'create', **kwargs)
        except AlreadyExistsError:
            return {}

    def list_topics(self, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'project': 'projects/' + project_id,
            'pageSize': options.get('page_size'),
            'pageToken': options.get('page_token'),
        }
        res = self.request(['projects', 'topics'], 'list', **kwargs)
        if 'topics' not in res:
            return []
        ret = [x['name'].split('/')[-1] for x in res['topics']]
        if 'nextPageToken' in res:
            options['page_token'] = res['nextPageToken']
            ret.extend(self.list_topics(**options))
            return ret
        return ret

    def publish(self, topic, message, **options):
        messages = []
        if type(message) is ListType:
            messages = message
        else:
            messages.append(message)
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'topic': 'projects/' + project_id + '/topics/' + topic,
            'body': {
                'messages': [ { 'data': base64.b64encode(str(x)) } for x in messages ],
            }
        }
        return self.request(['projects', 'topics'], 'publish', **kwargs)

    def info_subscription(self, subscription, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'subscription': 'projects/' + project_id + '/subscriptions/' + subscription
        }
        try:
            return self.request(['projects', 'subscriptions'], 'get', **kwargs)
        except NotFoundError:
            return {}

    def exists_subscription(self, subscription, **options):
        return bool(self.info_subscription(subscription, **options))

    def drop_subscription(self, subscription, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'subscription': 'projects/' + project_id + '/subscriptions/' + subscription
        }
        try:
            return self.request(['projects', 'subscriptions'], 'delete', **kwargs)
        except NotFoundError:
            return {}

    def create_subscription(self, subscription, topic, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'name': 'projects/' + project_id + '/subscriptions/' + subscription,
            'body': {
                'topic': 'projects/' + project_id + '/topics/' + topic,
                'pushConfig': options.get('push_config'),
                'ackDeadlineSeconds': options.get('ack_deadline_seconds', 10),
            }
        }
        try:
            return self.request(['projects', 'subscriptions'], 'create', **kwargs)
        except AlreadyExistsError:
            return {}

    def list_subscriptions(self, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'project': 'projects/' + project_id,
            'pageSize': options.get('page_size'),
            'pageToken': options.get('page_token'),
        }
        res = self.request(['projects', 'subscriptions'], 'list', **kwargs)
        if 'subscriptions' not in res:
            return []
        ret = [x['name'].split('/')[-1] for x in res['subscriptions']]
        if 'nextPageToken' in res:
            options['page_token'] = res['nextPageToken']
            ret.extend(self.list_subscriptions(**options))
            return ret
        return ret

    def list_topic_subscriptions(self, topic, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'topic': 'projects/' + project_id + '/topics/' + topic
        }
        try:
            res = self.request(['projects', 'topics', 'subscriptions'], 'list', **kwargs)
        except NotFoundError:
            return {}
        if 'subscriptions' not in res:
            return []
        ret = [x.split('/')[-1] for x in res['subscriptions']]
        if 'nextPageToken' in res:
            options['page_token'] = res['nextPageToken']
            ret.extend(self.list_topic_subscriptions(topic, **options))
            return ret
        return ret

    def pull(self, subscription, **options):
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'subscription': 'projects/' + project_id + '/subscriptions/' + subscription,
            'body': {
                'returnImmediately': options.get('return_immediately', True),
                'maxMessages': options.get('max_messages', PubSub.MAX_MESSAGES),
            }
        }
        return self.request(['projects', 'subscriptions'], 'pull', **kwargs)

    def acknowledge(self, subscription, ack_id, **options):
        ack_ids = []
        if type(ack_id) is ListType:
            ack_ids = ack_id
        else:
            ack_ids.append(ack_id)
        project_id = options.get('project_id', self.project_id)
        kwargs = {
            'subscription': 'projects/' + project_id + '/subscriptions/' + subscription,
            'body': {
                'ackIds': ack_ids
            }
        }
        try:
            return self.request(['projects', 'subscriptions'], 'acknowledge', **kwargs)
        except NotFoundError:
            raise
        except HttpError as e:
            raise AcknowledgeError(e)

    def ack(self, subscription, ack_id, **options):
        return self.acknowledge(subscription, ack_id, **options)

