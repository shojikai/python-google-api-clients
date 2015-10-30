import base64
import json
import os
import sys
import unittest

from pprint import pprint

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/../')

from google_api_clients.pubsub import PubSub
from google_api_clients.pubsub.errors import AcknowledgeError
from google_api_clients.pubsub.errors import NotFoundError

class PubSubTest(unittest.TestCase):

    def setUp(self):
        self.project_id = os.getenv('PROJECT_ID')
        self.topic = os.getenv('TOPIC', 'test_topic')
        self.subscription = os.getenv('SUBSCRIPTION', 'test_subscription')
        if self.project_id is None:
            raise RuntimeError('PROJECT_ID is not defined.')
        self.pubsub = PubSub(self.project_id)

    def TearDown(self):
        pass

    def test_error(self):
        # exists topic?
        if self.pubsub.exists_topic(self.topic):
            # delete topic
            res = self.pubsub.drop_topic(self.topic)
            self.assertFalse(bool(res))

        # exists subscription?
        if self.pubsub.exists_subscription(self.subscription):
            # delete subscription
            res = self.pubsub.drop_subscription(self.subscription)
            self.assertFalse(bool(res))

        # info topic (NotFound)
        res = self.pubsub.info_topic(self.topic)
        self.assertFalse(bool(res))

        # info subscription (NotFound)
        res = self.pubsub.info_subscription(self.subscription)
        self.assertFalse(bool(res))

        # list topic subscriptions (NotFound)
        res = self.pubsub.list_topic_subscriptions(self.topic)
        self.assertFalse(bool(res))

        # publish (NotFound)
        messages = [
            json.dumps({ 'id': 1, 'name': 'foo' }),
            json.dumps({ 'id': 2, 'name': 'bar' }),
            json.dumps({ 'id': 3, 'name': 'baz' }),
        ]
        with self.assertRaises(NotFoundError):
            res = self.pubsub.publish(self.topic, messages)

        # pull (NotFound)
        with self.assertRaises(NotFoundError):
            res = self.pubsub.pull(self.subscription)

        # acknowledge (NotFound)
        with self.assertRaises(NotFoundError):
            res = self.pubsub.ack(self.subscription, None)

        # create topic
        res = self.pubsub.create_topic(self.topic)
        self.assertTrue(bool(res))

        # create topic (AlreadyExists)
        res = self.pubsub.create_topic(self.topic)
        self.assertFalse(bool(res))

        # create subscription
        res = self.pubsub.create_subscription(self.subscription, self.topic)
        self.assertTrue(bool(res))

        # create subscription (AlreadyExists)
        res = self.pubsub.create_subscription(self.subscription, self.topic)
        self.assertFalse(bool(res))

        # publish
        messages = [
            json.dumps({ 'id': 1, 'name': 'foo' }),
            json.dumps({ 'id': 2, 'name': 'bar' }),
            json.dumps({ 'id': 3, 'name': 'baz' }),
        ]
        res = self.pubsub.publish(self.topic, messages)
        self.assertTrue(bool(res))

        # pull
        res = self.pubsub.pull(self.subscription)
        ack_ids = []
        messages = []
        message_ids = []
        for received_message in res['receivedMessages']:
            ack_ids.append(received_message['ackId'])
            messages.append(base64.b64decode(received_message['message']['data']))
            message_ids.append(received_message['message']['messageId'])

        # acknowledge (AcknowledgeError)
        dummy_ack_ids = ['dummy_' + x for x in ack_ids]
        with self.assertRaises(AcknowledgeError):
            res = self.pubsub.ack(self.subscription, dummy_ack_ids)

        # acknowledge
        res = self.pubsub.ack(self.subscription, ack_ids)
        self.assertFalse(bool(res))

        # acknowledge (2nd)
        res = self.pubsub.ack(self.subscription, ack_ids)
        self.assertFalse(bool(res))

        # drop subscription
        res = self.pubsub.drop_subscription(self.subscription)
        self.assertFalse(bool(res))

        # drop subscription (NotFound)
        res = self.pubsub.drop_subscription(self.subscription)
        self.assertFalse(bool(res))

        # drop topic
        res = self.pubsub.drop_topic(self.topic)
        self.assertFalse(bool(res))

        # drop topic (NotFound)
        res = self.pubsub.drop_topic(self.topic)
        self.assertFalse(bool(res))

    def test_normal(self):
        # exists topic?
        if self.pubsub.exists_topic(self.topic):
            # delete topic
            res = self.pubsub.drop_topic(self.topic)
            self.assertFalse(bool(res))

        # create topic
        res = self.pubsub.create_topic(self.topic)
        self.assertTrue(bool(res))
        pprint(res)

        # info topic
        res = self.pubsub.info_topic(self.topic)
        self.assertTrue(bool(res))
        pprint(res)

        # list topics
        res = self.pubsub.list_topics()
        self.assertIn(self.topic, res)
        pprint(res)

        # exists subscription?
        if self.pubsub.exists_subscription(self.subscription):
            # delete subscription
            res = self.pubsub.drop_subscription(self.subscription)
            pprint(res)

        # create subscription
        res = self.pubsub.create_subscription(self.subscription, self.topic)
        print(res)

        # info subscription
        res = self.pubsub.info_subscription(self.subscription)
        self.assertTrue(bool(res))
        pprint(res)

        # list subscriptions
        res = self.pubsub.list_subscriptions()
        self.assertIn(self.subscription, res)
        pprint(res)

        # list topic subscriptions
        res = self.pubsub.list_topic_subscriptions(self.topic)
        self.assertIn(self.subscription, res)
        pprint(res)

        # publish
        messages = [
            json.dumps({ 'id': 1, 'name': 'foo' }),
            json.dumps({ 'id': 2, 'name': 'bar' }),
            json.dumps({ 'id': 3, 'name': 'baz' }),
        ]
        res = self.pubsub.publish(self.topic, messages)
        pprint(res)

        # pull
        res = self.pubsub.pull(self.subscription)
        ack_ids = []
        for received_message in res['receivedMessages']:
            ack_id = received_message['ackId']
            message = base64.b64decode(received_message['message']['data'])
            message_id = received_message['message']['messageId']
            print(ack_id, message, message_id)
            ack_ids.append(ack_id)

        # acknowledge
        res = self.pubsub.ack(self.subscription, ack_ids)
        pprint(res)
        
        # drop subscription
        res = self.pubsub.drop_subscription(self.subscription)
        pprint(res)

        # drop topic
        res = self.pubsub.drop_topic(self.topic)
        self.assertFalse(bool(res))

if __name__ == '__main__':
    unittest.main()
