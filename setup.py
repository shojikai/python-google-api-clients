from setuptools import setup

setup(
    name='python-google-api-clients',
    packages=[
        'google_api_clients',
        'google_api_clients.bigquery',
        'google_api_clients.pubsub',
    ],
    version='0.0.1',
    author='Shoji Kai',
    author_email='sho2kai@gmail.com',
    description='Google API Clients for Python',
    url='https://github.com/shojikai/python-google-api-clients',
    test_suite='test'
)
