from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from datetime import datetime

class ElasticLink:
    def __init__(self, ela_host, ela_index):
        self.ela_host = ela_host
        self.ela_index = ela_index

    def connect(self):
        self.es = Elasticsearch(self.ela_host)

    def push_to_server(self, info):
        try:
            bulk(self.es, [{
                    '_op_type': 'create',
                    '_index': self.ela_index,
                    '@timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z',
                    'info': info
                }])
        except Exception as e:
            print('Unable to push to Elasticsearch server: ', e)

    def push_all_to_server(self, docs):
        for success, info in parallel_bulk(self.es, self._pre_process_docs(docs)):
            if not success:
                print('Push failed: ', info)

    def _pre_process_docs(self, docs):
        for doc in docs:
            yield {
                '_index': self.ela_index,
                '_timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z',
                '_id': doc['name'],
                'doc': doc
            }
