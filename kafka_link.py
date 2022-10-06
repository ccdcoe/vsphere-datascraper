from kafka.errors import KafkaError
from kafka import KafkaProducer
from datetime import datetime
import json
import copy

class KafkaLink:
    def __init__(self, kafka_host, kafka_topic, kafka_compression, ip_split):
        self.kafka_host = kafka_host
        self.kafka_topic = kafka_topic
        self.kafka_compression = kafka_compression
        self.ip_split = ip_split

    def connect(self):
        self.kafka = KafkaProducer(bootstrap_servers=self.kafka_host, retries=5, compression_type=self.kafka_compression)

    def push_to_server(self, info):
        if not self.ip_split: #Normal operation
            self.publish_to_kafka(info)

        else: #Split each IP per one line in Kafka, backward compatibility with older XS software
            line = copy.deepcopy(info)
            line.pop('nic', [])

            nics = info.get('nic', [])
            for nic in nics:
                line['mac'] = nic.get('mac')
                line['nic_connected'] = nic.get('connected')
                ips = nic.get('IP', [])
                for ip in ips:
                    line['ip'] = ip
                    self.publish_to_kafka(line) #Push single lines with one IP to kafka


    def publish_to_kafka(self, line):
        try:
            self.kafka.send(self.kafka_topic, key=bytes(line.get("name", "Unknown"), 'utf-8'), value=bytes(json.dumps(line), 'utf-8'))
        except Exception as e:
            print('Unable to publish to Kafka server: ', e)

    def push_all_to_server(self, docs):
        for success, info in parallel_bulk(self.es, self._pre_process_docs(docs)):
            if not success:
                print('Push failed: ', info)


    def _pre_process_docs(self, docs):
        for doc in docs:
            yield {
                "ts": timestamp,
                "ansible_name": name, 
                "host_name": hostname, 
                "domain": domain, 
                "ip": ip, 
                "mac": mac, 
                "team": team
            }
