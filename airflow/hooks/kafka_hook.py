# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Ported to Airflow by Bolke de Bruin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.hooks.base_hook import BaseHook
import pykafka
import json


class KafkaHook(BaseHook):
    """
    Interact with Kafka, send it a bunch of messages and it produces them into
    a given topic.

    :param kafka_topic: the topic to produce to
    :type kafka_topic: str
    :param kafka_conn_id: the airflow connection
    :type kafka_conn_id: str
    """
    default_host = 'localhost'
    default_port = 9092

    def __init__(self, kafka_topic, kafka_conn_id='kafka_default'):
        self.conn = self.get_connection(kafka_conn_id)
        self.topic = kafka_topic
        self.producer = None

    def get_conn(self):
        host = self.conn.host or self.default_host
        port = self.conn.port or self.default_port

        server = '{host}:{port}'.format(**locals())
        producer = pykafka.KafkaClient(hosts=server)

        return producer.topics[self.topic]

    def produce(self, producer, msg):
        """
        Produce messages into a given topic.

        :param msgs: a list of messages.
        type list:
        """
        producer = self.get_conn()

        with producer.get_producer() as producer:
            for msg in msgs:
                producer.produce(bytes(json.dumps(msg)))

    def consume(self):
        """
        TODO: Implement this.
        """
        raise NotImplementedError
