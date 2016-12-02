# -*- coding: utf-8 -*-
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

import json
import pandas as pd
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.contrib.hooks.kafka_hook import KafkaHook


class MsSqlToKafka(BaseOperator):
    """
    Copy data from SqlServer to Kafka in JSON format.
    """
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 sql,
                 kafka_topic,
                 kafka_conn_id='kafka_default',
                 mssql_conn_id='mssql_default',
                 *args,
                 **kwargs):
        """
        :param sql: The SQL to execute on the SqlServer table.
        :type sql: string
        :param kafka_conn: Reference to a specific Kafka hook.
        :type kafka_conn: string
        :param kafka_topic: The topic to produce results to.
        :type kafka_topic: string
        :param mssql_conn_id: Reference to a specific SqlServer hook.
        :type mssql_conn_id: string
        """
        super(MsSqlToKafka, self).__init__(*args, **kwargs)
        self.sql = sql
        self.kafka_conn_id = kafka_conn_id
        self.kafka_topic = kafka_topic
        self.mssql_conn_id = mssql_conn_id

    def execute(self, context):
        kafka_conn = self._get_kafka_producer()

        with kafka_conn.get_producer() as producer:
            logging.info("Got kafka producer {0}".format(producer))
            for df in self._query_mssql():
                logging.info("Loading query chunk {0}".format(df))
                try:
                    logging.info("Loading chunk into json")
                    msgs = json.loads(df.to_json(orient='records',date_format='iso'))
                except Exception as e:
                    logging.info("Exception found when loading dataframe to json: {0}".format(e))
                logging.info("Loaded {0} messages".format(len(msgs)))

                for msg in msgs:
                    producer.produce(bytes(json.dumps(msg)))
                logging.info("Finished producing messages for chunk {0}".format(producer))
            logging.info("Exiting kafka hook execution")

    def _get_kafka_producer(self):
        hook = KafkaHook(kafka_topic=self.kafka_topic,
                         kafka_conn_id=self.kafka_conn_id)

        return hook.get_conn()

    def _query_mssql(self):
        """
        Query the mssql instance using the mssql hook and return a dataframe.
        Using a dataframe makes working with JSON objects much easier.

        :return df: the dataframe that relates to the sql query.
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn()
        logging.info("Connected to mssql db {0}".format(conn))


        #CHANGE THIS TO MSSQL.GETPANDASDF!
        try:
            for df in pd.read_sql(self.sql, conn, chunksize=10000):
                yield df
        except Exception as e:
            logging.exception("Error reading from mssql: {0}".format(e))
