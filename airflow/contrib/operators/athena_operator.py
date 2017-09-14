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

import logging

from airflow.contrib.hooks.athena_hook import AthenaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class AthenaOperator(BaseOperator):
    """
    Executes sql code on Athena

    :param athena_conn_id: reference to a specific Athena instance
    :type athena_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self, sql, athena_conn_id='athena_default', parameters=None, *args, **kwargs):
        super(AthenaOperator, self).__init__(*args, **kwargs)
        self.athena_conn_id = athena_conn_id
        if type(sql) is not list:
            raise AirflowException('sql must be a list of statements')
        self.sql = sql
        self.parameters = parameters

    def execute(self, context):
        hook = AthenaHook(athena_conn_id=self.athena_conn_id)

        for s in self.sql:
            logging.info('Executing: ' + str(s))

            hook.run(
                s,
                parameters=self.parameters
            )
