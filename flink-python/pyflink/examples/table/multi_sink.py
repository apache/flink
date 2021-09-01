################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import logging
import sys

from pyflink.table import (EnvironmentSettings, TableEnvironment, DataTypes)
from pyflink.table.udf import udf


def multi_sink():
    t_env = TableEnvironment.create(EnvironmentSettings.new_instance().in_streaming_mode().build())

    table = t_env.from_elements(
        elements=[(1, 'Hello'), (2, 'World'), (3, "Flink"), (4, "PyFlink")],
        schema=['id', 'data'])

    # define the sink tables
    t_env.execute_sql("""
        CREATE TABLE first_sink (
            id BIGINT,
            data VARCHAR
        ) WITH (
            'connector' = 'print'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE second_sink (
            id BIGINT,
            data VARCHAR
        ) WITH (
            'connector' = 'print'
        )
    """)

    # create a statement set
    statement_set = t_env.create_statement_set()

    # emit the data with id <= 3 to the "first_sink" via sql statement
    statement_set.add_insert_sql("INSERT INTO first_sink SELECT * FROM %s WHERE id <= 3" % table)

    # emit the data which contains "Flink" to the "second_sink"
    @udf(result_type=DataTypes.BOOLEAN())
    def contains_flink(data):
        return "Flink" in data

    second_table = table.where(contains_flink(table.data))
    statement_set.add_insert("second_sink", second_table)

    # execute the statement set
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details
    statement_set.execute().wait()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    multi_sink()
