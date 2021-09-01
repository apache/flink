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

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col, row_interval, CURRENT_ROW
from pyflink.table.window import Over


def tumble_window_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # define the source with watermark definition
    t_env.execute_sql("""
        CREATE TABLE source (
            ts TIMESTAMP(3),
            name STRING,
            price FLOAT,
            watermark FOR ts as ts - INTERVAL '3' SECOND
        ) with (
            'connector' = 'datagen',
            'number-of-rows' = '10'
        )
    """)

    # define the sink
    t_env.execute_sql("""
        CREATE TABLE sink (
            name STRING,
            total_price FLOAT
        ) with (
            'connector' = 'print'
        )
    """)

    table = t_env.from_path("source")

    # define the over window operation
    table = table.over_window(
        Over.partition_by("name")
            .order_by("ts")
            .preceding(row_interval(2))
            .following(CURRENT_ROW)
            .alias('w')) \
        .select(table.name, table.price.max.over(col('w')))

    # submit for execution
    table.execute_insert('sink') \
         .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    tumble_window_demo()
