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

from pyflink.common.time import Instant

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, TableDescriptor, Schema, StreamTableEnvironment)
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def session_window_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # define the source with watermark definition
    ds = env.from_collection(
        collection=[
            (Instant.of_epoch_milli(1000), 'Alice', 110.1),
            (Instant.of_epoch_milli(4000), 'Bob', 30.2),
            (Instant.of_epoch_milli(3000), 'Alice', 20.0),
            (Instant.of_epoch_milli(2000), 'Bob', 53.1),
            (Instant.of_epoch_milli(8000), 'Bob', 16.1),
            (Instant.of_epoch_milli(10000), 'Alice', 20.1)
        ],
        type_info=Types.ROW([Types.INSTANT(), Types.STRING(), Types.FLOAT()]))

    table = t_env.from_data_stream(
        ds,
        Schema.new_builder()
              .column_by_expression("ts", "CAST(f0 AS TIMESTAMP(3))")
              .column("f1", DataTypes.STRING())
              .column("f2", DataTypes.FLOAT())
              .watermark("ts", "ts - INTERVAL '3' SECOND")
              .build()
    ).alias("ts, name, price")

    # define the sink
    t_env.create_temporary_table(
        'sink',
        TableDescriptor.for_connector('print')
                       .schema(Schema.new_builder()
                               .column('name', DataTypes.STRING())
                               .column('total_price', DataTypes.FLOAT())
                               .column('w_start', DataTypes.TIMESTAMP_LTZ())
                               .column('w_end', DataTypes.TIMESTAMP_LTZ())
                               .build())
                       .build())

    # define the session window operation
    table = table.window(Session.with_gap(lit(5).seconds).on(col("ts")).alias("w")) \
                 .group_by(table.name, col('w')) \
                 .select(table.name, table.price.sum, col("w").start, col("w").end)

    # submit for execution
    table.execute_insert('sink') \
         .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    session_window_demo()
