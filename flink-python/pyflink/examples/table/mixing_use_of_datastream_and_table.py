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

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, TableDescriptor, Schema, StreamTableEnvironment)
from pyflink.table.udf import udf


def mixing_use_of_datastream_and_table():
    # use StreamTableEnvironment instead of TableEnvironment when mixing use of table & datastream
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # define the source
    t_env.create_temporary_table(
        'source',
        TableDescriptor.for_connector('datagen')
                       .schema(Schema.new_builder()
                               .column('id', DataTypes.BIGINT())
                               .column('data', DataTypes.STRING())
                               .build())
                       .option("number-of-rows", "10")
                       .build())

    # define the sink
    t_env.create_temporary_table(
        'sink',
        TableDescriptor.for_connector('print')
                       .schema(Schema.new_builder()
                               .column('a', DataTypes.BIGINT())
                               .build())
                       .build())

    @udf(result_type=DataTypes.BIGINT())
    def length(data):
        return len(data)

    # perform table api operations
    table = t_env.from_path("source")
    table = table.select(table.id, length(table.data))

    # convert table to datastream and perform datastream api operations
    ds = t_env.to_data_stream(table)
    ds = ds.map(lambda i: i[0] + i[1], output_type=Types.LONG())

    # convert datastream to table and perform table api operations as you want
    table = t_env.from_data_stream(
        ds,
        Schema.new_builder().column("f0", DataTypes.BIGINT()).build())

    # execute
    table.execute_insert('sink') \
         .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    mixing_use_of_datastream_and_table()
