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
import argparse
import logging
import sys

from pyflink.common import Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (FileSink, OutputFileConfig, RollingPolicy)
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, DataTypes

words = ["flink", "window", "timer", "event_time", "processing_time", "state",
         "connector", "pyflink", "checkpoint", "watermark", "sideoutput", "sql",
         "datastream", "broadcast", "asyncio", "catalog", "batch", "streaming"]

max_word_id = len(words) - 1


def word_count(output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # define the source
    # randomly select 5 words per second from a predefined list
    t_env.create_temporary_table(
        'source',
        TableDescriptor.for_connector('datagen')
                       .schema(Schema.new_builder()
                               .column('word_id', DataTypes.INT())
                               .build())
                       .option('fields.word_id.kind', 'random')
                       .option('fields.word_id.min', '0')
                       .option('fields.word_id.max', str(max_word_id))
                       .option('rows-per-second', '5')
                       .build())

    table = t_env.from_path('source')
    ds = t_env.to_data_stream(table)

    def id_to_word(r):
        # word_id is the first column of the input row
        return words[r[0]]

    # compute word count
    ds = ds.map(id_to_word) \
           .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
           .key_by(lambda i: i[0]) \
           .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    word_count(known_args.output)
