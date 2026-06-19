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
import asyncio
import functools
import json
import logging
import random
import sys
from typing import List

from pyflink.common import Encoder, Types, Time, Row
from pyflink.datastream import StreamExecutionEnvironment, AsyncDataStream, AsyncFunction, \
    RuntimeContext, AsyncRetryStrategy, async_retry_predicates, CheckpointingMode
from pyflink.datastream.connectors.file_system import (FileSink, OutputFileConfig, RollingPolicy)
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, DataTypes


class AsyncLLMRequest(AsyncFunction[Row, str]):

    def __init__(self):
        self.retried_keys = {}

    def open(self, runtime_context: RuntimeContext):
        # create model inference client here
        pass

    def close(self):
        # close the model inference client here
        pass

    async def async_invoke(self, value: Row) -> List[str]:
        # issue the asynchronous request
        await asyncio.sleep(random.randint(1, 2))

        if value.user_id not in self.retried_keys and random.randint(1, 10) % 3 == 0:
            self.retried_keys[value.user_id] = True
            # remote model inference request may time out
            raise TimeoutError
        else:
            if value.user_id in self.retried_keys:
                del self.retried_keys[value.user_id]
            # remote model inference request completes
            # note that the result should be a collection even there is only one result
            analysis_result = "positive"
            result = {
                "user_id": value.user_id,
                "comments": value.comments,
                "analysis_result": analysis_result
            }
            return [json.dumps(result)]

    def timeout(self, value: Row) -> List[str]:
        # return a default value in case timeout
        result = {
            "user_id": value.user_id,
            "comments": value.comments,
            "analysis_result": None
        }
        return [json.dumps(result)]


def main(output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30000, CheckpointingMode.EXACTLY_ONCE)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # source: user_id, comments
    t_env.create_temporary_table(
        'source',
        TableDescriptor.for_connector('datagen')
                       .schema(Schema.new_builder()
                               .column('user_id', DataTypes.INT())
                               .column('comments', DataTypes.STRING())
                               .build())
                       .option('fields.user_id.kind', 'random')
                       .option('fields.comments.kind', 'random')
                       .option('rows-per-second', '100')
                       .build())

    table = t_env.from_path('source')
    ds = t_env.to_data_stream(table)

    # create an async retry strategy via utility class or a user defined strategy
    async_retry_strategy = AsyncRetryStrategy.fixed_delay(
        max_attempts=100,
        backoff_time_millis=1000,
        result_predicate=None,
        exception_predicate=functools.partial(async_retry_predicates.exception_type_predicate,
                                              expected_error_type=TimeoutError))

    result_stream = AsyncDataStream.unordered_wait_with_retry(
        data_stream=ds,
        async_function=AsyncLLMRequest(),
        timeout=Time.seconds(10),
        async_retry_strategy=async_retry_strategy,
        capacity=1000,
        output_type=Types.STRING())

    # define the sink
    if output_path is not None:
        result_stream.sink_to(
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
        result_stream.print()

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

    main(known_args.output)
