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
import sys

import argparse
from typing import Iterable

from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy

from pyflink.common import Types, Encoder
from pyflink.datastream import StreamExecutionEnvironment, WindowFunction
from pyflink.datastream.window import CountWindow


class SumWindowFunction(WindowFunction[tuple, tuple, str, CountWindow]):
    def apply(self, key: str, window: CountWindow, inputs: Iterable[tuple]):
        result = 0
        for i in inputs:
            result += i[0]
        return [(key, result)]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    output_path = known_args.output

    env = StreamExecutionEnvironment.get_execution_environment()
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    data_stream = env.from_collection([
        (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=Types.TUPLE([Types.INT(), Types.STRING()]))

    ds = data_stream.key_by(lambda x: x[1], key_type=Types.STRING()) \
        .count_window(2) \
        .apply(SumWindowFunction(), Types.TUPLE([Types.STRING(), Types.INT()]))

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
