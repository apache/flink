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

from flink.plan.Environment import get_environment
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction


class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        for word in value.lower().split():
            collector.collect((1, word))


class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))


if __name__ == "__main__":
    env = get_environment()
    if len(sys.argv) != 1 and len(sys.argv) != 3:
        sys.exit("Usage: ./bin/pyflink.sh WordCount[ - <text path> <result path>]")

    if len(sys.argv) == 3:
        data = env.read_text(sys.argv[1])
    else:
        data = env.from_elements("hello","world","hello","car","tree","data","hello")

    result = data \
        .flat_map(Tokenizer()) \
        .group_by(1) \
        .reduce_group(Adder(), combinable=True) \

    if len(sys.argv) == 3:
        result.write_csv(sys.argv[2])
    else:
        result.output()

    env.set_parallelism(1)

    env.execute(local=True)