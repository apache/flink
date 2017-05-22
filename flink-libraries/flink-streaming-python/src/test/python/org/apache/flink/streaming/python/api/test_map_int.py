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
from org.apache.flink.api.common.functions import MapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import seconds

from utils import constants
from utils.pygeneratorbase import PyGeneratorBase
from utils.python_test_base import TestBase


class Generator(PyGeneratorBase):
    def __init__(self, num_iters):
        super(Generator, self).__init__(num_iters)
        self._alternator = True

    def do(self, ctx):
        ctx.collect(10 if self._alternator else -10)
        self._alternator = not self._alternator


class Tokenizer(MapFunction):
    def map(self, value):
        return (1, 2 * value)


class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)


class Main(TestBase):
    def __init__(self):
        super(Main, self).__init__()

    def run(self):
        env = self._get_execution_environment()
        env.add_source(Generator(num_iters=constants.NUM_ITERATIONS_IN_TEST)) \
            .map(Tokenizer()) \
            .key_by(Selector()) \
            .time_window(seconds(1)) \
            .reduce(Sum()) \
            .print()

        env.execute(True)


def main():
    Main().run()


if __name__ == '__main__':
    main()
    print("Job completed ({})\n".format(sys.argv))
