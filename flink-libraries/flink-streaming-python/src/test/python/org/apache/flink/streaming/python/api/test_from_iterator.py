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
from java.util import NoSuchElementException
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.python.util import PythonIterator
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds

from utils import constants
from utils.python_test_base import TestBase


class SomeIterator(PythonIterator):
    def __init__(self, n):
        self._iii = 0
        self._nnn = n

    def hasNext(self):
        return self._iii < self._nnn

    def next(self):
        if self._iii < self._nnn:
            i = self._iii
            self._iii += 1
            return 111 if i % 2 == 0 else 222
        else:
            raise NoSuchElementException()


class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        collector.collect((1, value))


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)


class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


class Main(TestBase):
    def __init__(self):
        super(Main, self).__init__()

    def run(self):
        env = self._get_execution_environment()
        env.from_collection(SomeIterator(constants.NUM_ITERATIONS_IN_TEST)) \
            .flat_map(Tokenizer()) \
            .key_by(Selector()) \
            .time_window(milliseconds(10)) \
            .reduce(Sum()) \
            .print()

        result = env.execute("MyJob", True)
        print("Job completed, job_id={}".format(result.jobID))


def main():
    Main().run()


if __name__ == '__main__':
    main()
