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
import os
import re
import uuid
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds

from utils import constants


class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        for word in re.sub(r'\s', '', value).split(','):
            collector.collect((1, word))


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, word1 = input1
        count2, word2 = input2
        return (count1 + count2, word1)


class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


def generate_tmp_text_file(num_lines=100):
    tmp_f = open("/tmp/{}".format(uuid.uuid4().get_hex()), 'w')
    for iii in range(num_lines):
        tmp_f.write('111, 222, 333, 444, 555, 666, 777\n')
    return tmp_f


class Main:
    def run(self, flink):
        tmp_f = generate_tmp_text_file(constants.NUM_ELEMENTS_IN_TEST)
        try:
            env = flink.get_execution_environment()
            env.read_text_file(tmp_f.name) \
                .flat_map(Tokenizer()) \
                .key_by(Selector()) \
                .time_window(milliseconds(100)) \
                .reduce(Sum()) \
                .output()

            env.execute()
        finally:
            tmp_f.close()
            os.unlink(tmp_f.name)


def main(flink):
    Main().run(flink)
