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
from org.apache.flink.api.common.functions import FilterFunction
from org.apache.flink.api.common.functions import MapFunction

from utils import constants


class MinusOne(MapFunction):
    def map(self, value):
        return value - 1


class PositiveNumber(FilterFunction):
    def filter(self, value):
        return value > 0


class LessEquelToZero(FilterFunction):
    def filter(self, value):
        return value <= 0


class Main:
    def run(self, flink):
        env = flink.get_execution_environment()
        some_integers = env.from_collection([2] * 5)

        iterative_stream = some_integers.iterate(constants.MAX_EXECUTION_TIME_MS)

        minus_one_stream = iterative_stream.map(MinusOne())

        still_greater_then_zero = minus_one_stream.filter(PositiveNumber())

        iterative_stream.close_with(still_greater_then_zero)

        less_then_zero = minus_one_stream.filter(LessEquelToZero())

        less_then_zero.output()

        env.execute()


def main(flink):
    Main().run(flink)
