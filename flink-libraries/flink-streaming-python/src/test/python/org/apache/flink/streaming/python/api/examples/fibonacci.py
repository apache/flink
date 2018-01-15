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

#
# An example that illustrates iterations in Flink streaming. The program sums up random numbers and counts
# additions. The operation is done until it reaches a specific threshold in an iterative streaming fashion.
#

import random
import argparse

from org.apache.flink.api.common.functions import MapFunction
from org.apache.flink.streaming.api.collector.selector import OutputSelector
from org.apache.flink.streaming.api.functions.source import SourceFunction

TARGET_VAL = 100
MAX_INT_START = 50


class Generator(SourceFunction):
    def __init__(self, num_iters=1000):
        self._running = True
        self._num_iters = num_iters

    def run(self, ctx):
        counter = 0
        while self._running and counter < self._num_iters:
            self.do(ctx)
            counter += 1

    def do(self, ctx):
        two_numbers = "{}, {}".format(random.randrange(1, MAX_INT_START), random.randrange(1, MAX_INT_START))
        ctx.collect(two_numbers)

    def cancel(self):
        self._running = False


class Step(MapFunction):
    def map(self, value):
        return (value[0], value[1], value[3], value[2] + value[3], value[4] + 1)


class InputMap(MapFunction):
    def map(self, value):
        num1, num2 = value.split(",")

        num1 = int(num1.strip())
        num2 = int(num2.strip())

        return (num1, num2, num1, num2, 0)


class OutPut(MapFunction):
    def map(self, value):
        return ((value[0], value[1]), value[4])


class Selector(OutputSelector):
    def select(self, value):
        return ["iterate"] if value[2] < TARGET_VAL and value[3] < TARGET_VAL else ["output"]


class Main:
    def run(self, flink, args):
        env = flink.get_execution_environment()

        # create input stream of integer pairs
        if args.input:
            input_stream = env.read_text_file(args.input)
        else:
            input_stream = env.create_python_source(Generator(num_iters=50))

        # create an iterative data stream from the input with 5 second timeout
        it = input_stream\
            .map(InputMap())\
            .iterate(5000)

        # apply the step function to get the next Fibonacci number
        # increment the counter and split the output with the output selector
        step = it\
            .map(Step())\
            .split(Selector())

        # close the iteration by selecting the tuples that were directed to the
        # 'iterate' channel in the output selector
        it.close_with(step.select("iterate"))

        # to produce the final output select the tuples directed to the
        # 'output' channel then get the input pairs that have the greatest iteration counter
        # on a 1 second sliding window
        output = step.select("output")
        parsed_output = output.map(OutPut())
        if args.output:
            parsed_output.write_as_text(args.output)
        else:
            parsed_output.output()
        result = env.execute("Fibonacci Example (py)")
        print("Fibonacci job completed, job_id={}".format(result.jobID))


def main(flink):
    parser = argparse.ArgumentParser(description='Fibonacci.')
    parser.add_argument('--input', metavar='IN', help='input file path')
    parser.add_argument('--output', metavar='OUT', help='output file path')
    args = parser.parse_args()
    Main().run(flink, args)
