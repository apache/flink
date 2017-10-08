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
import sys
import time
from org.apache.flink.api.common.functions import MapFunction
from org.apache.flink.streaming.api.collector.selector import OutputSelector
from org.apache.flink.streaming.api.functions.source import SourceFunction
from org.apache.flink.api.java.utils import ParameterTool
from org.apache.flink.streaming.python.api.environment import PythonStreamExecutionEnvironment

BOUND = 100
MAX_INT_START = int(BOUND / 2) - 1


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
        two_numbers = "{}, {}".format(random.randint(0, MAX_INT_START), random.randint(0, MAX_INT_START))
        ctx.collect(two_numbers)
        time.sleep(0.05)

    def cancel(self):
        self._running = False


class Fib(MapFunction):
    def map(self, value):
        return (value[0], value[1], value[3], value[2] + value[3], value[4] + 1)


class InPut(MapFunction):
    def map(self, value):
        num1, num2 = value.split(",")

        num1 = int(num1.strip())
        num2 = int(num2.strip())

        return (num1, num2, num1, num2, 0)


class OutPut(MapFunction):
    def map(self, value):
        return ((value[0], value[1]), value[4])


class StreamSelector(OutputSelector):
    def select(self, value):
        return ["iterate"] if value[3] < BOUND else ["output"]


class Main:
    def __init__(self, args):
        self._args = args

    def run(self):
        _params = ParameterTool.fromArgs(self._args)

        env = PythonStreamExecutionEnvironment.get_execution_environment()

        # create input stream of integer pairs
        if "--input" in self._args:
            try:
                file_path = _params.get("input")
                input_stream = env.read_text_file(file_path)
            except Exception as e:
                print("Error in reading input file. Exiting...", e)
                sys.exit(5)
        else:
            input_stream = env.add_source(Generator(num_iters=100))

        # create an iterative data stream from the input with 5 second timeout
        it = input_stream.map(InPut()).iterate(5000)

        # apply the step function to get the next Fibonacci number
        # increment the counter and split the output with the output selector
        step = it.map(Fib()).split(StreamSelector())
        it.close_with(step.select("iterate"))

        # to produce the final output select the tuples directed to the
        # 'output' channel then get the input pairs that have the greatest iteration counter
        # on a 1 second sliding window
        output = step.select("output")
        parsed_output = output.map(OutPut())
        if "--output" in self._args:
            try:
                file_path = _params.get("output")
                parsed_output.write_as_text(file_path)
            except Exception as e:
                print("Error in writing to output file. Printing to the console instead.", e)
                parsed_output.print()
        else:
            parsed_output.print()
        result = env.execute("Fibonacci Example (py)", True if _params.has("local") else False)
        print("Fibonacci job completed, job_id={}".format(result.jobID))


def main():
    argv = sys.argv[1:] if len(sys.argv) > 1 else []
    Main(argv).run()


if __name__ == '__main__':
    main()
