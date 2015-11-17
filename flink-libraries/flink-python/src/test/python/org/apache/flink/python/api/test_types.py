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
from flink.plan.Environment import get_environment
from flink.functions.MapFunction import MapFunction
from flink.functions.MapPartitionFunction import MapPartitionFunction
from flink.plan.Constants import BOOL, INT, FLOAT, STRING, BYTES


class Verify(MapPartitionFunction):
    def __init__(self, expected, name):
        super(Verify, self).__init__()
        self.expected = expected
        self.name = name

    def map_partition(self, iterator, collector):
        index = 0
        for value in iterator:
            if value != self.expected[index]:
                print(self.name + " Test failed. Expected: " + str(self.expected[index]) + " Actual: " + str(value))
                raise Exception(self.name + " failed!")
            index += 1
        collector.collect(self.name + " successful!")


class Id(MapFunction):
    def map(self, value):
        return value


if __name__ == "__main__":
    env = get_environment()

    d1 = env.from_elements(bytearray(b"hello"), bytearray(b"world"))

    d1.map(Id(), BYTES).map_partition(Verify([bytearray(b"hello"), bytearray(b"world")], "Byte"), STRING).output()

    d2 = env.from_elements(1,2,3,4,5)

    d2.map(Id(), INT).map_partition(Verify([1,2,3,4,5], "Int"), STRING).output()

    d3 = env.from_elements(True, True, False)

    d3.map(Id(), BOOL).map_partition(Verify([True, True, False], "Bool"), STRING).output()

    d4 = env.from_elements(1.4, 1.7, 12312.23)

    d4.map(Id(), FLOAT).map_partition(Verify([1.4, 1.7, 12312.23], "Float"), STRING).output()

    d5 = env.from_elements("hello", "world")

    d5.map(Id(), STRING).map_partition(Verify(["hello", "world"], "String"), STRING).output()

    env.set_degree_of_parallelism(1)

    env.execute(local=True)