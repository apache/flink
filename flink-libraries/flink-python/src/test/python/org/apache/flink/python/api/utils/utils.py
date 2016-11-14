# ###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################
from flink.functions.MapFunction import MapFunction
from flink.functions.MapPartitionFunction import MapPartitionFunction


class Id(MapFunction):
    def map(self, value):
        """
        Simple map function to forward test results.

        :param value: Input value.
        :return: Forwarded value.
        """
        return value


class Verify(MapPartitionFunction):
    def __init__(self, expected, name):
        super(Verify, self).__init__()
        self.expected = expected
        self.name = name

    def map_partition(self, iterator, collector):
        """
        Compares elements in the expected values list against actual values in resulting DataSet.

        :param iterator: Iterator for the corresponding DataSet partition.
        :param collector: Collector for the result records.
        """
        index = 0
        for value in iterator:
            try:
                if value != self.expected[index]:
                    raise Exception(self.name + " Test failed. Expected: " + str(self.expected[index]) + " Actual: " + str(value))
            except IndexError:
                raise Exception(self.name + " Test failed. Discrepancy in the number of elements between expected and actual values.")
            index += 1
        if len(self.expected) != index:
            raise Exception(self.name + " Test failed. Discrepancy in the number of elements between expected and actual values.")
        #collector.collect(self.name + " successful!")


class Verify2(MapPartitionFunction):
    def __init__(self, expected, name):
        super(Verify2, self).__init__()
        self.expected = expected
        self.name = name

    def map_partition(self, iterator, collector):
        """
        Compares elements in the expected values list against actual values in resulting DataSet.

        This function does not compare element by element, since for example in a Union order is not guaranteed.

        Elements are removed from the expected values list for the whole DataSet.

        :param iterator: Iterator for the corresponding DataSet partition.
        :param collector: Collector for the result records.
        """
        for value in iterator:
            try:
                self.expected.remove(value)
            except Exception:
                raise Exception(self.name + " failed! Actual value " + str(value) + "not contained in expected values: " + str(self.expected))
        #collector.collect(self.name + " successful!")
