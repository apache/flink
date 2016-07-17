# ###############################################################################
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
from flink.functions.GroupReduceFunction import GroupReduceFunction

class AggregationFunction(GroupReduceFunction):
    def __init__(self, aggregation, field):
        super(AggregationFunction, self).__init__()
        self.aggregations = [aggregation(field)]

    def add_aggregation(self, aggregation, field):
        """
        Add an additional aggregation operator
        :param aggregation: Built-in aggregation operator to apply
        :param field: Field on which to apply the specified aggregation
        """
        self.aggregations.append(aggregation(field))

    def reduce(self, iterator, collector):
        # Reset each aggregator
        for aggregator in self.aggregations:
            aggregator.initialize_aggregation()

        # Tuple that will be filled in with aggregated values
        item = None

        # Run each value through the aggregator
        for x in iterator:
            if item is None:
                # Get first value that will be filled in
                item = list(x)

            for aggregator in self.aggregations:
                aggregator.aggregate(x[aggregator.field])

        # Get results
        for aggregator in self.aggregations:
            item[aggregator.field] = aggregator.get_aggregate()

        collector.collect(tuple(item))


class AggregationOperator(object):
    def __init__(self, field):
        self.field = field

    def initialize_aggregation(self):
        """Set up or reset the aggregator operator."""
        self.agg = None

    def aggregate(self, value):
        """Incorporate a value into the aggregation."""
        pass

    def get_aggregate(self):
        """Return the result of the aggregation."""
        return self.agg


class Sum(AggregationOperator):
    def initialize_aggregation(self):
        self.agg = 0

    def aggregate(self, value):
        self.agg += value


class Min(AggregationOperator):
    def aggregate(self, value):
        if self.agg != None:
            if value < self.agg:
                self.agg = value
        else:
            self.agg = value


class Max(AggregationOperator):
    def aggregate(self, value):
        if self.agg != None:
            if value > self.agg:
                self.agg = value
        else:
            self.agg = value



