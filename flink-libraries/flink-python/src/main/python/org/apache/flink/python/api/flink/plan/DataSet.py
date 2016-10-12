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
import collections
import types as TYPES

from flink.functions.Aggregation import AggregationFunction, Min, Max, Sum
from flink.plan.Constants import _Identifier, WriteMode, _createKeyValueTypeInfo, _createArrayTypeInfo
from flink.plan.OperationInfo import OperationInfo
from flink.functions.CoGroupFunction import CoGroupFunction
from flink.functions.FilterFunction import FilterFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.CrossFunction import CrossFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.JoinFunction import JoinFunction
from flink.functions.MapFunction import MapFunction
from flink.functions.MapPartitionFunction import MapPartitionFunction
from flink.functions.ReduceFunction import ReduceFunction
from flink.functions.KeySelectorFunction import KeySelectorFunction


class Stringify(MapFunction):
    def map(self, value):
        if isinstance(value, (tuple, list)):
            return "(" + ", ".join([self.map(x) for x in value]) + ")"
        else:
            return str(value)


class CsvStringify(MapFunction):
    def __init__(self, f_delim):
        super(CsvStringify, self).__init__()
        self.delim = f_delim

    def map(self, value):
        return self.delim.join([self._map(field) for field in value])

    def _map(self, value):
        if isinstance(value, (tuple, list)):
            return "(" + ", ".join([self._map(x) for x in value]) + ")"
        else:
            return str(value)


class DataSink(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info
        info.id = env._counter
        env._counter += 1

    def name(self, name):
        self._info.name = name
        return self

    def set_parallelism(self, parallelism):
        self._info.parallelism.value = parallelism
        return self


class DataSet(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info
        info.id = env._counter
        env._counter += 1

    def output(self, to_error=False):
        """
        Writes a DataSet to the standard output stream (stdout).
        """
        return self.map(Stringify())._output(to_error)

    def _output(self, to_error):
        child = OperationInfo()
        child_set = DataSink(self._env, child)
        child.identifier = _Identifier.SINK_PRINT
        child.parent = self._info
        child.to_err = to_error
        self._info.parallelism = child.parallelism
        self._info.sinks.append(child)
        self._env._sinks.append(child)
        return child_set

    def write_text(self, path, write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a DataSet as a text file to the specified location.

        :param path: he path pointing to the location the text file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        return self.map(Stringify())._write_text(path, write_mode)

    def _write_text(self, path, write_mode):
        child = OperationInfo()
        child_set = DataSink(self._env, child)
        child.identifier = _Identifier.SINK_TEXT
        child.parent = self._info
        child.path = path
        child.write_mode = write_mode
        self._info.parallelism = child.parallelism
        self._info.sinks.append(child)
        self._env._sinks.append(child)
        return child_set

    def write_csv(self, path, line_delimiter="\n", field_delimiter=',', write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a Tuple DataSet as a CSV file to the specified location.

        Note: Only a Tuple DataSet can written as a CSV file.
        :param path: The path pointing to the location the CSV file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        return self.map(CsvStringify(field_delimiter))._write_csv(path, line_delimiter, field_delimiter, write_mode)

    def _write_csv(self, path, line_delimiter, field_delimiter, write_mode):
        child = OperationInfo()
        child_set = DataSink(self._env, child)
        child.identifier = _Identifier.SINK_CSV
        child.path = path
        child.parent = self._info
        child.delimiter_field = field_delimiter
        child.delimiter_line = line_delimiter
        child.write_mode = write_mode
        self._info.parallelism = child.parallelism
        self._info.sinks.append(child)
        self._env._sinks.append(child)
        return child_set

    def reduce_group(self, operator, combinable=False):
        """
        Applies a GroupReduce transformation.

        The transformation calls a GroupReduceFunction once for each group of the DataSet, or one when applied on a
        non-grouped DataSet.
        The GroupReduceFunction can iterate over all elements of the DataSet and
        emit any number of output elements including none.

        :param operator: The GroupReduceFunction that is applied on the DataSet.
        :return:A GroupReduceOperator that represents the reduced DataSet.
        """
        child = self._reduce_group(operator, combinable)
        child_set = OperatorSet(self._env, child)
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def _reduce_group(self, operator, combinable=False):
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = GroupReduceFunction()
            operator.reduce = f
        child = OperationInfo()
        child.identifier = _Identifier.GROUPREDUCE
        child.parent = self._info
        child.operator = operator
        child.types = _createArrayTypeInfo()
        child.name = "PythonGroupReduce"
        return child

    def reduce(self, operator):
        """
        Applies a Reduce transformation on a non-grouped DataSet.

        The transformation consecutively calls a ReduceFunction until only a single element remains which is the result
        of the transformation. A ReduceFunction combines two elements into one new element of the same type.

        :param operator:The ReduceFunction that is applied on the DataSet.
        :return:A ReduceOperator that represents the reduced DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = ReduceFunction()
            operator.reduce = f
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.REDUCE
        child.parent = self._info
        child.operator = operator
        child.name = "PythonReduce"
        child.types = _createArrayTypeInfo()
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def aggregate(self, aggregation, field):
        """
        Applies an Aggregate transformation (using a GroupReduceFunction) on a non-grouped Tuple DataSet.
        :param aggregation: The built-in aggregation function to apply on the DataSet.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated DataSet.
        """
        child = self._reduce_group(AggregationFunction(aggregation, field), combinable=True)
        child.name = "PythonAggregate" + aggregation.__name__  # include aggregation type in name
        child_set = AggregateOperator(self._env, child)
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def min(self, field):
        """
        Syntactic sugar for the minimum aggregation.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated DataSet.
        """
        return self.aggregate(Min, field)

    def max(self, field):
        """
        Syntactic sugar for the maximum aggregation.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated DataSet.
        """
        return self.aggregate(Max, field)

    def sum(self, field):
        """
        Syntactic sugar for the sum aggregation.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated DataSet.
        """
        return self.aggregate(Sum, field)

    def project(self, *fields):
        """
        Applies a Project transformation on a Tuple DataSet.

        Note: Only Tuple DataSets can be projected. The transformation projects each Tuple of the DataSet onto a
        (sub)set of fields.

        :param fields: The field indexes of the input tuples that are retained.
                        The order of fields in the output tuple corresponds to the order of field indexes.
        :return: The projected DataSet.

        """
        return self.map(lambda x: tuple([x[key] for key in fields]))

    def group_by(self, *keys):
        """
        Groups a Tuple DataSet using field position keys.
        Note: Field position keys only be specified for Tuple DataSets.
        The field position keys specify the fields of Tuples on which the DataSet is grouped.
        This method returns an UnsortedGrouping on which one of the following grouping transformation can be applied.
        sort_group() to get a SortedGrouping.
        reduce() to apply a Reduce transformation.
        group_reduce() to apply a GroupReduce transformation.

        :param keys: One or more field positions on which the DataSet will be grouped.
        :return:A Grouping on which a transformation needs to be applied to obtain a transformed DataSet.
        """
        return self.map(lambda x: x)._group_by(keys)

    def _group_by(self, keys):
        child = OperationInfo()
        child_chain = []
        child_set = UnsortedGrouping(self._env, child, child_chain)
        child.identifier = _Identifier.GROUP
        child.parent = self._info
        child.keys = keys
        child_chain.append(child)
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def co_group(self, other_set):
        """
        Initiates a CoGroup transformation which combines the elements of two DataSets into on DataSet.

        It groups each DataSet individually on a key and gives groups of both DataSets with equal keys together into a
        CoGroupFunction. If a DataSet has a group with no matching key in the other DataSet,
        the CoGroupFunction is called with an empty group for the non-existing group.
        The CoGroupFunction can iterate over the elements of both groups and return any number of elements
        including none.

        :param other_set: The other DataSet of the CoGroup transformation.
        :return:A CoGroupOperator to continue the definition of the CoGroup transformation.
        """
        child = OperationInfo()
        other_set._info.children.append(child)
        child_set = CoGroupOperatorWhere(self._env, child)
        child.identifier = _Identifier.COGROUP
        child.parent_set = self
        child.other_set = other_set
        return child_set

    def cross(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        return self._cross(other_set, _Identifier.CROSS)

    def cross_with_huge(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.
        This method also gives the hint to the optimizer that
        the second DataSet to cross is much larger than the first one.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        return self._cross(other_set, _Identifier.CROSSH)

    def cross_with_tiny(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.
        This method also gives the hint to the optimizer that
        the second DataSet to cross is much smaller than the first one.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        return self._cross(other_set, _Identifier.CROSST)

    def _cross(self, other_set, identifier):
        child = OperationInfo()
        child_set = CrossOperator(self._env, child)
        child.identifier = identifier
        child.parent = self._info
        child.other = other_set._info
        self._info.children.append(child)
        other_set._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def distinct(self, *fields):
        """
        Returns a distinct set of a tuple DataSet using field position keys.

        :param fields: One or more field positions on which the distinction of the DataSet is decided.
        :return: The distinct DataSet.
        """
        f = None
        if len(fields) == 0:
            f = lambda x: (x,)
            fields = (0,)
        if isinstance(fields[0], TYPES.FunctionType):
            f = lambda x: (fields[0](x),)
        if isinstance(fields[0], KeySelectorFunction):
            f = lambda x: (fields[0].get_key(x),)
        if f is None:
            f = lambda x: tuple([x[key] for key in fields])
        return self.map(lambda x: (f(x), x)).name("DistinctPreStep")._distinct(tuple([x for x in range(len(fields))]))

    def _distinct(self, fields):
        self._info.types = _createKeyValueTypeInfo(len(fields))
        child = OperationInfo()
        child_set = DataSet(self._env, child)
        child.identifier = _Identifier.DISTINCT
        child.parent = self._info
        child.keys = fields
        self._info.parallelism = child.parallelism
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def filter(self, operator):
        """
        Applies a Filter transformation on a DataSet.

        he transformation calls a FilterFunction for each element of the DataSet and retains only those element
        for which the function returns true. Elements for which the function returns false are filtered.

        :param operator: The FilterFunction that is called for each element of the DataSet.
        :return:A FilterOperator that represents the filtered DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = FilterFunction()
            operator.filter = f
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.FILTER
        child.parent = self._info
        child.operator = operator
        child.name = "PythonFilter"
        child.types = _createArrayTypeInfo()
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def first(self, count):
        """
        Returns a new set containing the first n elements in this DataSet.

        :param count: The desired number of elements.
        :return: A DataSet containing the elements.
        """
        child = OperationInfo()
        child_set = DataSet(self._env, child)
        child.identifier = _Identifier.FIRST
        child.parent = self._info
        child.count = count
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def flat_map(self, operator):
        """
        Applies a FlatMap transformation on a DataSet.

        The transformation calls a FlatMapFunction for each element of the DataSet.
        Each FlatMapFunction call can return any number of elements including none.

        :param operator: The FlatMapFunction that is called for each element of the DataSet.
        :return:A FlatMapOperator that represents the transformed DataSe
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = FlatMapFunction()
            operator.flat_map = f
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.FLATMAP
        child.parent = self._info
        child.operator = operator
        child.types = _createArrayTypeInfo()
        child.name = "PythonFlatMap"
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def join(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        return self._join(other_set, _Identifier.JOIN)

    def join_with_huge(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.
        This method also gives the hint to the optimizer that
        the second DataSet to join is much larger than the first one.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        return self._join(other_set, _Identifier.JOINH)

    def join_with_tiny(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.
        This method also gives the hint to the optimizer that
        the second DataSet to join is much smaller than the first one.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        return self._join(other_set, _Identifier.JOINT)

    def _join(self, other_set, identifier):
        child = OperationInfo()
        child_set = JoinOperatorWhere(self._env, child)
        child.identifier = identifier
        child.parent_set = self
        child.other_set = other_set
        return child_set

    def map(self, operator):
        """
        Applies a Map transformation on a DataSet.

        The transformation calls a MapFunction for each element of the DataSet.
        Each MapFunction call returns exactly one element.

        :param operator: The MapFunction that is called for each element of the DataSet.
        :return:A MapOperator that represents the transformed DataSet
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = MapFunction()
            operator.map = f
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.MAP
        child.parent = self._info
        child.operator = operator
        child.types = _createArrayTypeInfo()
        child.name = "PythonMap"
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def map_partition(self, operator):
        """
        Applies a MapPartition transformation on a DataSet.

        The transformation calls a MapPartitionFunction once per parallel partition of the DataSet.
        The entire partition is available through the given Iterator.
        Each MapPartitionFunction may return an arbitrary number of results.

        The number of elements that each instance of the MapPartition function
        sees is non deterministic and depends on the degree of parallelism of the operation.

        :param operator: The MapFunction that is called for each element of the DataSet.
        :return:A MapOperator that represents the transformed DataSet
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = MapPartitionFunction()
            operator.map_partition = f
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.MAPPARTITION
        child.parent = self._info
        child.operator = operator
        child.types = _createArrayTypeInfo()
        child.name = "PythonMapPartition"
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def partition_by_hash(self, *fields):
        f = None
        if len(fields) == 0:
            raise ValueError("fields argument must not be empty.")
        if isinstance(fields[0], TYPES.FunctionType):
            f = lambda x: (fields[0](x),)
        if isinstance(fields[0], KeySelectorFunction):
            f = lambda x: (fields[0].get_key(x),)
        if f is None:
            f = lambda x: tuple([x[key] for key in fields])
        return self.map(lambda x: (f(x), x)).name("HashPartitionPreStep")._partition_by_hash(tuple([x for x in range(len(fields))]))

    def _partition_by_hash(self, fields):
        """
        Hash-partitions a DataSet on the specified key fields.
        Important:This operation shuffles the whole DataSet over the network and can take significant amount of time.

        :param fields: The field indexes on which the DataSet is hash-partitioned.
        :return: The partitioned DataSet.
        """
        self._info.types = _createKeyValueTypeInfo(len(fields))
        child = OperationInfo()
        child_set = DataSet(self._env, child)
        child.identifier = _Identifier.PARTITION_HASH
        child.parent = self._info
        child.keys = fields
        self._info.parallelism = child.parallelism
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def rebalance(self):
        """
        Enforces a re-balancing of the DataSet, i.e., the DataSet is evenly distributed over all parallel instances of the
        following task. This can help to improve performance in case of heavy data skew and compute intensive operations.
        Important:This operation shuffles the whole DataSet over the network and can take significant amount of time.

        :return: The re-balanced DataSet.
        """
        child = OperationInfo()
        child_set = DataSet(self._env, child)
        child.identifier = _Identifier.REBALANCE
        child.parent = self._info
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def union(self, other_set):
        """
        Creates a union of this DataSet with an other DataSet.

        The other DataSet must be of the same data type.

        :param other_set: The other DataSet which is unioned with the current DataSet.
        :return:The resulting DataSet.
        """
        child = OperationInfo()
        child_set = DataSet(self._env, child)
        child.identifier = _Identifier.UNION
        child.parent = self._info
        child.other = other_set._info
        self._info.children.append(child)
        other_set._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def name(self, name):
        self._info.name = name
        return self

    def set_parallelism(self, parallelism):
        self._info.parallelism.value = parallelism
        return self

    def count_elements_per_partition(self):
        """
        Method that goes over all the elements in each partition in order to retrieve the total number of elements.
        :return: A DataSet containing Tuples of subtask index, number of elements mappings.
        """
        class CountElementsPerPartitionMapper(MapPartitionFunction):
            def map_partition(self, iterator, collector):
                counter = 0
                for x in iterator:
                    counter += 1

                collector.collect((self.context.get_index_of_this_subtask(), counter))
        return self.map_partition(CountElementsPerPartitionMapper())

    def zip_with_index(self):
        """
        Method that assigns a unique Long value to all elements of the DataSet. The generated values are consecutive.
        :return: A DataSet of Tuples consisting of consecutive ids and initial values.
        """
        element_count = self.count_elements_per_partition()
        class ZipWithIndexMapper(MapPartitionFunction):
            start = -1

            def _run(self):
                offsets = self.context.get_broadcast_variable("counts")
                offsets = sorted(offsets, key=lambda t: t[0]) # sort by task ID
                offsets = collections.deque(offsets)

                # compute the offset for each partition
                for i in range(self.context.get_index_of_this_subtask()):
                    self.start += offsets[i][1]

                super(ZipWithIndexMapper, self)._run()

            def map_partition(self, iterator, collector):
                for value in iterator:
                    self.start += 1
                    collector.collect((self.start, value))
        return self\
            .map_partition(ZipWithIndexMapper())\
            .with_broadcast_set("counts", element_count)


class OperatorSet(DataSet):
    def __init__(self, env, info):
        super(OperatorSet, self).__init__(env, info)

    def with_broadcast_set(self, name, set):
        child = OperationInfo()
        child.identifier = _Identifier.BROADCAST
        child.parent = self._info
        child.other = set._info
        child.name = name
        self._info.bcvars.append(child)
        self._env._broadcast.append(child)
        return self


class Grouping(object):
    def __init__(self, env, info, child_chain):
        self._env = env
        self._child_chain = child_chain
        self._info = info
        info.id = env._counter
        env._counter += 1

    def _finalize(self):
        pass

    def first(self, count):
        """
        Returns a new set containing the first n elements in this DataSet.

        :param count: The desired number of elements.
        :return: A DataSet containing the elements.
        """
        self._finalize()
        child = OperationInfo()
        child_set = DataSet(self._env, child)
        child.identifier = _Identifier.FIRST
        child.parent = self._info
        child.count = count
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def reduce_group(self, operator, combinable=False):
        """
        Applies a GroupReduce transformation.

        The transformation calls a GroupReduceFunction once for each group of the DataSet, or one when applied on a
        non-grouped DataSet.
        The GroupReduceFunction can iterate over all elements of the DataSet and
        emit any number of output elements including none.

        :param operator: The GroupReduceFunction that is applied on the DataSet.
        :return:A GroupReduceOperator that represents the reduced DataSet.
        """
        child = self._reduce_group(operator, combinable)
        child_set = OperatorSet(self._env, child)
        self._info.parallelism = child.parallelism
        self._info.children.append(child)
        self._env._sets.append(child)

        return child_set

    def _reduce_group(self, operator, combinable=False):
        self._finalize()
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = GroupReduceFunction()
            operator.reduce = f
        child = OperationInfo()
        child.identifier = _Identifier.GROUPREDUCE
        child.parent = self._info
        child.operator = operator
        child.types = _createArrayTypeInfo()
        child.name = "PythonGroupReduce"
        child.key1 = self._child_chain[0].keys
        return child

    def sort_group(self, field, order):
        """
        Sorts Tuple elements within a group on the specified field in the specified Order.

        Note: Only groups of Tuple elements can be sorted.
        Groups can be sorted by multiple fields by chaining sort_group() calls.

        :param field:The Tuple field on which the group is sorted.
        :param order: The Order in which the specified Tuple field is sorted. See DataSet.Order.
        :return:A SortedGrouping with specified order of group element.
        """
        child = OperationInfo()
        child_set = SortedGrouping(self._env, child, self._child_chain)
        child.identifier = _Identifier.SORT
        child.parent = self._info
        child.field = field
        child.order = order
        self._info.children.append(child)
        self._child_chain.append(child)
        self._env._sets.append(child)
        return child_set


class UnsortedGrouping(Grouping):
    def __init__(self, env, info, child_chain):
        super(UnsortedGrouping, self).__init__(env, info, child_chain)

    def reduce(self, operator):
        """
        Applies a Reduce transformation on a non-grouped DataSet.

        The transformation consecutively calls a ReduceFunction until only a single element remains which is the result
        of the transformation. A ReduceFunction combines two elements into one new element of the same type.

        :param operator:The ReduceFunction that is applied on the DataSet.
        :return:A ReduceOperator that represents the reduced DataSet.
        """
        self._finalize()
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = ReduceFunction()
            operator.reduce = f
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.REDUCE
        child.parent = self._info
        child.operator = operator
        child.name = "PythonReduce"
        child.types = _createArrayTypeInfo()
        child.key1 = self._child_chain[0].keys
        self._info.parallelism = child.parallelism
        self._info.children.append(child)
        self._env._sets.append(child)

        return child_set

    def aggregate(self, aggregation, field):
        """
        Applies an Aggregate transformation (using a GroupReduceFunction) on a Tuple UnsortedGrouping.
        :param aggregation: The built-in aggregation function to apply on the UnsortedGrouping.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated UnsortedGrouping.
        """
        child = self._reduce_group(AggregationFunction(aggregation, field), combinable=True)
        child.name = "PythonAggregate" + aggregation.__name__  # include aggregation type in name
        child_set = AggregateOperator(self._env, child)
        self._env._sets.append(child)
        self._info.children.append(child)
        return child_set

    def min(self, field):
        """
        Syntactic sugar for the minimum aggregation.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated UnsortedGrouping.
        """
        return self.aggregate(Min, field)

    def max(self, field):
        """
        Syntactic sugar for the maximum aggregation.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated UnsortedGrouping.
        """
        return self.aggregate(Max, field)

    def sum(self, field):
        """
        Syntactic sugar for the sum aggregation.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated UnsortedGrouping.
        """
        return self.aggregate(Sum, field)

    def _finalize(self):
        grouping = self._child_chain[0]
        keys = grouping.keys
        f = None
        if isinstance(keys[0], TYPES.FunctionType):
            f = lambda x: (keys[0](x),)
        if isinstance(keys[0], KeySelectorFunction):
            f = lambda x: (keys[0].get_key(x),)
        if f is None:
            f = lambda x: tuple([x[key] for key in keys])

        grouping.parent.operator.map = lambda x: (f(x), x)
        grouping.parent.types = _createKeyValueTypeInfo(len(keys))
        grouping.keys = tuple([i for i in range(len(grouping.keys))])


class SortedGrouping(Grouping):
    def __init__(self, env, info, child_chain):
        super(SortedGrouping, self).__init__(env, info, child_chain)

    def _finalize(self):
        grouping = self._child_chain[0]
        sortings = self._child_chain[1:]

        #list of used index keys to prevent duplicates and determine final index
        index_keys = set()

        if not isinstance(grouping.keys[0], (TYPES.FunctionType, KeySelectorFunction)):
            index_keys = index_keys.union(set(grouping.keys))

        #list of sorts using indices
        index_sorts = []
        #list of sorts using functions
        ksl_sorts = []
        for s in sortings:
            if not isinstance(s.field, (TYPES.FunctionType, KeySelectorFunction)):
                index_keys.add(s.field)
                index_sorts.append(s)
            else:
                ksl_sorts.append(s)

        used_keys = sorted(index_keys)
        #all data gathered

        #construct list of extractor lambdas
        lambdas = []
        i = 0
        for key in used_keys:
            lambdas.append(lambda x, k=key: x[k])
            i += 1
        if isinstance(grouping.keys[0], (TYPES.FunctionType, KeySelectorFunction)):
            lambdas.append(grouping.keys[0])
        for ksl_op in ksl_sorts:
            lambdas.append(ksl_op.field)

        grouping.parent.operator.map = lambda x: (tuple([l(x) for l in lambdas]), x)
        grouping.parent.types = _createKeyValueTypeInfo(len(lambdas))
        #modify keys
        ksl_offset = len(used_keys)
        if not isinstance(grouping.keys[0], (TYPES.FunctionType, KeySelectorFunction)):
            grouping.keys = tuple([used_keys.index(key) for key in grouping.keys])
        else:
            grouping.keys = (ksl_offset,)
            ksl_offset += 1

        for iop in index_sorts:
            iop.field = used_keys.index(iop.field)

        for kop in ksl_sorts:
            kop.field = ksl_offset
            ksl_offset += 1


class CoGroupOperatorWhere(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info

    def where(self, *fields):
        """
        Continues a CoGroup transformation.

        Defines the Tuple fields of the first co-grouped DataSet that should be used as grouping keys.
        Note: Fields can only be selected as grouping keys on Tuple DataSets.

        :param fields: The indexes of the Tuple fields of the first co-grouped DataSets that should be used as keys.
        :return: An incomplete CoGroup transformation.
        """
        f = None
        if isinstance(fields[0], TYPES.FunctionType):
            f = lambda x: (fields[0](x),)
        if isinstance(fields[0], KeySelectorFunction):
            f = lambda x: (fields[0].get_key(x),)
        if f is None:
            f = lambda x: tuple([x[key] for key in fields])

        new_parent_set = self._info.parent_set.map(lambda x: (f(x), x))
        new_parent_set._info.types = _createKeyValueTypeInfo(len(fields))
        self._info.parent = new_parent_set._info
        self._info.parent.children.append(self._info)
        self._info.key1 = fields
        return CoGroupOperatorTo(self._env, self._info)


class CoGroupOperatorTo(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info

    def equal_to(self, *fields):
        """
        Continues a CoGroup transformation.

        Defines the Tuple fields of the second co-grouped DataSet that should be used as grouping keys.
        Note: Fields can only be selected as grouping keys on Tuple DataSets.

        :param fields: The indexes of the Tuple fields of the second co-grouped DataSet that should be used as keys.
        :return: An incomplete CoGroup transformation.
        """
        f = None
        if isinstance(fields[0], TYPES.FunctionType):
            f = lambda x: (fields[0](x),)
        if isinstance(fields[0], KeySelectorFunction):
            f = lambda x: (fields[0].get_key(x),)
        if f is None:
            f = lambda x: tuple([x[key] for key in fields])

        new_other_set = self._info.other_set.map(lambda x: (f(x), x))
        new_other_set._info.types = _createKeyValueTypeInfo(len(fields))
        self._info.other = new_other_set._info
        self._info.other.children.append(self._info)
        self._info.key2 = fields
        return CoGroupOperatorUsing(self._env, self._info)


class CoGroupOperatorUsing(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info

    def using(self, operator):
        """
        Finalizes a CoGroup transformation.

        Applies a CoGroupFunction to groups of elements with identical keys.
        Each CoGroupFunction call returns an arbitrary number of keys.

        :param operator: The CoGroupFunction that is called for all groups of elements with identical keys.
        :return:An CoGroupOperator that represents the co-grouped result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = CoGroupFunction()
            operator.co_group = f
        new_set = OperatorSet(self._env, self._info)
        self._info.key1 = tuple([x for x in range(len(self._info.key1))])
        self._info.key2 = tuple([x for x in range(len(self._info.key2))])
        operator._keys1 = self._info.key1
        operator._keys2 = self._info.key2
        self._info.parent.parallelism = self._info.parallelism
        self._info.other.parallelism = self._info.parallelism
        self._info.operator = operator
        self._info.types = _createArrayTypeInfo()
        self._info.name = "PythonCoGroup"
        self._env._sets.append(self._info)
        return new_set


class JoinOperatorWhere(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info

    def where(self, *fields):
        """
        Continues a Join transformation.

        Defines the Tuple fields of the first join DataSet that should be used as join keys.
        Note: Fields can only be selected as join keys on Tuple DataSets.

        :param fields: The indexes of the Tuple fields of the first join DataSets that should be used as keys.
        :return:An incomplete Join transformation.

        """
        f = None
        if isinstance(fields[0], TYPES.FunctionType):
            f = lambda x: (fields[0](x),)
        if isinstance(fields[0], KeySelectorFunction):
            f = lambda x: (fields[0].get_key(x),)
        if f is None:
            f = lambda x: tuple([x[key] for key in fields])

        new_parent_set = self._info.parent_set.map(lambda x: (f(x), x))
        new_parent_set._info.types = _createKeyValueTypeInfo(len(fields))
        self._info.parent = new_parent_set._info
        self._info.parent.parallelism = self._info.parallelism
        self._info.parent.children.append(self._info)
        self._info.key1 = tuple([x for x in range(len(fields))])
        return JoinOperatorTo(self._env, self._info)


class JoinOperatorTo(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info

    def equal_to(self, *fields):
        """
        Continues a Join transformation.

        Defines the Tuple fields of the second join DataSet that should be used as join keys.
        Note: Fields can only be selected as join keys on Tuple DataSets.

        :param fields:The indexes of the Tuple fields of the second join DataSet that should be used as keys.
        :return:An incomplete Join Transformation.
        """
        f = None
        if isinstance(fields[0], TYPES.FunctionType):
            f = lambda x: (fields[0](x),)
        if isinstance(fields[0], KeySelectorFunction):
            f = lambda x: (fields[0].get_key(x),)
        if f is None:
            f = lambda x: tuple([x[key] for key in fields])

        new_other_set = self._info.other_set.map(lambda x: (f(x), x))
        new_other_set._info.types = _createKeyValueTypeInfo(len(fields))
        self._info.other = new_other_set._info
        self._info.other.parallelism = self._info.parallelism
        self._info.other.children.append(self._info)
        self._info.key2 = tuple([x for x in range(len(fields))])
        self._env._sets.append(self._info)
        return JoinOperator(self._env, self._info)


class Projector(DataSet):
    def __init__(self, env, info):
        super(Projector, self).__init__(env, info)

    def project_first(self, *fields):
        """
        Initiates a Project transformation.

        Projects the first input.
        If the first input is a Tuple DataSet, fields can be selected by their index.
        If the first input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete Projection.
        """
        for field in fields:
            self._info.projections.append((0, field))
        self._info.operator.map = lambda x : tuple([x[side][index] for side, index in self._info.projections])
        return self

    def project_second(self, *fields):
        """
        Initiates a Project transformation.

        Projects the second input.
        If the second input is a Tuple DataSet, fields can be selected by their index.
        If the second input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete Projection.
        """
        for field in fields:
            self._info.projections.append((1, field))
        self._info.operator.map = lambda x : tuple([x[side][index] for side, index in self._info.projections])
        return self


class Projectable:
    def __init__(self):
        pass

    def project_first(self, *fields):
        """
        Initiates a Project  transformation.

        Projects the first  input.
        If the first input is a Tuple DataSet, fields can be selected by their index.
        If the first input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete Projection.
        """
        return Projectable._createProjector(self._env, self._info).project_first(*fields)

    def project_second(self, *fields):
        """
        Initiates a Project transformation.

        Projects the second input.
        If the second input is a Tuple DataSet, fields can be selected by their index.
        If the second input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete Projection.
        """
        return Projectable._createProjector(self._env, self._info).project_second(*fields)

    @staticmethod
    def _createProjector(env, info):
        child = OperationInfo()
        child_set = Projector(env, child)
        child.identifier = _Identifier.MAP
        child.operator = MapFunction()
        child.parent = info
        child.types = _createArrayTypeInfo()
        child.name = "Projector"
        child.parallelism = info.parallelism
        info.children.append(child)
        env._sets.append(child)
        return child_set


class JoinOperator(DataSet, Projectable):
    def __init__(self, env, info):
        super(JoinOperator, self).__init__(env, info)

    def using(self, operator):
        """
        Finalizes a Join transformation.

        Applies a JoinFunction to each pair of joined elements. Each JoinFunction call returns exactly one element.

        :param operator:The JoinFunction that is called for each pair of joined elements.
        :return:An Set that represents the joined result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = JoinFunction()
            operator.join = f
        self._info.operator = operator
        self._info.types = _createArrayTypeInfo()
        self._info.name = "PythonJoin"
        self._info.uses_udf = True
        return OperatorSet(self._env, self._info)


class CrossOperator(DataSet, Projectable):
    def __init__(self, env, info):
        super(CrossOperator, self).__init__(env, info)

    def using(self, operator):
        """
        Finalizes a Cross transformation.

        Applies a CrossFunction to each pair of joined elements. Each CrossFunction call returns exactly one element.

        :param operator:The CrossFunction that is called for each pair of joined elements.
        :return:An Set that represents the joined result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = CrossFunction()
            operator.cross = f
        self._info.operator = operator
        self._info.types = _createArrayTypeInfo()
        self._info.name = "PythonCross"
        self._info.uses_udf = True
        return OperatorSet(self._env, self._info)


class AggregateOperator(OperatorSet):
    def __init__(self, env, info):
        super(AggregateOperator, self).__init__(env, info)

    def and_agg(self, aggregation, field):
        """
        Applies an additional Aggregate transformation.
        :param aggregation: The built-in aggregation operation to apply on the DataSet.
        :param field: The index of the Tuple field on which to perform the function.
        :return: An AggregateOperator that represents the aggregated DataSet.
        """
        self._info.operator.add_aggregation(aggregation, field)
        return self
