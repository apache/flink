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
import inspect
import copy
import types as TYPES

from flink.plan.Constants import _Identifier, WriteMode, STRING
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

def deduct_output_type(dataset):
    skip = set([_Identifier.GROUP, _Identifier.SORT, _Identifier.UNION])
    source = set([_Identifier.SOURCE_CSV, _Identifier.SOURCE_TEXT, _Identifier.SOURCE_VALUE])
    default = set([_Identifier.CROSS, _Identifier.CROSSH, _Identifier.CROSST, _Identifier.JOINT, _Identifier.JOINH, _Identifier.JOIN])

    while True:
        dataset_type = dataset.identifier
        if dataset_type in skip:
            dataset = dataset.parent
            continue
        if dataset_type in source:
            if dataset_type == _Identifier.SOURCE_TEXT:
                return STRING
            if dataset_type == _Identifier.SOURCE_VALUE:
                return dataset.values[0]
            if dataset_type == _Identifier.SOURCE_CSV:
                return dataset.types
        if dataset_type == _Identifier.PROJECTION:
            return tuple([deduct_output_type(dataset.parent)[k] for k in dataset.keys])
        if dataset_type in default:
            if dataset.operator is not None: #udf-join/cross
                return dataset.types
            if len(dataset.projections) == 0: #defaultjoin/-cross
                return (deduct_output_type(dataset.parent), deduct_output_type(dataset.other))
            else: #projectjoin/-cross
                t1 = deduct_output_type(dataset.parent)
                t2 = deduct_output_type(dataset.other)
                out_type = []
                for prj in dataset.projections:
                    if len(prj[1]) == 0: #projection on non-tuple dataset
                        if prj[0] == "first":
                            out_type.append(t1)
                        else:
                            out_type.append(t2)
                    else: #projection on tuple dataset
                        for key in prj[1]:
                            if prj[0] == "first":
                                out_type.append(t1[key])
                            else:
                                out_type.append(t2[key])
                return tuple(out_type)
        return dataset.types


class Set(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info
        info.id = env._counter
        env._counter += 1

    def output(self, to_error=False):
        """
        Writes a DataSet to the standard output stream (stdout).
        """
        child = OperationInfo()
        child.identifier = _Identifier.SINK_PRINT
        child.parent = self._info
        child.to_err = to_error
        self._info.sinks.append(child)
        self._env._sinks.append(child)

    def write_text(self, path, write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a DataSet as a text file to the specified location.

        :param path: he path pointing to the location the text file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        child = OperationInfo()
        child.identifier = _Identifier.SINK_TEXT
        child.parent = self._info
        child.path = path
        child.write_mode = write_mode
        self._info.sinks.append(child)
        self._env._sinks.append(child)

    def write_csv(self, path, line_delimiter="\n", field_delimiter=',', write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a Tuple DataSet as a CSV file to the specified location.

        Note: Only a Tuple DataSet can written as a CSV file.
        :param path: The path pointing to the location the CSV file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        child = OperationInfo()
        child.identifier = _Identifier.SINK_CSV
        child.path = path
        child.parent = self._info
        child.delimiter_field = field_delimiter
        child.delimiter_line = line_delimiter
        child.write_mode = write_mode
        self._info.sinks.append(child)
        self._env._sinks.append(child)

    def reduce_group(self, operator, types, combinable=False):
        """
        Applies a GroupReduce transformation.

        The transformation calls a GroupReduceFunction once for each group of the DataSet, or one when applied on a
        non-grouped DataSet.
        The GroupReduceFunction can iterate over all elements of the DataSet and
        emit any number of output elements including none.

        :param operator: The GroupReduceFunction that is applied on the DataSet.
        :param types: The type of the resulting DataSet.
        :return:A GroupReduceOperator that represents the reduced DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = GroupReduceFunction()
            operator.reduce = f
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.GROUPREDUCE
        child.parent = self._info
        child.operator = copy.deepcopy(operator)
        child.operator._combine = False
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.types = types
        child.combine = combinable
        child.combineop = operator
        child.combineop._combine = True
        child.name = "PythonGroupReduce"
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set


class ReduceSet(Set):
    def __init__(self, env, info):
        super(ReduceSet, self).__init__(env, info)

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
        child.combineop = operator
        child.combine = False
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.name = "PythonReduce"
        child.types = deduct_output_type(self._info)
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set


class DataSet(ReduceSet):
    def __init__(self, env, info):
        super(DataSet, self).__init__(env, info)

    def project(self, *fields):
        """
        Applies a Project transformation on a Tuple DataSet.

        Note: Only Tuple DataSets can be projected. The transformation projects each Tuple of the DataSet onto a
        (sub)set of fields.

        :param fields: The field indexes of the input tuples that are retained.
                        The order of fields in the output tuple corresponds to the order of field indexes.
        :return: The projected DataSet.

        """
        child = OperationInfo()
        child_set = DataSet(self._env, child)
        child.identifier = _Identifier.PROJECTION
        child.parent = self._info
        child.keys = fields
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

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
        child.parent = self._info
        child.other = other_set._info
        self._info.children.append(child)
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
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.name = "PythonFilter"
        child.types = deduct_output_type(self._info)
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def flat_map(self, operator, types):
        """
        Applies a FlatMap transformation on a DataSet.

        The transformation calls a FlatMapFunction for each element of the DataSet.
        Each FlatMapFunction call can return any number of elements including none.

        :param operator: The FlatMapFunction that is called for each element of the DataSet.
        :param types: The type of the resulting DataSet.
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
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.types = types
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
        child.parent = self._info
        child.other = other_set._info
        self._info.children.append(child)
        other_set._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def map(self, operator, types):
        """
        Applies a Map transformation on a DataSet.

        The transformation calls a MapFunction for each element of the DataSet.
        Each MapFunction call returns exactly one element.

        :param operator: The MapFunction that is called for each element of the DataSet.
        :param types: The type of the resulting DataSet
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
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.types = types
        child.name = "PythonMap"
        self._info.children.append(child)
        self._env._sets.append(child)
        return child_set

    def map_partition(self, operator, types):
        """
        Applies a MapPartition transformation on a DataSet.

        The transformation calls a MapPartitionFunction once per parallel partition of the DataSet.
        The entire partition is available through the given Iterator.
        Each MapPartitionFunction may return an arbitrary number of results.

        The number of elements that each instance of the MapPartition function
        sees is non deterministic and depends on the degree of parallelism of the operation.

        :param operator: The MapFunction that is called for each element of the DataSet.
        :param types: The type of the resulting DataSet
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
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.types = types
        child.name = "PythonMapPartition"
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


class OperatorSet(DataSet):
    def __init__(self, env, info):
        super(OperatorSet, self).__init__(env, info)

    def with_broadcast_set(self, name, set):
        child = OperationInfo()
        child.parent = self._info
        child.other = set._info
        child.name = name
        self._info.bcvars.append(child)
        set._info.children.append(child)
        self._env._broadcast.append(child)
        return self


class Grouping(object):
    def __init__(self, env, info, child_chain):
        self._env = env
        self._child_chain = child_chain
        self._info = info
        info.id = env._counter
        env._counter += 1

    def reduce_group(self, operator, types, combinable=False):
        """
        Applies a GroupReduce transformation.

        The transformation calls a GroupReduceFunction once for each group of the DataSet, or one when applied on a
        non-grouped DataSet.
        The GroupReduceFunction can iterate over all elements of the DataSet and
        emit any number of output elements including none.

        :param operator: The GroupReduceFunction that is applied on the DataSet.
        :param types: The type of the resulting DataSet.
        :return:A GroupReduceOperator that represents the reduced DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = GroupReduceFunction()
            operator.reduce = f
        operator._set_grouping_keys(self._child_chain[0].keys)
        operator._set_sort_ops([(x.field, x.order) for x in self._child_chain[1:]])
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.GROUPREDUCE
        child.parent = self._info
        child.operator = copy.deepcopy(operator)
        child.operator._combine = False
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.types = types
        child.combine = combinable
        child.combineop = operator
        child.combineop._combine = True
        child.name = "PythonGroupReduce"
        self._info.children.append(child)
        self._env._sets.append(child)

        return child_set

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
        operator._set_grouping_keys(self._child_chain[0].keys)
        for i in self._child_chain:
            self._env._sets.append(i)
        child = OperationInfo()
        child_set = OperatorSet(self._env, child)
        child.identifier = _Identifier.REDUCE
        child.parent = self._info
        child.operator = copy.deepcopy(operator)
        child.operator._combine = False
        child.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        child.combine = True
        child.combineop = operator
        child.combineop._combine = True
        child.name = "PythonReduce"
        child.types = deduct_output_type(self._info)
        self._info.children.append(child)
        self._env._sets.append(child)

        return child_set


class SortedGrouping(Grouping):
    def __init__(self, env, info, child_chain):
        super(SortedGrouping, self).__init__(env, info, child_chain)


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
        self._info.key2 = fields
        return CoGroupOperatorUsing(self._env, self._info)


class CoGroupOperatorUsing(object):
    def __init__(self, env, info):
        self._env = env
        self._info = info

    def using(self, operator, types):
        """
        Finalizes a CoGroup transformation.

        Applies a CoGroupFunction to groups of elements with identical keys.
        Each CoGroupFunction call returns an arbitrary number of keys.

        :param operator: The CoGroupFunction that is called for all groups of elements with identical keys.
        :param types: The type of the resulting DataSet.
        :return:An CoGroupOperator that represents the co-grouped result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = CoGroupFunction()
            operator.co_group = f
        new_set = OperatorSet(self._env, self._info)
        operator._keys1 = self._info.key1
        operator._keys2 = self._info.key2
        self._info.operator = operator
        self._info.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._info.types = types
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
        self._info.key1 = fields
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
        self._info.key2 = fields
        return JoinOperator(self._env, self._info)


class JoinOperatorProjection(DataSet):
    def __init__(self, env, info):
        super(JoinOperatorProjection, self).__init__(env, info)

    def project_first(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._info.projections.append(("first", fields))
        return self

    def project_second(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._info.projections.append(("second", fields))
        return self


class JoinOperator(DataSet):
    def __init__(self, env, info):
        super(JoinOperator, self).__init__(env, info)

    def project_first(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        return JoinOperatorProjection(self._env, self._info).project_first(*fields)

    def project_second(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        return JoinOperatorProjection(self._env, self._info).project_second(*fields)

    def using(self, operator, types):
        """
        Finalizes a Join transformation.

        Applies a JoinFunction to each pair of joined elements. Each JoinFunction call returns exactly one element.

        :param operator:The JoinFunction that is called for each pair of joined elements.
        :param types:
        :return:An Set that represents the joined result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = JoinFunction()
            operator.join = f
        self._info.operator = operator
        self._info.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._info.types = types
        self._info.name = "PythonJoin"
        self._env._sets.append(self._info)
        return OperatorSet(self._env, self._info)


class CrossOperatorProjection(DataSet):
    def __init__(self, env, info):
        super(CrossOperatorProjection, self).__init__(env, info)

    def project_first(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._info.projections.append(("first", fields))
        return self

    def project_second(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._info.projections.append(("second", fields))
        return self


class CrossOperator(DataSet):
    def __init__(self, env, info):
        super(CrossOperator, self).__init__(env, info)

    def project_first(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        return CrossOperatorProjection(self._env, self._info).project_first(*fields)

    def project_second(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        return CrossOperatorProjection(self._env, self._info).project_second(*fields)

    def using(self, operator, types):
        """
        Finalizes a Cross transformation.

        Applies a CrossFunction to each pair of joined elements. Each CrossFunction call returns exactly one element.

        :param operator:The CrossFunction that is called for each pair of joined elements.
        :param types: The type of the resulting DataSet.
        :return:An Set that represents the joined result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = CrossFunction()
            operator.cross = f
        self._info.operator = operator
        self._info.meta = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._info.types = types
        self._info.name = "PythonCross"
        return OperatorSet(self._env, self._info)
