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
from flink.connection import Connection
from flink.connection import Collector
from flink.plan.DataSet import DataSet
from flink.plan.Constants import _Identifier
from flink.plan.OperationInfo import OperationInfo
from flink.utilities import Switch
import copy
import sys
from struct import pack

def get_environment():
    """
    Creates an execution environment that represents the context in which the program is currently executed.
    
    :return:The execution environment of the context in which the program is executed.
    """
    return Environment()


class Environment(object):
    def __init__(self):
        # util
        self._counter = 0

        #parameters
        self._parameters = []

        #sets
        self._sources = []
        self._sets = []
        self._sinks = []

        #specials
        self._broadcast = []

        self._types = []

    def register_type(self, type, serializer, deserializer):
        """
        Registers the given type with this environment, allowing all operators within to
        (de-)serialize objects of the given type.

        :param type: class of the objects to be (de-)serialized
        :param serializer: instance of the serializer
        :param deserializer: instance of the deserializer
        """
        self._types.append((pack(">i",126 - len(self._types))[3:], type, serializer, deserializer))

    def read_csv(self, path, types, line_delimiter="\n", field_delimiter=','):
        """
        Create a DataSet that represents the tuples produced by reading the given CSV file.

        :param path: The path of the CSV file.
        :param types: Specifies the types for the CSV fields.
        :return:A CsvReader that can be used to configure the CSV input.
        """
        child = OperationInfo()
        child_set = DataSet(self, child)
        child.identifier = _Identifier.SOURCE_CSV
        child.delimiter_line = line_delimiter
        child.delimiter_field = field_delimiter
        child.path = path
        child.types = types
        self._sources.append(child)
        return child_set

    def read_text(self, path):
        """
        Creates a DataSet that represents the Strings produced by reading the given file line wise.

        The file will be read with the system's default character set.

        :param path: The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
        :return: A DataSet that represents the data read from the given file as text lines.
        """
        child = OperationInfo()
        child_set = DataSet(self, child)
        child.identifier = _Identifier.SOURCE_TEXT
        child.path = path
        self._sources.append(child)
        return child_set

    def from_elements(self, *elements):
        """
        Creates a new data set that contains the given elements.

        The elements must all be of the same type, for example, all of the String or Integer.
        The sequence of elements must not be empty.

        :param elements: The elements to make up the data set.
        :return: A DataSet representing the given list of elements.
        """
        child = OperationInfo()
        child_set = DataSet(self, child)
        child.identifier = _Identifier.SOURCE_VALUE
        child.values = elements
        self._sources.append(child)
        return child_set

    def set_degree_of_parallelism(self, degree):
        """
        Sets the degree of parallelism (DOP) for operations executed through this environment.

        Setting a DOP of x here will cause all operators (such as join, map, reduce) to run with x parallel instances.

        :param degreeOfParallelism: The degree of parallelism
        """
        self._parameters.append(("dop", degree))

    def execute(self, local=False, debug=False):
        """
        Triggers the program execution.

        The environment will execute all parts of the program that have resulted in a "sink" operation.
        """
        if debug:
            local = True
        self._parameters.append(("mode", local))
        self._parameters.append(("debug", debug))
        self._optimize_plan()

        plan_mode = sys.stdin.readline().rstrip('\n') == "plan"

        if plan_mode:
            output_path = sys.stdin.readline().rstrip('\n')
            self._connection = Connection.OneWayBusyBufferingMappedFileConnection(output_path)
            self._collector = Collector.TypedCollector(self._connection, self)
            self._send_plan()
            self._connection._write_buffer()
        else:
            import struct
            operator = None
            try:
                port = int(sys.stdin.readline().rstrip('\n'))

                id = int(sys.stdin.readline().rstrip('\n'))
                input_path = sys.stdin.readline().rstrip('\n')
                output_path = sys.stdin.readline().rstrip('\n')

                operator = None
                for set in self._sets:
                    if set.id == id:
                        operator = set.operator
                    if set.id == -id:
                        operator = set.combineop
                operator._configure(input_path, output_path, port, self)
                operator._go()
                sys.stdout.flush()
                sys.stderr.flush()
            except:
                sys.stdout.flush()
                sys.stderr.flush()
                if operator is not None:
                    operator._connection._socket.send(struct.pack(">i", -2))
                raise

    def _optimize_plan(self):
        self._find_chains()

    def _find_chains(self):
        udf = set([_Identifier.MAP, _Identifier.FLATMAP, _Identifier.FILTER, _Identifier.MAPPARTITION,
                   _Identifier.GROUPREDUCE, _Identifier.REDUCE, _Identifier.COGROUP,
                   _Identifier.CROSS, _Identifier.CROSSH, _Identifier.CROSST,
                   _Identifier.JOIN, _Identifier.JOINH, _Identifier.JOINT])
        chainable = set([_Identifier.MAP, _Identifier.FILTER, _Identifier.FLATMAP, _Identifier.GROUPREDUCE, _Identifier.REDUCE])
        multi_input = set([_Identifier.JOIN, _Identifier.JOINH, _Identifier.JOINT, _Identifier.CROSS, _Identifier.CROSSH, _Identifier.CROSST, _Identifier.COGROUP, _Identifier.UNION])
        x = len(self._sets) - 1
        while x > -1:
            child = self._sets[x]
            child_type = child.identifier
            if child_type in chainable:
                parent = child.parent
                parent_type = parent.identifier
                if len(parent.sinks) == 0:
                    if child_type == _Identifier.GROUPREDUCE or child_type == _Identifier.REDUCE:
                        if child.combine:
                            while parent_type == _Identifier.GROUP or parent_type == _Identifier.SORT:
                                parent = parent.parent
                                parent_type = parent.identifier
                            if parent_type in udf and len(parent.children) == 1:
                                if parent.operator is not None:
                                    function = child.combineop
                                    parent.operator._chain(function)
                                    child.combine = False
                                    parent.name += " -> PythonCombine"
                                    for bcvar in child.bcvars:
                                        bcvar_copy = copy.deepcopy(bcvar)
                                        bcvar_copy.parent = parent
                                        self._broadcast.append(bcvar_copy)
                    else:
                        if parent_type in udf and len(parent.children) == 1:
                            parent_op = parent.operator
                            if parent_op is not None:
                                function = child.operator
                                parent_op._chain(function)
                                parent.name += " -> " + child.name
                                parent.types = child.types
                                for grand_child in child.children:
                                    if grand_child.identifier in multi_input:
                                        if grand_child.parent.id == child.id:
                                            grand_child.parent = parent
                                        else:
                                            grand_child.other = parent
                                    else:
                                        grand_child.parent = parent
                                        parent.children.append(grand_child)
                                parent.children.remove(child)
                                for sink in child.sinks:
                                    sink.parent = parent
                                    parent.sinks.append(sink)
                                for bcvar in child.bcvars:
                                    bcvar.parent = parent
                                    parent.bcvars.append(bcvar)
                                self._remove_set((child))
            x -= 1

    def _remove_set(self, set):
        self._sets[:] = [s for s in self._sets if s.id!=set.id]

    def _send_plan(self):
        self._send_parameters()
        self._collector.collect(len(self._sources) + len(self._sets) + len(self._sinks) + len(self._broadcast))
        self._send_sources()
        self._send_operations()
        self._send_sinks()
        self._send_broadcast()

    def _send_parameters(self):
        self._collector.collect(len(self._parameters))
        for parameter in self._parameters:
            self._collector.collect(parameter)

    def _send_sources(self):
        for source in self._sources:
            identifier = source.identifier
            collect = self._collector.collect
            collect(identifier)
            collect(source.id)
            for case in Switch(identifier):
                if case(_Identifier.SOURCE_CSV):
                    collect(source.path)
                    collect(source.delimiter_field)
                    collect(source.delimiter_line)
                    collect(source.types)
                    break
                if case(_Identifier.SOURCE_TEXT):
                    collect(source.path)
                    break
                if case(_Identifier.SOURCE_VALUE):
                    collect(len(source.values))
                    for value in source.values:
                        collect(value)
                    break

    def _send_operations(self):
        collect = self._collector.collect
        for set in self._sets:
            identifier = set.identifier
            collect(set.identifier)
            collect(set.id)
            collect(set.parent.id)
            for case in Switch(identifier):
                if case(_Identifier.SORT):
                    collect(set.field)
                    collect(set.order)
                    break
                if case(_Identifier.GROUP):
                    collect(set.keys)
                    break
                if case(_Identifier.COGROUP):
                    collect(set.other.id)
                    collect(set.key1)
                    collect(set.key2)
                    collect(set.types)
                    collect(set.name)
                    break
                if case(_Identifier.CROSS, _Identifier.CROSSH, _Identifier.CROSST):
                    collect(set.other.id)
                    collect(set.types)
                    collect(len(set.projections))
                    for p in set.projections:
                        collect(p[0])
                        collect(p[1])
                    collect(set.name)
                    break
                if case(_Identifier.REDUCE, _Identifier.GROUPREDUCE):
                    collect(set.types)
                    collect(set.combine)
                    collect(set.name)
                    break
                if case(_Identifier.JOIN, _Identifier.JOINH, _Identifier.JOINT):
                    collect(set.key1)
                    collect(set.key2)
                    collect(set.other.id)
                    collect(set.types)
                    collect(len(set.projections))
                    for p in set.projections:
                        collect(p[0])
                        collect(p[1])
                    collect(set.name)
                    break
                if case(_Identifier.MAP, _Identifier.MAPPARTITION, _Identifier.FLATMAP, _Identifier.FILTER):
                    collect(set.types)
                    collect(set.name)
                    break
                if case(_Identifier.UNION):
                    collect(set.other.id)
                    break
                if case(_Identifier.PROJECTION):
                    collect(set.keys)
                    break
                if case():
                    raise KeyError("Environment._send_child_sets(): Invalid operation identifier: " + str(identifier))

    def _send_sinks(self):
        for sink in self._sinks:
            identifier = sink.identifier
            collect = self._collector.collect
            collect(identifier)
            collect(sink.parent.id)
            for case in Switch(identifier):
                if case(_Identifier.SINK_CSV):
                    collect(sink.path)
                    collect(sink.delimiter_field)
                    collect(sink.delimiter_line)
                    collect(sink.write_mode)
                    break;
                if case(_Identifier.SINK_TEXT):
                    collect(sink.path)
                    collect(sink.write_mode)
                    break
                if case(_Identifier.SINK_PRINT):
                    collect(sink.to_err)
                    break

    def _send_broadcast(self):
        collect = self._collector.collect
        for entry in self._broadcast:
            collect(_Identifier.BROADCAST)
            collect(entry.parent.id)
            collect(entry.other.id)
            collect(entry.name)
