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
from flink.plan.Constants import _Fields, _Identifier
from flink.utilities import Switch
import copy
import sys


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

    def read_csv(self, path, types, line_delimiter="\n", field_delimiter=','):
        """
        Create a DataSet that represents the tuples produced by reading the given CSV file.

        :param path: The path of the CSV file.
        :param types: Specifies the types for the CSV fields.
        :return:A CsvReader that can be used to configure the CSV input.
        """
        child = dict()
        child_set = DataSet(self, child)
        child[_Fields.IDENTIFIER] = _Identifier.SOURCE_CSV
        child[_Fields.DELIMITER_LINE] = line_delimiter
        child[_Fields.DELIMITER_FIELD] = field_delimiter
        child[_Fields.PATH] = path
        child[_Fields.TYPES] = types
        self._sources.append(child)
        return child_set

    def read_text(self, path):
        """
        Creates a DataSet that represents the Strings produced by reading the given file line wise.

        The file will be read with the system's default character set.

        :param path: The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
        :return: A DataSet that represents the data read from the given file as text lines.
        """
        child = dict()
        child_set = DataSet(self, child)
        child[_Fields.IDENTIFIER] = _Identifier.SOURCE_TEXT
        child[_Fields.PATH] = path
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
        child = dict()
        child_set = DataSet(self, child)
        child[_Fields.IDENTIFIER] = _Identifier.SOURCE_VALUE
        child[_Fields.VALUES] = elements
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
            self._collector = Collector.TypedCollector(self._connection)
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
                    if set[_Fields.ID] == id:
                        operator = set[_Fields.OPERATOR]
                    if set[_Fields.ID] == -id:
                        operator = set[_Fields.COMBINEOP]
                operator._configure(input_path, output_path, port)
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
            child_type = child[_Fields.IDENTIFIER]
            if child_type in chainable:
                parent = child[_Fields.PARENT]
                parent_type = parent[_Fields.IDENTIFIER]
                if len(parent[_Fields.SINKS]) == 0:
                    if child_type == _Identifier.GROUPREDUCE or child_type == _Identifier.REDUCE:
                        if child[_Fields.COMBINE]:
                            while parent_type == _Identifier.GROUP or parent_type == _Identifier.SORT:
                                parent = parent[_Fields.PARENT]
                                parent_type = parent[_Fields.IDENTIFIER]
                            if parent_type in udf and len(parent[_Fields.CHILDREN]) == 1:
                                if parent[_Fields.OPERATOR] is not None:
                                    function = child[_Fields.COMBINEOP]
                                    parent[_Fields.OPERATOR]._chain(function)
                                    child[_Fields.COMBINE] = False
                                    parent[_Fields.NAME] += " -> PythonCombine"
                                    for bcvar in child[_Fields.BCVARS]:
                                        bcvar_copy = copy.deepcopy(bcvar)
                                        bcvar_copy[_Fields.PARENT] = parent
                                        self._broadcast.append(bcvar_copy)
                    else:
                        if parent_type in udf and len(parent[_Fields.CHILDREN]) == 1:
                            parent_op = parent[_Fields.OPERATOR]
                            if parent_op is not None:
                                function = child[_Fields.OPERATOR]
                                parent_op._chain(function)
                                parent[_Fields.NAME] += " -> " + child[_Fields.NAME]
                                parent[_Fields.TYPES] = child[_Fields.TYPES]
                                for grand_child in child[_Fields.CHILDREN]:
                                    if grand_child[_Fields.IDENTIFIER] in multi_input:
                                        if grand_child[_Fields.PARENT][_Fields.ID] == child[_Fields.ID]:
                                            grand_child[_Fields.PARENT] = parent
                                        else:
                                            grand_child[_Fields.OTHER] = parent
                                    else:
                                        grand_child[_Fields.PARENT] = parent
                                        parent[_Fields.CHILDREN].append(grand_child)
                                parent[_Fields.CHILDREN].remove(child)
                                for sink in child[_Fields.SINKS]:
                                    sink[_Fields.PARENT] = parent
                                    parent[_Fields.SINKS].append(sink)
                                for bcvar in child[_Fields.BCVARS]:
                                    bcvar[_Fields.PARENT] = parent
                                    parent[_Fields.BCVARS].append(bcvar)
                                self._remove_set((child))
            x -= 1

    def _remove_set(self, set):
        self._sets[:] = [s for s in self._sets if s[_Fields.ID]!=set[_Fields.ID]]

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
            identifier = source[_Fields.IDENTIFIER]
            collect = self._collector.collect
            collect(identifier)
            collect(source[_Fields.ID])
            for case in Switch(identifier):
                if case(_Identifier.SOURCE_CSV):
                    collect(source[_Fields.PATH])
                    collect(source[_Fields.DELIMITER_FIELD])
                    collect(source[_Fields.DELIMITER_LINE])
                    collect(source[_Fields.TYPES])
                    break
                if case(_Identifier.SOURCE_TEXT):
                    collect(source[_Fields.PATH])
                    break
                if case(_Identifier.SOURCE_VALUE):
                    collect(len(source[_Fields.VALUES]))
                    for value in source[_Fields.VALUES]:
                        collect(value)
                    break

    def _send_operations(self):
        collect = self._collector.collect
        for set in self._sets:
            identifier = set.get(_Fields.IDENTIFIER)
            collect(set[_Fields.IDENTIFIER])
            collect(set[_Fields.ID])
            collect(set[_Fields.PARENT][_Fields.ID])
            for case in Switch(identifier):
                if case(_Identifier.SORT):
                    collect(set[_Fields.FIELD])
                    collect(set[_Fields.ORDER])
                    break
                if case(_Identifier.GROUP):
                    collect(set[_Fields.KEYS])
                    break
                if case(_Identifier.COGROUP):
                    collect(set[_Fields.OTHER][_Fields.ID])
                    collect(set[_Fields.KEY1])
                    collect(set[_Fields.KEY2])
                    collect(set[_Fields.TYPES])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.CROSS, _Identifier.CROSSH, _Identifier.CROSST):
                    collect(set[_Fields.OTHER][_Fields.ID])
                    collect(set[_Fields.TYPES])
                    collect(len(set[_Fields.PROJECTIONS]))
                    for p in set[_Fields.PROJECTIONS]:
                        collect(p[0])
                        collect(p[1])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.REDUCE, _Identifier.GROUPREDUCE):
                    collect(set[_Fields.TYPES])
                    collect(set[_Fields.COMBINE])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.JOIN, _Identifier.JOINH, _Identifier.JOINT):
                    collect(set[_Fields.KEY1])
                    collect(set[_Fields.KEY2])
                    collect(set[_Fields.OTHER][_Fields.ID])
                    collect(set[_Fields.TYPES])
                    collect(len(set[_Fields.PROJECTIONS]))
                    for p in set[_Fields.PROJECTIONS]:
                        collect(p[0])
                        collect(p[1])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.MAP, _Identifier.MAPPARTITION, _Identifier.FLATMAP, _Identifier.FILTER):
                    collect(set[_Fields.TYPES])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.UNION):
                    collect(set[_Fields.OTHER][_Fields.ID])
                    break
                if case(_Identifier.PROJECTION):
                    collect(set[_Fields.KEYS])
                    break
                if case():
                    raise KeyError("Environment._send_child_sets(): Invalid operation identifier: " + str(identifier))

    def _send_sinks(self):
        for sink in self._sinks:
            identifier = sink[_Fields.IDENTIFIER]
            collect = self._collector.collect
            collect(identifier)
            collect(sink[_Fields.PARENT][_Fields.ID])
            for case in Switch(identifier):
                if case(_Identifier.SINK_CSV):
                    collect(sink[_Fields.PATH])
                    collect(sink[_Fields.DELIMITER_FIELD])
                    collect(sink[_Fields.DELIMITER_LINE])
                    collect(sink[_Fields.WRITE_MODE])
                    break;
                if case(_Identifier.SINK_TEXT):
                    collect(sink[_Fields.PATH])
                    collect(sink[_Fields.WRITE_MODE])
                    break
                if case(_Identifier.SINK_PRINT):
                    collect(sink[_Fields.TO_ERR])
                    break

    def _send_broadcast(self):
        collect = self._collector.collect
        for entry in self._broadcast:
            collect(_Identifier.BROADCAST)
            collect(entry[_Fields.PARENT][_Fields.ID])
            collect(entry[_Fields.OTHER][_Fields.ID])
            collect(entry[_Fields.NAME])