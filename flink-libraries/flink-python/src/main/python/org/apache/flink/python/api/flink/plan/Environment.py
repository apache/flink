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
from flink.connection import Iterator
from flink.plan.DataSet import DataSet
from flink.plan.Constants import _Identifier
from flink.plan.OperationInfo import OperationInfo
from flink.utilities import Switch
import socket as SOCKET
import copy
import sys
from struct import pack


class EnvironmentContainer(object):
    """Keeps track of which ExecutionEnvironment is active."""

    _environment_counter = 0
    _environment_id_to_execute = None
    _plan_mode = None

    def create_environment(self):
        """Creates a new environment with a unique id."""
        env = Environment(self, self._environment_counter)
        self._environment_counter += 1
        return env

    def is_planning(self):
        """
        Checks whether we are generating the plan or executing an operator.

        :return: True, if the plan is generated, false otherwise
        """
        if self._plan_mode is None:
            mode = sys.stdin.readline().rstrip('\n')
            if mode == "plan":
                self._plan_mode = True
            elif mode == "operator":
                self._plan_mode = False
            else:
                raise ValueError("Invalid mode specified: " + mode)
        return self._plan_mode

    def should_execute(self, environment):
        """
        Checks whether the given ExecutionEnvironment should run the contained plan.

        :param: ExecutionEnvironment to check
        :return: True, if the environment should run the contained plan, false otherise
        """
        if self._environment_id_to_execute is None:
            self._environment_id_to_execute = int(sys.stdin.readline().rstrip('\n'))

        return environment._env_id == self._environment_id_to_execute


container = EnvironmentContainer()


def get_environment():
    """
    Creates an execution environment that represents the context in which the program is currently executed.

    :return:The execution environment of the context in which the program is executed.
    """
    return container.create_environment()


class Environment(object):
    def __init__(self, container, env_id):
        # util
        self._counter = 0

        #parameters
        self._dop = -1
        self._retry = 0

        self._container = container
        self._env_id = env_id

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

    def generate_sequence(self, frm, to):
        """
        Creates a new data set that contains the given sequence

        :param frm: The start number for the sequence.
        :param to: The end number for the sequence.
        :return: A DataSet representing the given sequence of numbers.
        """
        child = OperationInfo()
        child_set = DataSet(self, child)
        child.identifier = _Identifier.SOURCE_SEQ
        child.frm = frm
        child.to = to
        self._sources.append(child)
        return child_set

    def set_parallelism(self, parallelism):
        """
        Sets the parallelism for operations executed through this environment.

        Setting a DOP of x here will cause all operators (such as join, map, reduce) to run with x parallel instances.

        :param parallelism: The degree of parallelism
        """
        self._dop = parallelism

    def get_parallelism(self):
        """
        Gets the parallelism with which operation are executed by default.
        :return The parallelism used by operations.
        """
        return self._dop

    def set_number_of_execution_retries(self, count):
        self._retry = count

    def get_number_of_execution_retries(self):
        return self._retry

    def execute(self, local=False):
        """
        Triggers the program execution.

        The environment will execute all parts of the program that have resulted in a "sink" operation.
        """
        self._optimize_plan()

        if self._container.is_planning():
            port = int(sys.stdin.readline().rstrip('\n'))
            self._connection = Connection.PureTCPConnection(port)
            self._iterator = Iterator.PlanIterator(self._connection, self)
            self._collector = Collector.PlanCollector(self._connection, self)
            self._send_plan()
            result = self._receive_result()
            self._connection.close()
            return result
        else:
            import struct
            operator = None
            port = None
            try:
                if self._container.should_execute(self):
                    id = int(sys.stdin.readline().rstrip('\n'))

                    port = int(sys.stdin.readline().rstrip('\n'))
                    subtask_index = int(sys.stdin.readline().rstrip('\n'))
                    mmap_size = int(sys.stdin.readline().rstrip('\n'))
                    input_path = sys.stdin.readline().rstrip('\n')
                    output_path = sys.stdin.readline().rstrip('\n')

                    used_set = None
                    operator = None

                    for set in self._sets:
                        if set.id == id:
                            used_set = set
                            operator = set.operator
                    operator._configure(input_path, output_path, mmap_size, port, self, used_set, subtask_index)
                    operator._go()
                    operator._close()
                    sys.stdout.flush()
                    sys.stderr.flush()
            except:
                sys.stdout.flush()
                sys.stderr.flush()
                if operator is not None and operator._connection is not None:
                    operator._connection._socket.send(struct.pack(">i", -2))
                elif port is not None:
                    socket = SOCKET.socket(family=SOCKET.AF_INET, type=SOCKET.SOCK_STREAM)
                    socket.connect((SOCKET.gethostbyname("localhost"), port))
                    socket.send(struct.pack(">i", -2))
                    socket.close()
                raise

    def _optimize_plan(self):
        self._find_chains()

    def _find_chains(self):
        chainable = set([_Identifier.MAP, _Identifier.FILTER, _Identifier.FLATMAP])
        dual_input = set([_Identifier.JOIN, _Identifier.JOINH, _Identifier.JOINT, _Identifier.CROSS, _Identifier.CROSSH, _Identifier.CROSST, _Identifier.COGROUP, _Identifier.UNION])
        x = len(self._sets) - 1
        while x > -1:
            # CHAIN(parent -> child) -> grand_child
            # for all intents and purposes the child set ceases to exist; it is merged into the parent
            child = self._sets[x]
            child_type = child.identifier
            if child_type in chainable:
                parent = child.parent
                # we can only chain to an actual python udf (=> operator is not None)
                # we may only chain if the parent has only 1 child
                # we may only chain if the parent is not used as a broadcast variable
                # we may only chain if the parent does not use the child as a broadcast variable
                if parent.operator is not None and len(parent.children) == 1 and len(parent.sinks) == 0 and parent not in self._broadcast and child not in parent.bcvars:
                    parent.chained_info = child
                    parent.name += " -> " + child.name
                    parent.types = child.types
                    # grand_children now belong to the parent
                    for grand_child in child.children:
                        # dual_input operations have 2 parents; hence we have to change the correct one
                        if grand_child.identifier in dual_input:
                            if grand_child.parent.id == child.id:
                                grand_child.parent = parent
                            else:
                                grand_child.other = parent
                        else:
                            grand_child.parent = parent
                        parent.children.append(grand_child)
                    # if child is used as a broadcast variable the parent must now be used instead
                    for s in self._sets:
                        if child in s.bcvars:
                            s.bcvars.remove(child)
                            s.bcvars.append(parent)
                    for bcvar in self._broadcast:
                        if bcvar.other.id == child.id:
                            bcvar.other = parent
                    # child sinks now belong to the parent
                    for sink in child.sinks:
                        sink.parent = parent
                        parent.sinks.append(sink)
                    # child broadcast variables now belong to the parent
                    for bcvar in child.bcvars:
                        bcvar.parent = parent
                        parent.bcvars.append(bcvar)
                    # remove child set as it has been merged into the parent
                    parent.children.remove(child)
                    self._remove_set(child)
            x -= 1

    def _remove_set(self, set):
        self._sets[:] = [s for s in self._sets if s.id != set.id]

    def _send_plan(self):
        self._send_parameters()
        self._send_operations()

    def _send_parameters(self):
        collect = self._collector.collect
        collect(("dop", self._dop))
        collect(("retry", self._retry))
        collect(("id", self._env_id))

    def _send_operations(self):
        self._collector.collect(len(self._sources) + len(self._sets) + len(self._sinks) + len(self._broadcast))
        for source in self._sources:
            self._send_operation(source)
        for set in self._sets:
            self._send_operation(set)
        for sink in self._sinks:
            self._send_operation(sink)
        for bcv in self._broadcast:
            self._send_operation(bcv)

    def _send_operation(self, set):
        collect = self._collector.collect
        collect(set.identifier)
        collect(set.parent.id if set.parent is not None else -1)
        collect(set.other.id if set.other is not None else -1)
        collect(set.field)
        collect(set.order)
        collect(set.keys)
        collect(set.key1)
        collect(set.key2)
        collect(set.types)
        collect(set.uses_udf)
        collect(set.name)
        collect(set.delimiter_line)
        collect(set.delimiter_field)
        collect(set.write_mode)
        collect(set.path)
        collect(set.frm)
        collect(set.to)
        collect(set.id)
        collect(set.to_err)
        collect(set.count)
        collect(len(set.values))
        for value in set.values:
            collect(value)
        collect(set.parallelism.value)

    def _receive_result(self):
        jer = JobExecutionResult()
        jer._net_runtime = self._iterator.next()
        return jer


class JobExecutionResult:
    def __init__(self):
        self._net_runtime = 0

    def get_net_runtime(self):
        return self._net_runtime
