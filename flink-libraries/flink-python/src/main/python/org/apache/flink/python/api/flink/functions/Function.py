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
from abc import ABCMeta, abstractmethod
from collections import deque
from flink.connection import Connection, Iterator, Collector
from flink.connection.Iterator import IntegerDeserializer, StringDeserializer, _get_deserializer
from flink.functions import RuntimeContext


class Function(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._connection = None
        self._iterator = None
        self._collector = None
        self.context = None
        self._env = None

    def _configure(self, input_file, output_file, mmap_size, port, env, info, subtask_index):
        self._connection = Connection.BufferingTCPMappedFileConnection(input_file, output_file, mmap_size, port)
        self._iterator = Iterator.Iterator(self._connection, env)
        self._collector = Collector.Collector(self._connection, env, info)
        self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector, subtask_index)
        self._env = env
        if info.chained_info is not None:
            info.chained_info.operator._configure_chain(self.context, self._collector, info.chained_info)
            self._collector = info.chained_info.operator

    def _configure_chain(self, context, collector, info):
        self.context = context
        if info.chained_info is None:
            self._collector = collector
        else:
            self._collector = info.chained_info.operator
            info.chained_info.operator._configure_chain(context, collector, info.chained_info)

    @abstractmethod
    def _run(self):
        pass

    def _close(self):
        self._collector._close()
        if self._connection is not None:
            self._connection.close()

    def _go(self):
        self._receive_broadcast_variables()
        self._run()

    def _receive_broadcast_variables(self):
        con = self._connection
        deserializer_int = IntegerDeserializer()
        broadcast_count = deserializer_int.deserialize(con.read_secondary)
        deserializer_string = StringDeserializer()
        for _ in range(broadcast_count):
            name = deserializer_string.deserialize(con.read_secondary)
            bc = deque()
            if con.read_secondary(1) == b"\x01":
                serializer_data = _get_deserializer(con.read_secondary, self._env._types)
                value = serializer_data.deserialize(con.read_secondary)
                bc.append(value)
                while con.read_secondary(1) == b"\x01":
                    con.read_secondary(serializer_data.get_type_info_size()) #skip type info
                    value = serializer_data.deserialize(con.read_secondary)
                    bc.append(value)
            self.context._add_broadcast_variable(name, bc)



