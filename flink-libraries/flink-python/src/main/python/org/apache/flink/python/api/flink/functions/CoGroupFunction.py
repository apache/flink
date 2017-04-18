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
from flink.functions import Function, RuntimeContext
from flink.connection import Iterator, Connection, Collector


class CoGroupFunction(Function.Function):
    def __init__(self):
        super(CoGroupFunction, self).__init__()
        self._keys1 = None
        self._keys2 = None

    def _configure(self, input_file, output_file, mmap_size, port, env, info, subtask_index):
        self._connection = Connection.TwinBufferingTCPMappedFileConnection(input_file, output_file, mmap_size, port)
        self._iterator = Iterator.Iterator(self._connection, env, 0)
        self._iterator2 = Iterator.Iterator(self._connection, env, 1)
        self._cgiter = Iterator.CoGroupIterator(self._iterator, self._iterator2, self._keys1, self._keys2)
        self._collector = Collector.Collector(self._connection, env, info)
        self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector, subtask_index)
        if info.chained_info is not None:
            info.chained_info.operator._configure_chain(self.context, self._collector, info.chained_info)
            self._collector = info.chained_info.operator

    def _run(self):
        collector = self._collector
        iterator = self._cgiter
        function = self.co_group
        iterator._init()
        while iterator.next():
            result = function(iterator.p1, iterator.p2, collector)
            if result is not None:
                for res in result:
                    collector.collect(res)
            while iterator.p1.has_next():
                iterator.p1.next()
            while iterator.p2.has_next():
                iterator.p2.next()
        collector._close()

    def co_group(self, iterator1, iterator2, collector):
        pass
