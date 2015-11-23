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
from collections import defaultdict
from flink.functions import Function, RuntimeContext
from flink.connection import Connection, Iterator, Collector
from flink.plan.Constants import Order


class GroupReduceFunction(Function.Function):
    def __init__(self):
        super(GroupReduceFunction, self).__init__()
        self._keys = None

    def _configure(self, input_file, output_file, port, env):
        self._connection = Connection.BufferingTCPMappedFileConnection(input_file, output_file, port)
        self._iterator = Iterator.Iterator(self._connection, env)
        if self._keys is None:
            self._run = self._run_all_group_reduce
        else:
            self._run = self._run_grouped_group_reduce
            self._group_iterator = Iterator.GroupIterator(self._iterator, self._keys)
        self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)
        self._collector = Collector.Collector(self._connection, env)

    def _set_grouping_keys(self, keys):
        self._keys = keys

    def _run(self):
        pass

    def _run_all_group_reduce(self):
        collector = self._collector
        function = self.reduce
        iterator = self._iterator
        result = function(iterator, collector)
        if result is not None:
            for value in result:
                collector.collect(value)
        collector._close()

    def _run_grouped_group_reduce(self):
        collector = self._collector
        function = self.reduce
        iterator = self._group_iterator
        iterator._init()
        while iterator.has_group():
            iterator.next_group()
            result = function(iterator, collector)
            if result is not None:
                for value in result:
                    collector.collect(value)
        collector._close()

    def reduce(self, iterator, collector):
        pass

    def combine(self, iterator, collector):
        self.reduce(iterator, collector)