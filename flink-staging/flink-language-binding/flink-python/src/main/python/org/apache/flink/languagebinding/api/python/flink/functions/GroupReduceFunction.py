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
        self._sort_ops = []
        self._combine = False
        self._values = []

    def _configure(self, input_file, output_file, port):
        if self._combine:
            self._connection = Connection.BufferingTCPMappedFileConnection(input_file, output_file, port)
            self._iterator = Iterator.Iterator(self._connection)
            self._collector = Collector.Collector(self._connection)
            self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)
            self._run = self._run_combine
        else:
            self._connection = Connection.BufferingTCPMappedFileConnection(input_file, output_file, port)
            self._iterator = Iterator.Iterator(self._connection)
            self._group_iterator = Iterator.GroupIterator(self._iterator, self._keys)
            self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)
            self._configure_chain(Collector.Collector(self._connection))
        self._open()

    def _open(self):
        if self._keys is None:
            self._extract_keys = self._extract_keys_id

    def _close(self):
        self._sort_and_combine()
        self._collector._close()

    def _set_grouping_keys(self, keys):
        self._keys = keys

    def _set_sort_ops(self, ops):
        self._sort_ops = ops

    def _run(self):#reduce
        connection = self._connection
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
        connection.send_end_signal()

    def _run_combine(self):#unchained combine
        connection = self._connection
        collector = self._collector
        function = self.combine
        iterator = self._iterator
        while 1:
            result = function(iterator, collector)
            if result is not None:
                for value in result:
                    collector.collect(value)
            connection.send_end_signal()
            connection.reset()

    def collect(self, value):#chained combine
        self._values.append(value)
        if len(self._values) > 1000:
            self._sort_and_combine()

    def _sort_and_combine(self):
        values = self._values
        function = self.combine
        collector = self._collector
        extractor = self._extract_keys
        grouping = defaultdict(list)
        for value in values:
            grouping[extractor(value)].append(value)
        keys = list(grouping.keys())
        keys.sort()
        for key in keys:
            values = grouping[key]
            for op in reversed(self._sort_ops):
                values.sort(key=lambda x:x[op[0]], reverse = op[1] == Order.DESCENDING)
            result = function(Iterator.ListIterator(values), collector)
            if result is not None:
                for res in result:
                    collector.collect(res)
        self._values = []

    def _extract_keys(self, x):
        return tuple([x[k] for k in self._keys])

    def _extract_sort_keys(self, x):
        return tuple(x[k] for k in self._sort_keys)

    def _extract_keys_id(self, x):
        return x

    def reduce(self, iterator, collector):
        pass

    def combine(self, iterator, collector):
        self.reduce(iterator, collector)