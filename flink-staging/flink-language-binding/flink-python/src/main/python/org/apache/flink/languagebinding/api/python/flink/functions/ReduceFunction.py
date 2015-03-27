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


class ReduceFunction(Function.Function):
    def __init__(self):
        super(ReduceFunction, self).__init__()
        self._keys = None
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
            if self._keys is None:
                self._run = self._run_allreduce
            else:
                self._group_iterator = Iterator.GroupIterator(self._iterator, self._keys)
            self._configure_chain(Collector.Collector(self._connection))
            self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)

    def _set_grouping_keys(self, keys):
        self._keys = keys

    def _close(self):
        self._sort_and_combine()
        self._collector._close()

    def _run(self):#grouped reduce
        collector = self._collector
        function = self.reduce
        iterator = self._group_iterator
        iterator._init()
        while iterator.has_group():
            iterator.next_group()
            if iterator.has_next():
                base = iterator.next()
                for value in iterator:
                    base = function(base, value)
            collector.collect(base)
        collector._close()

    def _run_allreduce(self):#ungrouped reduce
        collector = self._collector
        function = self.reduce
        iterator = self._iterator
        if iterator.has_next():
            base = iterator.next()
            for value in iterator:
                base = function(base, value)
            collector.collect(base)
        collector._close()

    def _run_combine(self):#unchained combine
        connection = self._connection
        collector = self._collector
        function = self.combine
        iterator = self._iterator
        while 1:
            if iterator.has_next():
                base = iterator.next()
                while iterator.has_next():
                    base = function(base, iterator.next())
            collector.collect(base)
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
            iterator = Iterator.ListIterator(grouping[key])
            base = iterator.next()
            while iterator.has_next():
                base = function(base, iterator.next())
            collector.collect(base)
        self._values = []

    def _extract_keys(self, x):
        return tuple([x[k] for k in self._keys])

    def reduce(self, value1, value2):
        pass

    def combine(self, value1, value2):
        return self.reduce(value1, value2)