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

    def _configure(self, input_file, output_file, port, env, info, subtask_index):
        super(ReduceFunction, self)._configure(input_file, output_file, port, env, info, subtask_index)
        if len(info.key1) == 0:
            self._run = self._run_all_reduce
        else:
            self._run = self._run_grouped_reduce
            self._group_iterator = Iterator.GroupIterator(self._iterator, info.key1)

    def _run(self):
        pass

    def _run_all_reduce(self):
        collector = self._collector
        function = self.reduce
        iterator = self._iterator
        if iterator.has_next():
            base = iterator.next()
            for value in iterator:
                base = function(base, value)
            collector.collect(base)
        collector._close()

    def _run_grouped_reduce(self):
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

    def reduce(self, value1, value2):
        pass

    def combine(self, value1, value2):
        return self.reduce(value1, value2)
