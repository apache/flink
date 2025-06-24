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
from pyflink.metrics import Counter


class CounterImpl(Counter):
    def __init__(self, inner_counter):
        self._inner_counter = inner_counter

    def inc(self, n: int = 1):
        """
        Increment the current count by the given value.

        .. versionadded:: 1.11.0
        """
        self._inner_counter.inc(n)

    def dec(self, n: int = 1):
        """
        Decrement the current count by 1.

        .. versionadded:: 1.11.0
        """
        self.inc(-n)

    def get_count(self) -> int:
        """
        Returns the current count.

        .. versionadded:: 1.11.0
        """
        from apache_beam.metrics.execution import MetricsEnvironment
        container = MetricsEnvironment.current_container()
        return container.get_counter(self._inner_counter.metric_name).get_cumulative()
