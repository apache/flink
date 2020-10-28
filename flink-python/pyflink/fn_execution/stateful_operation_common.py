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
import time

from typing import Any

from pyflink.datastream.functions import Collector, TimerService


class InternalCollector(Collector):
    """
    Internal implementation of the Collector. It usa a buffer list to store data to be emitted.
    There will be a header flag for each data type. 0 means it is a proc time timer registering
    request, while 1 means it is an event time timer and 2 means it is a normal data. When
    registering a timer, it must take along the corresponding key for it.
    """
    def __init__(self):
        self.buf = []

    def collect_proc_timer(self, a: Any, key: Any):
        self.buf.append((0, a, key, None))

    def collect_event_timer(self, a: Any, key: Any):
        self.buf.append((1, a, key, None))

    def collect_data(self, a: Any):
        self.buf.append((2, a))

    def collect(self, a: Any):
        self.collect_data(a)

    def clear(self):
        self.buf.clear()


class InternalTimerService(TimerService):
    """
    Internal implementation of TimerService.
    """
    def __init__(self, collector, keyed_state_backend):
        self._collector: InternalCollector = collector
        self._keyed_state_backend = keyed_state_backend
        self._current_watermark = None

    def current_processing_time(self) -> int:
        return int(time.time() * 1000)

    def register_processing_time_timer(self, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        self._collector.collect_proc_timer(t, current_key)

    def register_event_time_timer(self, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        self._collector.collect_event_timer(t, current_key)

    def current_watermark(self) -> int:
        return self._current_watermark
