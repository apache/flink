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

from pyflink.datastream import TimerService, TimeDomain
from pyflink.datastream.functions import KeyedProcessFunction, KeyedCoProcessFunction, \
    ProcessFunction, CoProcessFunction


class InternalKeyedProcessFunctionOnTimerContext(
        KeyedProcessFunction.OnTimerContext, KeyedCoProcessFunction.OnTimerContext):
    """
    Internal implementation of OnTimerContext of KeyedProcessFunction and KeyedCoProcessFunction.
    """

    def __init__(self, timer_service: TimerService):
        self._timer_service = timer_service
        self._time_domain = None
        self._timestamp = None
        self._current_key = None

    def get_current_key(self):
        return self._current_key

    def set_current_key(self, current_key):
        self._current_key = current_key

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._timestamp

    def set_timestamp(self, ts: int):
        self._timestamp = ts

    def time_domain(self) -> TimeDomain:
        return self._time_domain

    def set_time_domain(self, td: TimeDomain):
        self._time_domain = td


class InternalKeyedProcessFunctionContext(
        KeyedProcessFunction.Context, KeyedCoProcessFunction.Context):
    """
    Internal implementation of Context of KeyedProcessFunction and KeyedCoProcessFunction.
    """

    def __init__(self, timer_service: TimerService):
        self._timer_service = timer_service
        self._timestamp = None
        self._current_key = None

    def get_current_key(self):
        return self._current_key

    def set_current_key(self, current_key):
        self._current_key = current_key

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._timestamp

    def set_timestamp(self, ts: int):
        self._timestamp = ts


class InternalProcessFunctionContext(ProcessFunction.Context, CoProcessFunction.Context):
    """
    Internal implementation of ProcessFunction.Context and CoProcessFunction.Context.
    """

    def __init__(self, timer_service: TimerService):
        self._timer_service = timer_service
        self._timestamp = None

    def timer_service(self) -> TimerService:
        return self._timer_service

    def timestamp(self) -> int:
        return self._timestamp

    def set_timestamp(self, ts: int):
        self._timestamp = ts
