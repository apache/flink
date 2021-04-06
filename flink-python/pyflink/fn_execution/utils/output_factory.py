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
from enum import Enum
from io import BytesIO

from typing import List, Tuple, Iterable

from pyflink.common import Row
from pyflink.common.serializer import TypeSerializer
from pyflink.datastream.timerservice import InternalTimer
from pyflink.fn_execution.timerservice_impl import TimerOperandType


class RunnerOutputType(Enum):
    NORMAL_RECORD = 0
    TIMER_OPERATION = 1


class RowWithTimerOutputFactory(object):

    def __init__(self, namespace_serializer: TypeSerializer):
        self._namespace_serializer = namespace_serializer

    def from_timers(self, timers: List[Tuple[TimerOperandType, InternalTimer]]) -> Iterable[Row]:
        result = []
        for timer in timers:
            timer_operand_type = timer[0]

            internal_timer = timer[1]
            key = internal_timer.get_key()
            timestamp = internal_timer.get_timestamp()

            namespace = internal_timer.get_namespace()
            bytes_io = BytesIO()
            self._namespace_serializer.serialize(namespace, bytes_io)
            encoded_namespace = bytes_io.getvalue()

            timer_operand_type_value = timer_operand_type.value
            timer_data = Row(timer_operand_type_value, key, timestamp, encoded_namespace)

            runner_output_type_value = RunnerOutputType.TIMER_OPERATION.value
            row = Row(runner_output_type_value, None, timer_data)
            result.append(row)
        return result

    def from_normal_data(self, normal_data_iter: Iterable) -> Iterable[Row]:
        for normal_data in normal_data_iter:
            yield Row(RunnerOutputType.NORMAL_RECORD.value, normal_data, None)
