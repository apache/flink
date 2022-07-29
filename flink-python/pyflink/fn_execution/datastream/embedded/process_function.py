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
from pyflink.datastream import ProcessFunction, TimerService


class InternalProcessFunctionContext(ProcessFunction.Context, TimerService):
    def __init__(self, context):
        self._context = context

    def timer_service(self) -> TimerService:
        return self

    def timestamp(self) -> int:
        return self._context.timestamp()

    def current_processing_time(self):
        return self._context.currentProcessingTime()

    def current_watermark(self):
        return self._context.currentWatermark()

    def register_processing_time_timer(self, timestamp: int):
        raise Exception("Register timers is only supported on a keyed stream.")

    def register_event_time_timer(self, timestamp: int):
        raise Exception("Register timers is only supported on a keyed stream.")

    def delete_processing_time_timer(self, t: int):
        raise Exception("Deleting timers is only supported on a keyed streams.")

    def delete_event_time_timer(self, t: int):
        raise Exception("Deleting timers is only supported on a keyed streams.")
