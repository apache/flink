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
from pyflink.datastream import TimerService


class TimerServiceImpl(TimerService):
    def __init__(self, timer_service):
        self._timer_service = timer_service

    def current_processing_time(self):
        return self._timer_service.currentProcessingTime()

    def current_watermark(self):
        return self._timer_service.currentWatermark()

    def register_processing_time_timer(self, timestamp: int):
        self._timer_service.registerProcessingTimeTimer(timestamp)

    def register_event_time_timer(self, timestamp: int):
        self._timer_service.registerEventTimeTimer(timestamp)

    def delete_processing_time_timer(self, timestamp: int):
        self._timer_service.deleteProcessingTimeTimer(timestamp)

    def delete_event_time_timer(self, timestamp: int):
        self._timer_service.deleteEventTimeTimer(timestamp)
