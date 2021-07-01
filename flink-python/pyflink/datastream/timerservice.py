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
from abc import ABC, abstractmethod


class TimerService(ABC):
    """
    Interface for working with time and timers.
    """

    @abstractmethod
    def current_processing_time(self):
        """
        Returns the current processing time.
        """
        pass

    @abstractmethod
    def current_watermark(self):
        """
        Returns the current event-time watermark.
        """
        pass

    @abstractmethod
    def register_processing_time_timer(self, timestamp: int):
        """
        Registers a timer to be fired when processing time passes the given time.

        Timers can internally be scoped to keys and/or windows. When you set a timer in a keyed
        context, such as in an operation on KeyedStream then that context will so be active when you
        receive the timer notification.

        :param timestamp: The processing time of the timer to be registered.
        """
        pass

    @abstractmethod
    def register_event_time_timer(self, timestamp: int):
        """
        Registers a timer tobe fired when the event time watermark passes the given time.

        Timers can internally be scoped to keys and/or windows. When you set a timer in a keyed
        context, such as in an operation on KeyedStream then that context will so be active when you
        receive the timer notification.

        :param timestamp: The event time of the timer to be registered.
        """
        pass

    def delete_processing_time_timer(self, timestamp: int):
        """
        Deletes the processing-time timer with the given trigger time. This method has only an
        effect if such a timer was previously registered and did not already expire.

        Timers can internally be scoped to keys and/or windows. When you delete a timer, it is
        removed from the current keyed context.

        :param timestamp: The given trigger time of timer to be deleted.
        """
        pass

    def delete_event_time_timer(self, timestamp: int):
        """
        Deletes the event-time timer with the given trigger time. This method has only an effect if
        such a timer was previously registered and did not already expire.

        Timers can internally be scoped to keys and/or windows. When you delete a timer, it is
        removed from the current keyed context.

        :param timestamp: The given trigger time of timer to be deleted.
        """
        pass
