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
from abc import ABC, abstractmethod
from enum import Enum
from typing import TypeVar, Generic, List, Tuple

K = TypeVar('K')
N = TypeVar('N')


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


class InternalTimerService(Generic[N], ABC):
    """
    Interface for working with time and timers.

    This is the internal version of TimerService that allows to specify a key and a namespace to
    which timers should be scoped.
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
    def register_processing_time_timer(self, namespace: N, t: int):
        """
        Registers a timer to be fired when processing time passes the given time. The namespace you
        pass here will be provided when the timer fires.

        :param namespace: The namespace you pass here will be provided when the timer fires.
        :param t: The processing time of the timer to be registered.
        """
        pass

    @abstractmethod
    def register_event_time_timer(self, namespace: N, t: int):
        """
        Registers a timer to be fired when event time watermark passes the given time. The namespace
        you pass here will be provided when the timer fires.

        :param namespace: The namespace you pass here will be provided when the timer fires.
        :param t: The event time of the timer to be registered.
        """
        pass

    def delete_processing_time_timer(self, namespace: N, t: int):
        """
        Deletes the timer for the given key and namespace.

        :param namespace: The namespace you pass here will be provided when the timer fires.
        :param t: The given trigger time of timer to be deleted.
        """
        pass

    def delete_event_time_timer(self, namespace: N, t: int):
        """
        Deletes the timer for the given key and namespace.

        :param namespace: The namespace you pass here will be provided when the timer fires.
        :param t: The given trigger time of timer to be deleted.
        """
        pass


class InternalTimer(Generic[K, N], ABC):

    @abstractmethod
    def get_timestamp(self) -> int:
        """
        Returns the timestamp of the timer. This value determines the point in time when the timer
        will fire.
        """
        pass

    @abstractmethod
    def get_key(self) -> K:
        """
        Returns the key that is bound to this timer.
        """
        pass

    @abstractmethod
    def get_namespace(self) -> N:
        """
        Returns the namespace that is bound to this timer.
        :return:
        """
        pass


class InternalTimerImpl(InternalTimer[K, N]):

    def __init__(self, timestamp: int, key: K, namespace: N):
        self._timestamp = timestamp
        self._key = key
        self._namespace = namespace

    def get_timestamp(self) -> int:
        return self._timestamp

    def get_key(self) -> K:
        return self._key

    def get_namespace(self) -> N:
        return self._namespace


class TimerOperandType(Enum):
    REGISTER_EVENT_TIMER = 0
    REGISTER_PROC_TIMER = 1
    DELETE_EVENT_TIMER = 2
    DELETE_PROC_TIMER = 3


class InternalTimerServiceImpl(InternalTimerService[N]):
    """
    Internal implementation of InternalTimerService.
    """

    def __init__(self, keyed_state_backend):
        self._keyed_state_backend = keyed_state_backend
        self._current_watermark = None
        self.timers = []  # type: List[Tuple[TimerOperandType, InternalTimer]]

    def current_processing_time(self):
        return int(time.time() * 1000)

    def current_watermark(self):
        return self._current_watermark

    def advance_watermark(self, watermark: int):
        self._current_watermark = watermark

    def register_processing_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        self.timers.append(
            (TimerOperandType.REGISTER_PROC_TIMER, InternalTimerImpl(t, current_key, namespace)))

    def register_event_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        self.timers.append(
            (TimerOperandType.REGISTER_EVENT_TIMER, InternalTimerImpl(t, current_key, namespace)))

    def delete_processing_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        self.timers.append(
            (TimerOperandType.DELETE_PROC_TIMER, InternalTimerImpl(t, current_key, namespace)))

    def delete_event_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        self.timers.append(
            (TimerOperandType.DELETE_EVENT_TIMER, InternalTimerImpl(t, current_key, namespace)))
