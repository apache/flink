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

from abc import abstractmethod, ABC
from typing import Generic, TypeVar

K = TypeVar('K')
N = TypeVar('N')


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
