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
from typing import TypeVar, Callable, Optional, List, Generic

from pyflink.datastream.functions import AsyncRetryStrategy, AsyncRetryPredicate

__all__ = [
    'RetryPredicate',
    'NoRetryStrategy',
    'FixedDelayRetryStrategy',
    'ExponentialBackoffDelayRetryStrategy'
]


OUT = TypeVar('OUT')


class RetryPredicate(AsyncRetryPredicate, Generic[OUT]):

    def __init__(self,
                 result_predicate: Optional[Callable[[List[OUT]], bool]],
                 exception_predicate: Optional[Callable[[Exception], bool]]):
        self._result_predicate = result_predicate
        self._exception_predicate = exception_predicate

    def result_predicate(self) -> Optional[Callable[[List[OUT]], bool]]:
        return self._result_predicate

    def exception_predicate(self) -> Optional[Callable[[Exception], bool]]:
        return self._exception_predicate


class NoRetryStrategy(AsyncRetryStrategy, Generic[OUT]):

    def can_retry(self, current_attempts: int) -> bool:
        return False

    def get_backoff_time_millis(self, current_attempts: int) -> int:
        return -1

    def get_retry_predicate(self) -> AsyncRetryPredicate[OUT]:
        return RetryPredicate(None, None)


class FixedDelayRetryStrategy(AsyncRetryStrategy, Generic[OUT]):

    def __init__(self,
                 max_attempts: int,
                 backoff_time_millis: int,
                 result_predicate: Optional[Callable[[List[OUT]], bool]],
                 exception_predicate: Optional[Callable[[Exception], bool]]):
        assert max_attempts > 0, "max_attempts should be greater than zero."
        assert backoff_time_millis > 0, "backoff_time_millis should be greater than zero."
        self._max_attempts = max_attempts
        self._backoff_time_millis = backoff_time_millis
        self._result_predicate = result_predicate
        self._exception_predicate = exception_predicate

    def can_retry(self, current_attempts: int) -> bool:
        return current_attempts <= self._max_attempts

    def get_backoff_time_millis(self, current_attempts: int) -> int:
        return self._backoff_time_millis

    def get_retry_predicate(self) -> AsyncRetryPredicate[OUT]:
        return RetryPredicate(self._result_predicate, self._exception_predicate)


class ExponentialBackoffDelayRetryStrategy(AsyncRetryStrategy, Generic[OUT]):

    def __init__(self,
                 max_attempts: int,
                 initial_delay: int,
                 max_retry_delay: int,
                 multiplier: float,
                 result_predicate: Optional[Callable[[List[OUT]], bool]],
                 exception_predicate: Optional[Callable[[Exception], bool]]):
        assert max_attempts > 0, "max_attempts should be greater than zero."
        assert initial_delay > 0, "initial_delay should be greater than zero."
        assert max_retry_delay > 0, "max_retry_delay should be greater than zero."
        assert multiplier > 0, "multiplier should be greater than zero."
        self._max_attempts = max_attempts
        self._max_retry_delay = max_retry_delay
        self._multiplier = multiplier
        self._result_predicate = result_predicate
        self._exception_predicate = exception_predicate
        self._initial_delay = initial_delay
        self._last_retry_delay = initial_delay

    def can_retry(self, current_attempts: int) -> bool:
        return current_attempts <= self._max_attempts

    def get_backoff_time_millis(self, current_attempts: int) -> int:
        if current_attempts <= 1:
            self._last_retry_delay = self._initial_delay
            return self._last_retry_delay

        backoff = int(min(self._last_retry_delay * self._multiplier, self._max_retry_delay))
        self._last_retry_delay = backoff
        return backoff

    def get_retry_predicate(self) -> AsyncRetryPredicate[OUT]:
        return RetryPredicate(self._result_predicate, self._exception_predicate)


NO_RETRY_STRATEGY: AsyncRetryStrategy = NoRetryStrategy()
