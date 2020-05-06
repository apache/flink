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
from asyncio import Future

__all__ = ['CompletableFuture']


class CompletableFuture(Future):
    """
    A Future that may be explicitly completed (setting its value and status), supporting dependent
    functions and actions that trigger upon its completion.

    When two or more threads attempt to set_result, set_exception, or cancel a CompletableFuture,
    only one of them succeeds.
    """

    def __init__(self, j_completable_future, py_class=None):
        super().__init__()
        self._j_completable_future = j_completable_future
        self._py_class = py_class

    def cancel(self):
        return self._j_completable_future.cancel(True)

    def cancelled(self):
        return self._j_completable_future.isCancelled()

    def done(self):
        return self._j_completable_future.isDone()

    def result(self):
        if self._py_class is None:
            return self._j_completable_future.get()
        else:
            return self._py_class(self._j_completable_future.get())

    def exception(self):
        return self._exception

    def set_result(self, result):
        return self._j_completable_future.complete(result)

    def set_exception(self, exception):
        self._exception = exception
        return self._j_completable_future.completeExceptionally(exception)

    def __str__(self):
        return self._j_completable_future.toString()
