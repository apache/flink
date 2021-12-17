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

from py4j.protocol import Py4JJavaError

from pyflink.util.exceptions import convert_py4j_exception

__all__ = ['CompletableFuture']


class CompletableFuture(object):
    """
    A Future that may be explicitly completed (setting its value and status), supporting dependent
    functions and actions that trigger upon its completion.

    When two or more threads attempt to set_result, set_exception, or cancel a CompletableFuture,
    only one of them succeeds.

    .. versionadded:: 1.11.0
    """

    def __init__(self, j_completable_future, py_class=None):
        self._j_completable_future = j_completable_future
        self._py_class = py_class

    def cancel(self) -> bool:
        """
        Completes this CompletableFuture if not already completed.

        :return: true if this task is now cancelled

        .. versionadded:: 1.11.0
        """
        return self._j_completable_future.cancel(True)

    def cancelled(self) -> bool:
        """
        Returns true if this CompletableFuture was cancelled before it completed normally.

        .. versionadded:: 1.11.0
        """
        return self._j_completable_future.isCancelled()

    def done(self) -> bool:
        """
        Returns true if completed in any fashion: normally, exceptionally, or via cancellation.

        .. versionadded:: 1.11.0
        """
        return self._j_completable_future.isDone()

    def result(self):
        """
        Waits if necessary for this future to complete, and then returns its result.

        :return: the result value

        .. versionadded:: 1.11.0
        """
        if self._py_class is None:
            return self._j_completable_future.get()
        else:
            return self._py_class(self._j_completable_future.get())

    def exception(self):
        """
        Returns the exception that was set on this future or None if no exception was set.

        .. versionadded:: 1.11.0
        """
        if self._j_completable_future.isCompletedExceptionally():
            try:
                self._j_completable_future.getNow(None)
            except Py4JJavaError as e:
                return convert_py4j_exception(e)
        else:
            return None

    def __str__(self):
        return self._j_completable_future.toString()
