.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################


================
Asynchronous I/O
================

AsyncDataStream
---------------

.. currentmodule:: pyflink.datastream.async_data_stream

.. autosummary::
    :toctree: api/

    AsyncDataStream.unordered_wait
    AsyncDataStream.unordered_wait_with_retry
    AsyncDataStream.ordered_wait
    AsyncDataStream.ordered_wait_with_retry


AsyncFunction
-------------

.. currentmodule:: pyflink.datastream.functions

.. autosummary::
    :toctree: api/

    AsyncFunction.async_invoke
    AsyncFunction.timeout


AsyncRetryStrategy
------------------

.. currentmodule:: pyflink.datastream.functions

.. autosummary::
    :toctree: api/

    AsyncRetryStrategy.can_retry
    AsyncRetryStrategy.get_backoff_time_millis
    AsyncRetryStrategy.get_retry_predicate
    AsyncRetryStrategy.no_restart
    AsyncRetryStrategy.fixed_delay
    AsyncRetryStrategy.exponential_backoff


AsyncRetryPredicate
-------------------

.. currentmodule:: pyflink.datastream.functions

.. autosummary::
    :toctree: api/

    AsyncRetryPredicate.result_predicate
    AsyncRetryPredicate.exception_predicate


Async Retry Predicates Utilities
--------------------------------

.. currentmodule:: pyflink.datastream.async_retry_predicates

.. autosummary::
    :toctree: api/

    empty_result_predicate
    has_exception_predicate
    exception_type_predicate
