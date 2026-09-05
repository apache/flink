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

=============
Configuration
=============

A unified entry point for Flink configuration. The module-level singleton ``pf.config``
accepts any Flink configuration key and buffers the value, so configuration can be set at
any time -- even before an environment exists. Buffered values are used when the underlying
TableEnvironment is created, and are applied to an injected environment for every key it
does not already set explicitly.

Example::

    >>> import pyflink.dataframe as pf
    >>> _ = pf.config.set("parallelism.default", "4") \
    ...              .set("execution.runtime-mode", "batch")
    >>> pf.config.get("parallelism.default")
    '4'

DataFrameConfig
---------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    config
    DataFrameConfig
    DataFrameConfig.set
    DataFrameConfig.get
