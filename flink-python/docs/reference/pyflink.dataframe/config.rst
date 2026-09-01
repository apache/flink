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
accepts any Flink configuration key and buffers the value until
``pf.get_or_create_table_environment()`` creates the underlying TableEnvironment. Because
the values are supplied at creation time, options that can only be chosen then, such as
``execution.runtime-mode``, take effect.

Configuration must be set before the environment exists; ``pf.config.set()`` raises once an
environment is active. An environment injected via ``pf.set_table_environment()`` is treated
as fully configured and does not receive buffered values.

Example::

    >>> import pyflink.dataframe as pf
    >>> _ = pf.config.set("parallelism.default", "4") \
    ...              .set("execution.runtime-mode", "batch")
    >>> pf.config.get("parallelism.default")
    '4'

config
------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    config
    config.set
    config.get
