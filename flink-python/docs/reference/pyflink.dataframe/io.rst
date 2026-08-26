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

==============
Input / Output
==============

DataFrame I/O functions read from and write to external systems through Flink connectors.
Connector-specific methods provide convenient interfaces for common systems, while generic
methods expose the raw connector identifiers and string options used by Table connector factories.

Readers
-------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    read_generic

Writers
-------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.write_generic
