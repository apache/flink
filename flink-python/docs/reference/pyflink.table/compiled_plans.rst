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
Compiled Plans
==============

CompiledPlan
------------

Represents an immutable, fully optimized, and executable entity that has been compiled from a
Table & SQL API pipeline definition. It encodes operators, expressions, functions, data types,
and table connectors.

Every new Flink version might introduce improved optimizer rules, more efficient operators,
and other changes that impact the behavior of previously defined pipelines. In order to ensure
backwards compatibility and enable stateful streaming job upgrades, compiled plans can be
persisted and reloaded across Flink versions. See the
`website documentation <https://flink.apache.org/documentation/>`_ for more information about
provided guarantees during stateful pipeline upgrades.

A plan can be compiled from a SQL query using
:func:`~pyflink.table.TableEnvironment.compile_plan_sql`.
It can be persisted using :func:`~pyflink.table.CompiledPlan.write_to_file` or by manually
extracting the JSON representation with func:`~pyflink.table.CompiledPlan.as_json_string`.
A plan can be loaded back from a file or a string using
:func:`~pyflink.table.TableEnvironment.load_plan` with a :class:`~pyflink.table.PlanReference`.
Instances can be executed using :func:`~pyflink.table.CompiledPlan.execute`.

Depending on the configuration, permanent catalog metadata (such as information about tables
and functions) will be persisted in the plan as well. Anonymous/inline objects will be
persisted (including schema and options) if possible or fail the compilation otherwise.
For temporary objects, only the identifier is part of the plan and the object needs to be
present in the session context during a restore.

JSON encoding is assumed to be the default representation of a compiled plan in all API
endpoints, and is the format used to persist the plan to files by default.
For advanced use cases, :func:`~pyflink.table.CompiledPlan.as_smile_bytes` provides a binary
format representation of the compiled plan.

.. note::
    Plan restores assume a stable session context. Configuration, loaded modules and
    catalogs, and temporary objects must not change. Schema evolution and changes of function
    signatures are not supported.

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    CompiledPlan.as_json_string
    CompiledPlan.as_smile_bytes
    CompiledPlan.write_to_file
    CompiledPlan.get_flink_version
    CompiledPlan.print_json_string
    CompiledPlan.execute
    CompiledPlan.explain
    CompiledPlan.print_explain

PlanReference
-------------

Unresolved pointer to a persisted plan.

A plan represents a static, executable entity that has been compiled from a Table & SQL API
pipeline definition.

You can load the content of this reference into a :class:`~pyflink.table.CompiledPlan`
using :func:`~pyflink.table.TableEnvironment.load_plan` with a
:class:`~pyflink.table.PlanReference`, or you can directly load and execute it with
:func:`~pyflink.table.TableEnvironment.execute_plan`.

.. seealso:: :class:`~pyflink.table.CompiledPlan`

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    PlanReference.from_file
    PlanReference.from_json_string
    PlanReference.from_smile_bytes
