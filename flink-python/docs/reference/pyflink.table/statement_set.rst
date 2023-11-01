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

============
StatementSet
============

StatementSet
------------

A :class:`~StatementSet` accepts pipelines defined by DML statements or :class:`~Table` objects.
The planner can optimize all added statements together and then submit them as one job.

The added statements will be cleared when calling the :func:`~StatementSet.execute` method.

.. currentmodule:: pyflink.table.statement_set

.. autosummary::
    :toctree: api/

    StatementSet.add_insert_sql
    StatementSet.attach_as_datastream
    StatementSet.add_insert
    StatementSet.explain
    StatementSet.execute


TableResult
-----------

A :class:`~pyflink.table.TableResult` is the representation of the statement execution result.

.. currentmodule:: pyflink.table.table_result

.. autosummary::
    :toctree: api/

    TableResult.get_job_client
    TableResult.wait
    TableResult.get_table_schema
    TableResult.get_result_kind
    TableResult.collect
    TableResult.print


ResultKind
----------

ResultKind defines the types of the result.

:data:`SUCCESS`:

The statement (e.g. DDL, USE) executes successfully, and the result only contains a simple "OK".

:data:`SUCCESS_WITH_CONTENT`:

The statement (e.g. DML, DQL, SHOW) executes successfully, and the result contains important
content.

.. currentmodule:: pyflink.table.table_result

.. autosummary::
    :toctree: api/

    ResultKind.SUCCESS
    ResultKind.SUCCESS_WITH_CONTENT
