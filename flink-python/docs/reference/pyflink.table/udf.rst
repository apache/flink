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

======================
User Defined Functions
======================

Scalar Function
---------------

A user-defined scalar functions maps zero, one, or multiple scalar values to a new scalar value.

.. currentmodule:: pyflink.table.udf

.. autosummary::
    :toctree: api/

    ScalarFunction
    udf


Table Function
--------------

A user-defined table function creates zero, one, or multiple rows to a new row value.

.. currentmodule:: pyflink.table.udf

.. autosummary::
    :toctree: api/

    TableFunction
    udtf


Aggregate Function
------------------

A user-defined aggregate function maps scalar values of multiple rows to a new scalar value.

.. currentmodule:: pyflink.table.udf

.. autosummary::
    :toctree: api/

    AggregateFunction
    udaf


Table Aggregate Function
------------------------

A user-defined table aggregate function maps scalar values of multiple rows to zero, one, or multiple
rows (or structured types). If an output record consists of only one field, the structured record can
be omitted, and a scalar value can be emitted that will be implicitly wrapped into a row by the runtime.

.. currentmodule:: pyflink.table.udf

.. autosummary::
    :toctree: api/

    TableAggregateFunction
    udtaf

DataView
--------

If an accumulator needs to store large amounts of data, `pyflink.table.ListView` and `pyflink.table.MapView`
could be used instead of list and dict. These two data structures provide the similar functionalities
as list and dict, however usually having better performance by leveraging Flink’s state backend to eliminate
unnecessary state access. You can use them by declaring `DataTypes.LIST_VIEW(...)` and `DataTypes.MAP_VIEW(...)`
in the accumulator type.

.. currentmodule:: pyflink.table.data_view

.. autosummary::
    :toctree: api/

    ListView
    MapView
