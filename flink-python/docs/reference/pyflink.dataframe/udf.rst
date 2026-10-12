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
User-Defined Functions
======================

Scalar Functions
================

Use :func:`pyflink.dataframe.udf` to apply Python code to one or more DataFrame
columns. A scalar UDF produces one logical output column and can be used in
:meth:`~pyflink.dataframe.DataFrame.with_column`,
:meth:`~pyflink.dataframe.DataFrame.with_columns`, and
:meth:`~pyflink.dataframe.DataFrame.select`.

DataFrame scalar UDFs support general synchronous and asynchronous callables,
and synchronous pandas or Arrow vectorized callables. See :func:`pyflink.dataframe.udf`
for declaration forms, type inference, execution modes, and examples.

Per-UDF Execution Settings
--------------------------

``concurrency`` overrides the parallelism of the Python stage for a scalar UDF,
including synchronous, asynchronous, pandas, and Arrow UDFs. Python UDFs with
different execution settings run in separate operators. It does not change the
job-wide configuration.

For pandas and Arrow UDFs, ``batch_size`` overrides the maximum number of rows
per batch. The configured bundle size remains an upper bound. General scalar
UDFs reject ``batch_size``. Both options must be positive integers; omitting them
preserves the existing execution settings.

.. code-block:: python

    import pyflink.dataframe as pf
    import pyarrow as pa
    import pyarrow.compute as pc

    @pf.udf(return_dtype=int, concurrency=4, batch_size=256)
    def add_one(values: pa.Array) -> pa.Array:
        return pc.add(values, 1)

    result = df.with_columns(next_value=add_one(pf.col("value")))

Table Functions
===============

Use :func:`pyflink.dataframe.udtf` to declare a Python function that emits zero or
more rows per invocation. A table UDF can be used with
:meth:`~pyflink.dataframe.DataFrame.flat_map`, which returns only the emitted columns.

DataFrame table UDFs support synchronous functions, callable classes and instances, and
``TableFunction`` instances or classes. See :func:`pyflink.dataframe.udtf` for
declaration forms, type inference, lifecycle, and examples, and
:meth:`~pyflink.dataframe.DataFrame.flat_map` for row input and output semantics.

API Reference
=============

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    udf
    udtf
