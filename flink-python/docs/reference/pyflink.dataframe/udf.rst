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

=============================
User-Defined Scalar Functions
=============================

Use :func:`pyflink.dataframe.udf` to apply Python code to one or more DataFrame
columns. A scalar UDF produces one logical output column and can be used in
:meth:`~pyflink.dataframe.DataFrame.with_column`,
:meth:`~pyflink.dataframe.DataFrame.with_columns`, and
:meth:`~pyflink.dataframe.DataFrame.select`.

DataFrame scalar UDFs support general synchronous and asynchronous callables,
and synchronous pandas or Arrow vectorized callables. See :func:`pyflink.dataframe.udf` for declaration forms, type
inference, execution modes, and examples.

Arrow Vectorized Functions
==========================

Arrow UDFs receive columns as ``pyarrow.Array`` values and return an ``Array``
or ``ChunkedArray`` of the same length. They require an explicit logical
``return_dtype``. Container annotations infer Arrow mode; unannotated functions
can select ``func_type="arrow"`` explicitly.

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyflink.dataframe as pf

    @pf.udf(return_dtype=pf.DataType.string())
    def normalize_name(names: pa.Array) -> pa.Array:
        return pc.utf8_upper(names)

    df = pf.from_records([("Alice",), ("Bob",)], schema=["name"])
    result = df.with_column("normalized_name", normalize_name(pf.col("name")))

Multiple column arguments and scalar literals can be combined. Literals remain
Python scalars, and at least one argument must be column-valued. ROW columns use
``StructArray``; nested results and chunked arrays can feed subsequent UDFs.
Declared types, nested nullability, and row counts are validated without implicit
element casts. Python lists or scalars, Arrow tables, and record batches are not
supported scalar results.

Explicit ``func_type`` overrides annotations. Mixed pandas and Arrow container
annotations require an explicit choice. Arrow UDFs are synchronous; per-UDF
concurrency and batch-size options are not provided.

API Reference
=============

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    udf
