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

Table Functions and Row Expansion
=================================

Use :meth:`~pyflink.dataframe.DataFrame.flat_map` when each input row can produce
zero, one, or many output rows. The result contains only the emitted columns.
An undecorated function receives a dictionary keyed by input column name::

    from typing import Any, Dict, Iterator, TypedDict
    import pyflink.dataframe as pf

    class Token(TypedDict):
        word: str
        length: int

    def split(row: Dict[str, Any]) -> Iterator[Token]:
        for word in row["text"].split():
            yield {"word": word, "length": len(word)}

    df = pf.from_dict({"text": ["hello world", "flink", ""]})
    tokens = df.flat_map(split)

The return annotation determines the emitted row type, not the collection type.
``TypedDict`` supplies column names; alternatively, pass a named struct as
``return_dtype``. Scalar output uses the column name ``f0``. Unnamed multi-field
tuple output requires an explicit named struct in ``flat_map``.

For a reusable declaration, use :func:`pyflink.dataframe.udtf`. When passed to
``flat_map``, a decorated UDTF receives a Flink ``Row`` with input column names
in both process and thread execution modes::

    from pyflink.common import Row

    @pf.udtf
    def split_row(row: Row) -> Iterator[Token]:
        for word in row["text"].split():
            yield {"word": word, "length": len(word)}

    tokens = df.flat_map(split_row)

Do not specify ``return_dtype`` again when passing a UDTF declaration. The
declaration already contains its return type. Both forms build a lazy plan;
actions such as ``collect()`` execute it. A function may return ``None`` or an
empty iterator to emit no rows. A list or generator emits multiple rows; a scalar,
tuple, ``Row``, or dictionary returned directly represents one row. To emit a
single SQL ``NULL``, return ``[None]`` or yield ``None``.

UDTFs also support callable instances and ``TableFunction`` instances or classes.
A ``TableFunction`` class must have a zero-argument constructor and is instantiated
on the client when declared with ``pf.udtf``. The resulting instance, or an explicitly
provided instance, is serialized with the job.
Use ``TableFunction.open()`` to initialize worker resources and ``close()`` to
release them. Asynchronous UDTFs and per-function concurrency are not supported.

API Reference
=============

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    udf
    udtf
