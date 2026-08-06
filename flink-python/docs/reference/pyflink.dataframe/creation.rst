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

==================
DataFrame Creation
==================

Functions for creating DataFrames from row-oriented and column-oriented Python data, pandas
DataFrames, PyArrow tables, PyFlink Tables, and integer ranges.

``schema`` is an optional list of column names. For dictionaries and mapping records it selects
and reorders named fields. For pandas and Arrow inputs it renames columns positionally and must
contain exactly one name per input column. Names must be non-empty strings and must be unique.

Dictionary and record inputs must contain at least one row. Empty pandas and Arrow inputs are
supported when their column types can be inferred from pandas dtypes or the Arrow schema. An empty
:func:`range` still has one ``id BIGINT`` column.

The native data creators accept an optional ``watermark=(column, expression)`` declaration. The
column must exist and have a timestamp-compatible type. Watermark columns are normalized to
``TIMESTAMP(3)`` or ``TIMESTAMP_LTZ(3)``; sub-millisecond precision is truncated.

Example::

    >>> import pyflink.dataframe as pf
    >>> users = pf.from_records([
    ...     {"id": 1, "name": "Alice"},
    ...     {"id": 2, "name": "Bob"},
    ... ])
    >>> users = pf.from_dict({"id": [1, 2], "name": ["Alice", "Bob"]})
    >>> identifiers = pf.range(1, 5)

Pandas and Arrow inputs can be renamed positionally::

    >>> import pandas as pd
    >>> import pyarrow as pa
    >>> pandas_users = pf.from_pandas(
    ...     pd.DataFrame({"identifier": [1], "display_name": ["Alice"]}),
    ...     schema=["id", "name"],
    ... )
    >>> arrow_users = pf.from_arrow(
    ...     pa.table({"identifier": [1], "display_name": ["Alice"]}),
    ...     schema=["id", "name"],
    ... )

A watermark can be attached while creating event data::

    >>> from datetime import datetime
    >>> events = pf.from_records(
    ...     [{"id": 1, "ts": datetime(2026, 1, 1)}],
    ...     watermark=("ts", "ts - INTERVAL '5' SECOND"),
    ... )

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    from_records
    from_dict
    from_pandas
    from_arrow
    from_table
    range
