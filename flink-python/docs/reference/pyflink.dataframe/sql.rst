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

===
SQL
===

Execute SQL SELECT queries against DataFrames.

Example::

    >>> import pyflink.dataframe as pf
    >>> df1 = pf.from_dict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    >>> df2 = pf.from_dict({"a": [1, 2, 3], "c": ["p", "q", "r"]})
    >>> joined = pf.sql("SELECT df1.a, b, c FROM df1 JOIN df2 ON df1.a = df2.a")
    >>> result = pf.sql(
    ...     "SELECT * FROM src WHERE a > 1",
    ...     auto_bind=False,
    ...     src=df1,
    ... )
    >>> pf.sql("SELECT a, b FROM df1").filter(pf.col("a") > 1).to_pandas()

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    sql
