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

=========
DataFrame
=========

A DataFrame provides a Pythonic interface for composing data transformations.
Transformation methods return new DataFrames and support fluent chaining.

Example::

    >>> import pyflink.dataframe as pf
    >>> df = pf.from_dict({"id": [1, 2], "name": ["a", "b"]})
    >>> result = df.select("id", "name") \
    ...            .with_column("id_doubled", pf.col("id") * 2) \
    ...            .filter(pf.col("id") > 0)

DataFrame
---------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame

Transformations
---------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.select
    DataFrame.with_column
    DataFrame.filter
    DataFrame.__getitem__

Results
-------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.collect

Expressions
-----------

Functions for constructing column references and literal expressions.

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    col
    lit
