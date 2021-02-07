---
title: "Data Types"
weight: 31
type: docs
aliases:
  - /dev/python/table-api-users-guide/python_types.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Data Types

This page describes the data types supported in PyFlink Table API.

Data Type
---------

A *data type* describes the logical type of a value in the table ecosystem. It can be used to declare input and/or
output types of Python user-defined functions. Users of the Python Table API work with instances of
`pyflink.table.types.DataType` within the Python Table API or when defining user-defined functions.

A `DataType` instance declares the **logical type** which does not imply a concrete physical representation for transmission
or storage. All pre-defined data types are available in `pyflink.table.types` and can be instantiated with the utility methods
defined in `pyflink.table.types.DataTypes`.

A list of all pre-defined data types can be found [below]({{< ref "docs/dev/table/types" >}}#list-of-data-types).

Data Type and Python Type Mapping
------------------

A *data type* can be used to declare input and/or output types of Python user-defined functions. The inputs
will be converted to Python objects corresponding to the data type and the type of the user-defined functions
result must also match the defined data type.

For vectorized Python UDF, the input types and output type are `pandas.Series`. The element type
of the `pandas.Series` corresponds to the specified data type.

| Data Type | Python Type | Pandas Type |
|-----------|------|--------------------|
| `BOOLEAN` | `bool` | `numpy.bool_` |
| `TINYINT` | `int` | `numpy.int8` |
| `SMALLINT` | `int` | `numpy.int16` |
| `INT` | `int` | `numpy.int32` |
| `BIGINT` | `int` | `numpy.int64` |
| `FLOAT` | `float` | `numpy.float32` |
| `DOUBLE` | `float` | `numpy.float64` |
| `VARCHAR` | `str` | `str` |
| `VARBINARY` | `bytes` | `bytes` |
| `DECIMAL` | `decimal.Decimal` | `decimal.Decimal` |
| `DATE` | `datetime.date` | `datetime.date` |
| `TIME` | `datetime.time` | `datetime.time` |
| `TimestampType` | `datetime.datetime` | `datetime.datetime` |
| `LocalZonedTimestampType` | `datetime.datetime` | `datetime.datetime` |
| `INTERVAL YEAR TO MONTH` | `int` | `Not Supported Yet` |
| `INTERVAL DAY TO SECOND` | `datetime.timedelta` | `Not Supported Yet` |
| `ARRAY` | `list` | `numpy.ndarray` |
| `MULTISET` | `list` | `Not Supported Yet` |
| `MAP` | `dict` | `Not Supported Yet` |
| `ROW` | `Row` | `dict` |
