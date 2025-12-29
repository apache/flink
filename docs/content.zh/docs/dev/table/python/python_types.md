---
title: "数据类型"
weight: 32
type: docs
aliases:
  - /zh/dev/python/table-api-users-guide/python_types.html
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

# 数据类型

本节描述PyFlink Table API中所支持的数据类型.

Data Type
---------

在Table生态系统中，数据类型用于描述值的逻辑类型。它可以用来声明Python用户自定义函数的输入／输出类型。
Python Table API的用户可以在Python Table API中，或者定义Python用户自定义函数时，使用`pyflink.table.types.DataType`实例。

`DataType`实例声明了数据的**逻辑类型**，这并不能用于推断数据在进行传输或存储时的具体物理表示形式。
所有预定义的数据类型都位于`pyflink.table.types`中，并且可以通过类`pyflink.table.types.DataTypes`中所定义的方法创建。

可以在[下面]({{< ref "docs/dev/table/types" >}}#list-of-data-types)找到所有预定义数据类型的列表。

数据类型（Data Type）和Python类型的映射关系
------------------

*数据类型*可用于声明Python用户自定义函数的输入/输出类型。输入数据将被转换为与所定义的数据类型相对应的Python对象，用户自定义函数的执行结果的类型也必须与所定义的数据类型匹配。

对于向量化Python UDF，输入类型和输出类型都为`pandas.Series`。`pandas.Series`中的元素类型对应于指定的数据类型。

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
