---
title: "Python 数据类型"
nav-parent_id: python_tableapi
nav-pos: 15
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

This page describes the data types supported in PyFlink Table API.

* This will be replaced by the TOC
{:toc}

Data Type
---------

 _数据类型_描述表生态系统中值的逻辑类型。它可以用来声明Python用户定义函数的输入和/或
 输出类型。Python Table API的用户在Python Table API中操作`` pyflink.table.types.DataType ``实例，也可以在定义用户自定义函数时操作。

 ` DataType `实例声明**逻辑类型**，这并不能用于推断进行传输或存储的具体物理表示形式。
 所有预定义的数据类型在`` pyflink.table.types ``都可用，并且可以使用`pyflink.table.types.DataTypes`定义的
 工具函数实例化。 

可以在[下面]({{ site.baseurl }}/zh/dev/table/types.html#list-of-data-types)找到所有预定义数据类型的列表。 

数据类型和Python类型映射
------------------

*数据类型*可用于声明Python用户定义函数的输入和/或输出类型。输入将
转换为与数据类型相对应的Python对象，并且用户定义函数结果的类型
也必须与定义的数据类型匹配。 

对于矢量化Python UDF，输入类型和输出类型为`pandas.Series` 。 `pandas.Series`的元素类型
对应于指定的数据类型。 

| Data Type | Python Type | Pandas Type |
|:-----------------|:-----------------------|
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
| `INTERVAL YEAR TO MONTH` | `int` | `Not Supported` |
| `INTERVAL DAY TO SECOND` | `datetime.timedelta` | `Not Supported` |
| `ARRAY` | `list` | `numpy.ndarray` |
| `MULTISET` | `list` | `Not Supported` |
| `MAP` | `dict` | `Not Supported` |
| `ROW` | `Row` | `dict` |
