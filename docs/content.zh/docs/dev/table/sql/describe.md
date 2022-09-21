---
title: "DESCRIBE 语句"
weight: 8
type: docs
aliases:
  - /zh/dev/table/sql/describe.html
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

<a name="describe-statements"></a>

# DESCRIBE 语句

DESCRIBE 语句用于展示表的元信息，包括列名，列对应的数据类型，附加信息（comments）和分区信息（对于分区表）等。同时，也可以有选择的展示详细的表信息，
例如表的参数，表的统计信息和指定表某一列的列信息。

<a name="run-a-describe-statement"></a>

## 执行 DESCRIBE 语句

{{< tabs "describe" >}}
{{< tab "使用方法" >}}

DESCRIBE 语句可以通过如下途径使用:
- Java: 可以使用 TableEnvironment 的 executeSql() 方法执行 DESCRIBE 语句
- Scala: 可以使用 TableEnvironment 的 executeSql() 方法执行 DESCRIBE 语句
- Python: 可以使用 TableEnvironment 的 execute_sql() 方法执行 DESCRIBE 语句
- [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}})

以下示例展示了如何在 SQL CLI 中执行 DESCRIBE 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "SQL CLI" >}}
```sql
-- 在 "catalog1.testDb" 中注册一张普通表 "testTable"
Flink SQL> CREATE TABLE catalog1.testDb.testTable (
> `a` BIGINT,
> `b` STRING,
> `c` BOOLEAN,
> `d` BINARY(5)
> ) with
> ( .... );
[INFO] Table has been created.

-- 在 "catalog1.testDb" 中注册一张分区表 "part_table"
Flink SQL> CREATE TABLE catalog1.testDb.part_table (
> `a` BIGINT,
> `b` STRING,
> `c` BOOLEAN,
> `d` BINARY(5)
> ) PARTITIONED BY (`a`
> ) with
> ( .... );
[INFO] Table has been created.

-- 为了更好的展示示例，我们假设普通表 "testTable" 中已经存在 20 行的数据，且表的统计信息已经被收集

-- 普通表 "testTable" 使用 describe table 语句
Flink SQL> desc catalog1.testDb.testTable;
+----------+-----------+---------+
| col_name | data_type | comment |
+----------+-----------+---------+
| a        | BIGINT    |         |
| b        | STRING    |         |
| c        | BOOLEAN   |         |
| d        | BINARY(5) |         |
+----------+-----------+---------+
4 rows in set    
        
-- 普通表 "testTable" 使用 describe extended table 语句
Flink SQL> desc extended catalog1.testDb.testTable;
+------------------------------+------------------------------------+---------+
| col_name                     | data_type                          | comment |
+------------------------------+------------------------------------+---------+
| a                            | BIGINT                             |         |
| b                            | STRING                             |         |
| c                            | BOOLEAN                            |         |
| d                            | BINARY(5)                          |         |
|                              |                                    |         |
| # Detailed Table Information |                                    |         |
| Database Name                | testDb                             |         |
| Table Name                   | testTable                          |         |
| Table Statistics             | 10 rows                            |         |
+------------------------------+------------------------------------+---------+
10 rows in set
    
-- 普通表 "testTable" 使用 describe table 语句并指定某一列
Flink SQL> desc catalog1.testDb.testTable a;
+-----------+------------+
| name      | value      |
+-----------+------------+
| col_name  | a          |
| data_type | BIGINT     |
+-----------+------------+
2 rows in set
    
-- 普通表 "testTable" 使用 describe extended table 语句并指定某一列
Flink SQL> desc extended catalog1.testDb.testTable a;
+----------------+------------+
| name           | value      |
+----------------+------------+
| col_name       | a          |
| data_type      | BIGINT     |
| min            | 1          |
| max            | 10         |
| num_nulls      | 5          |
| distinct_count | 5          |
| avg_col_len    | 8.0        |
| max_col_len    | 8          |
+----------------+------------+
8 rows in set

-- 为了更好的展示本示例在分区表上的不同情况，我们假设分区表 "part_table" 有两个分区 a=1 和 a=2
-- 同时，我们假设分区 a=1 中已经写入了 10 行数据， 分区 a=2 中写入了 11 行数据， 且各分区的统计信息已经被收集

-- 分区表 "part_table" 使用 describe table 语句      
Flink SQL> desc catalog1.testDb.part_table;
+-------------------------+-----------+---------+
| col_name                | data_type | comment |
+-------------------------+-----------+---------+
| b                       | STRING    |         |
| c                       | BOOLEAN   |         |
| d                       | BINARY(5) |         |
|                         |           |         |
| # Partition Information |           |         |
| # col_name              | data_type | comment |
| a                       | BIGINT    |         |
+-------------------------+-----------+---------+
7 rows in set  

-- 分区表 "part_table" 使用 describe extended table 语句且不指定具体的分区信息
-- 如果用户不指定具体的分区信息，则我们默认会展示合并后的所有分区的信息，其等价于执行语句：desc extended catalog1.testDb.part_table partition(a)         
Flink SQL> desc extended catalog1.testDb.part_table;
+------------------------------+------------------------------------+---------+
| col_name                     | data_type                          | comment |
+------------------------------+------------------------------------+---------+
| b                            | STRING                             |         |
| c                            | BOOLEAN                            |         |
| d                            | BINARY(5)                          |         |
|                              |                                    |         |
| # Partition Information      |                                    |         |
| # col_name                   | data_type                          | comment |
| a                            | BIGINT                             |         |
|                              |                                    |         |
| # Detailed Table Information |                                    |         |
| Database Name                | testDb                             |         |
| Table Name                   | testTable                          |         |
| Table Statistics             | 21 rows                            |         |
+------------------------------+------------------------------------+---------+
13 rows in set  

-- 分区表 "part_table" 使用 describe extended table 语句并指定分区信息 "partition(a)"
-- 该语句打印的统计信息是分区统计信息合并后的结果   
Flink SQL> desc extended catalog1.testDb.part_table partition(a);
+------------------------------+------------------------------------+---------+
| col_name                     | data_type                          | comment |
+------------------------------+------------------------------------+---------+
| b                            | STRING                             |         |
| c                            | BOOLEAN                            |         |
| d                            | BINARY(5)                          |         |
|                              |                                    |         |
| # Partition Information      |                                    |         |
| # col_name                   | data_type                          | comment |
| a                            | BIGINT                             |         |
|                              |                                    |         |
| # Detailed Table Information |                                    |         |
| Database Name                | testDb                             |         |
| Table Name                   | testTable                          |         |
| Table Statistics             | 21 rows                            |         |
+------------------------------+------------------------------------+---------+
13 rows in set

-- 分区表 "part_table" 使用 describe extended table 语句并指定分区信息 "partition(a=1)"      
Flink SQL> desc extended catalog1.testDb.part_table partition(a=1);
+------------------------------+------------------------------------+---------+
| col_name                     | data_type                          | comment |
+------------------------------+------------------------------------+---------+
| b                            | STRING                             |         |
| c                            | BOOLEAN                            |         |
| d                            | BINARY(5)                          |         |
|                              |                                    |         |
| # Partition Information      |                                    |         |
| # col_name                   | data_type                          | comment |
| a                            | BIGINT                             |         |
|                              |                                    |         |
| # Detailed Table Information |                                    |         |
| Database Name                | testDb                             |         |
| Table Name                   | testTable                          |         |
| Table Statistics             | 10 rows                            |         |
+------------------------------+------------------------------------+---------+
13 rows in set

-- 分区表 "part_table" 使用 describe extended table 语句并指定某一列
-- 该语句打印的列统计信息是各分区统计信息合并后的结果        
Flink SQL> desc extended catalog1.testDb.testTable partition(a) a;
+----------------+------------+
| name           | value      |
+----------------+------------+
| col_name       | a          |
| data_type      | BIGINT     |
| min            | 1          |
| max            | 15         |
| num_nulls      | 7          |
| distinct_count | 15         |
| avg_col_len    | 8.0        |
| max_col_len    | 8          |
+----------------+------------+
8 rows in set
    
-- 分区表 "part_table" 使用 describe extended table 语句并指定某一列
-- 该语句只打印 a=1 的分区的列统计信息        
Flink SQL> desc extended catalog1.testDb.testTable partition(a=1) a;
+----------------+------------+
| name           | value      |
+----------------+------------+
| col_name       | a          |
| data_type      | BIGINT     |
| min            | 1          |
| max            | 10         |
| num_nulls      | 5          |
| distinct_count | 5          |
| avg_col_len    | 8.0        |
| max_col_len    | 8          |
+----------------+------------+
8 rows in set    
```

{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
对于下列的情况， Describe 语句会报错：
1. 表不存在，会报表不存在的验证错误
2. 列不存在，会报列 'x' 不存在的验证错误
3. 对于非分区表，尝试指定分区信息，会报表是非分区表的验证错误
4. 对于分区表，指定的分区信息里包含了非分区键，会报 'x' in partition spec {'x'='y'} is not partition key 的验证错误
5. 对于分区表，指定的分区不存在，会报 partition 'y' not found for partition spec {'x'='y'} 的验证错误
{{< /hint >}}

{{< top >}}

<a name="syntax"></a>

## 语法

```sql
{ DESCRIBE | DESC } [EXTENDED] [catalog_name.][db_name.]table_name [PARTITION(partcol1[=val1] [, partcol2[=val2], ...])] [column_name]
```
