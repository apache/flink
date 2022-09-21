---
title: "DESCRIBE Statements"
weight: 8
type: docs
aliases:
  - /dev/table/sql/describe.html
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

# DESCRIBE Statements

The DESCRIBE statements are used to display metadata about a table, such as the column names, their data types, 
comments and partition information. Also DESCRIBE statements can optionally display detailed table information 
like table options, statistics and information for specific columns.


## Run a DESCRIBE statement

{{< tabs "describe" >}}
{{< tab "HOW TO USE" >}}

DESCRIBE statements can be executed in:
- Java: can be executed with the executeSql() method of the TableEnvironment
- Scala: can be executed with the executeSql() method of the TableEnvironment
- Python: can be executed with the execute_sql() method of the TableEnvironment
- [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}})

The following examples show how to run a DESCRIBE statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "SQL CLI" >}}
```sql
-- register a non-partition table named "testTable"
Flink SQL> CREATE TABLE catalog1.testDb.testTable (
> `a` BIGINT,
> `b` STRING,
> `c` BOOLEAN,
> `d` BINARY(5)
> ) with
> (....);
[INFO] Table has been created.

-- register a partition table named "part_table"
Flink SQL> CREATE TABLE catalog1.testDb.part_table (
> `a` BIGINT,
> `b` STRING,
> `c` BOOLEAN,
> `d` BINARY(5)
> ) PARTITIONED BY (`a`
> ) with
> (....);
[INFO] Table has been created.

-- Suppose there are 20 rows in table "testTable", and the statistics has been collected for this catalog

-- For "testTable", describe table
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
        
-- For "testTable", describe extended table
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
    
-- For "testTable", describe table with specifying column
Flink SQL> desc catalog1.testDb.testTable a;
+-----------+------------+
| name      | value      |
+-----------+------------+
| col_name  | a          |
| data_type | BIGINT     |
+-----------+------------+
2 rows in set
    
-- For "testTable", describe extended table with specifying column
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
    
-- Suppose partition table "part_table" has 2 partitions with spec: Partition1 : a=1 and Partition2 : a=2
-- Suppose there are 10 rows in partition (a='1') and 11 rows in partition (a='2'), and the statistics has been collected for this catalog.

-- For partition table "part_table", describe table.        
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
    
-- For partition table "part_table", describe extended table without specifying partition spec.
-- If user don't specify partition spec, all partition stats will be merged, 
-- which equals to "desc extended catalog1.testDb.part_table partition(a)"          
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
    
-- For partition table "part_table", describe extended table with specifying partition spec 'partition(a)'.       
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
| Table Name                   | testTable                          |         |\
| Table Statistics             | 21 rows                            |         |
+------------------------------+------------------------------------+---------+
13 rows in set
    
-- For partition table "part_table", describe extended table with specifying partition spec 'partition(a=1)'.       
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
    
-- For partition table "part_table", describe extended table with specifying column for all partitions.
-- Merged column statistics for all partition will be printed.        
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
    
-- For partition table "part_table", describe extended table with specifying column for single partition 'a=1'.
-- Column statistics for partition 'a=1' will be printed.        
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
For these situation, Describe statement will throw error:
1. Table doesn't exist, a validation error that table doesn't exist will be thrown
2. Column doesn't exist, a validation error that column 'x' doesn't exist will be thrown
3. Try to specify partition specs for none partition table, a validation error that table is not a partition table will be thrown
4. For partition table, try to specify one partition spec whose key is not partition key. A validation error that key 'x' in partition spec {'x'='y'} is not partition key will be thrown
5. for partition table, try to specify one partition spec whose value is not exists(partition not exists). A validation error that partition 'y' not found for partition spec {'x'='y'} will be thrown
{{< /hint >}}

{{< top >}}

## Syntax

```sql
{ DESCRIBE | DESC } [EXTENDED] [catalog_name.][db_name.]table_name [PARTITION(partcol1[=val1] [, partcol2[=val2], ...])] [column_name]
```
