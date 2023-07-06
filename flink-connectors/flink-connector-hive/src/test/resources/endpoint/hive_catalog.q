# hive_catalog.q - CREATE/DROP/SHOW/USE CATALOG/DATABASE
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ==========================================================================
# test hive catalog
# ==========================================================================

# set to default dialect to execute the statements supported only in Flink default dialect
SET table.sql-dialect = default;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

show current catalog;
!output
+----------------------+
| current catalog name |
+----------------------+
|                 hive |
+----------------------+
1 row in set
!ok

show databases;
!output
+---------------+
| database name |
+---------------+
|       default |
+---------------+
1 row in set
!ok

show tables;
!output
Empty set
!ok

create database additional_test_database;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

use additional_test_database;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# set to hive dialect to execute the statements supported in Hive dialect
SET table.sql-dialect = hive;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

create table param_types_table (
    dec DECIMAL(10, 10),
    ch CHAR(5),
    vch VARCHAR(15)
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

show tables;
!output
+-------------------+
|        table name |
+-------------------+
| param_types_table |
+-------------------+
1 row in set
!ok

# set to flink dialect to execute the statements supported only in Flink default dialect
SET table.sql-dialect = default;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

show current database;
!output
+--------------------------+
|    current database name |
+--------------------------+
| additional_test_database |
+--------------------------+
1 row in set
!ok

# ==========================================================================
# test hive table with parameterized types
# ==========================================================================

# set to hive dialect to test the result of describe table since it's behavior is different from default dialect
SET table.sql-dialect = hive;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

describe additional_test_database.param_types_table;
!output
+----------+----------------+---------+
| col_name |      data_type | comment |
+----------+----------------+---------+
|      dec | decimal(10,10) |         |
|       ch |        char(5) |         |
|      vch |    varchar(15) |         |
+----------+----------------+---------+
3 rows in set
!ok

# test the SELECT query can run successfully, even result is empty
select * from hive.additional_test_database.param_types_table;
!output
Empty set
!ok
