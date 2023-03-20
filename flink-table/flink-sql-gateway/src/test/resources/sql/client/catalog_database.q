# catalog_database.q - CREATE/DROP/SHOW/USE CATALOG/DATABASE
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
# validation test
# ==========================================================================

use non_existing_db;
!output
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: A database with name [non_existing_db] does not exist in the catalog: [default_catalog].
!error

use catalog non_existing_catalog;
!output
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: A catalog with name [non_existing_catalog] does not exist.
!error

create catalog invalid.cat with ('type'='generic_in_memory');
!output
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.sql.parser.impl.ParseException: Encountered "." at line 1, column 23.
Was expecting one of:
    <EOF>
    "WITH" ...
    ";" ...
!error

create database my.db;
!output
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Catalog my does not exist
!error

# ==========================================================================
# test catalog
# ==========================================================================

create catalog c1 with ('type'='generic_in_memory');
!output
[INFO] Execute statement succeed.
!info

show catalogs;
!output
+-----------------+
|    catalog name |
+-----------------+
|              c1 |
| default_catalog |
+-----------------+
2 rows in set
!ok

show current catalog;
!output
+----------------------+
| current catalog name |
+----------------------+
|      default_catalog |
+----------------------+
1 row in set
!ok

use catalog c1;
!output
[INFO] Execute statement succeed.
!info

show current catalog;
!output
+----------------------+
| current catalog name |
+----------------------+
|                   c1 |
+----------------------+
1 row in set
!ok

drop catalog default_catalog;
!output
[INFO] Execute statement succeed.
!info

drop catalog c1;
!output
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: Cannot drop a catalog which is currently in use.
!error

# ==========================================================================
# test database
# ==========================================================================

create database db1;
!output
[INFO] Execute statement succeed.
!info

show databases;
!output
+---------------+
| database name |
+---------------+
|       default |
|           db1 |
+---------------+
2 rows in set
!ok

show current database;
!output
+-----------------------+
| current database name |
+-----------------------+
|               default |
+-----------------------+
1 row in set
!ok

use db1;
!output
[INFO] Execute statement succeed.
!info

show current database;
!output
+-----------------------+
| current database name |
+-----------------------+
|                   db1 |
+-----------------------+
1 row in set
!ok

create database db2 comment 'db2_comment' with ('k1' = 'v1');
!output
[INFO] Execute statement succeed.
!info

alter database db2 set ('k1' = 'a', 'k2' = 'b');
!output
[INFO] Execute statement succeed.
!info

# TODO: show database properties when we support DESCRIBE DATABSE

show databases;
!output
+---------------+
| database name |
+---------------+
|       default |
|           db1 |
|           db2 |
+---------------+
3 rows in set
!ok

drop database if exists db2;
!output
[INFO] Execute statement succeed.
!info

show databases;
!output
+---------------+
| database name |
+---------------+
|       default |
|           db1 |
+---------------+
2 rows in set
!ok

# ==========================================================================
# test playing with keyword identifiers
# ==========================================================================

create catalog `mod` with ('type'='generic_in_memory');
!output
[INFO] Execute statement succeed.
!info

use catalog `mod`;
!output
[INFO] Execute statement succeed.
!info

use `default`;
!output
[INFO] Execute statement succeed.
!info

drop database `default`;
!output
[INFO] Execute statement succeed.
!info

drop catalog `mod`;
!output
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: Cannot drop a catalog which is currently in use.
!error

use catalog `c1`;
!output
[INFO] Execute statement succeed.
!info

drop catalog `mod`;
!output
[INFO] Execute statement succeed.
!info

# ==========================================================================
# test hive catalog
# ==========================================================================

create catalog hivecatalog with (
 'type' = 'hive-test',
 'hive-version' = '2.3.4'
);
!output
[INFO] Execute statement succeed.
!info

use catalog hivecatalog;
!output
[INFO] Execute statement succeed.
!info

show current catalog;
!output
+----------------------+
| current catalog name |
+----------------------+
|          hivecatalog |
+----------------------+
1 row in set
!ok

show databases;
!output
+--------------------------+
|            database name |
+--------------------------+
| additional_test_database |
|                  default |
+--------------------------+
2 rows in set
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

use additional_test_database;
!output
[INFO] Execute statement succeed.
!info

show tables;
!output
+------------+
| table name |
+------------+
| test_table |
+------------+
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

describe hivecatalog.`default`.param_types_table;
!output
+------+-----------------+------+-----+--------+-----------+
| name |            type | null | key | extras | watermark |
+------+-----------------+------+-----+--------+-----------+
|  dec | DECIMAL(10, 10) | TRUE |     |        |           |
|   ch |         CHAR(5) | TRUE |     |        |           |
|  vch |     VARCHAR(15) | TRUE |     |        |           |
+------+-----------------+------+-----+--------+-----------+
3 rows in set
!ok

SET 'execution.runtime-mode' = 'batch';
!output
[INFO] Execute statement succeed.
!info

SET 'sql-client.execution.result-mode' = 'tableau';
!output
[INFO] Execute statement succeed.
!info

# test the SELECT query can run successfully, even result is empty
select * from hivecatalog.`default`.param_types_table;
!output
Empty set
!ok

# ==========================================================================
# test create/drop table with catalog
# ==========================================================================

use catalog hivecatalog;
!output
[INFO] Execute statement succeed.
!info

create table MyTable1 (a int, b string) with ('connector' = 'values');
!output
[INFO] Execute statement succeed.
!info

create table MyTable2 (a int, b string) with ('connector' = 'values');
!output
[INFO] Execute statement succeed.
!info

# hive catalog is case-insensitive
show tables;
!output
+------------+
| table name |
+------------+
|   mytable1 |
|   mytable2 |
| test_table |
+------------+
3 rows in set
!ok

show views;
!output
Empty set
!ok

create view MyView1 as select 1 + 1;
!output
[INFO] Execute statement succeed.
!info

create view MyView2 as select 1 + 1;
!output
[INFO] Execute statement succeed.
!info

show views;
!output
+-----------+
| view name |
+-----------+
|   myview1 |
|   myview2 |
+-----------+
2 rows in set
!ok

# test create with full qualified name
create table c1.db1.MyTable3 (a int, b string) with ('connector' = 'values');
!output
[INFO] Execute statement succeed.
!info

create table c1.db1.MyTable4 (a int, b string) with ('connector' = 'values');
!output
[INFO] Execute statement succeed.
!info

create view c1.db1.MyView3 as select 1 + 1;
!output
[INFO] Execute statement succeed.
!info

create view c1.db1.MyView4 as select 1 + 1;
!output
[INFO] Execute statement succeed.
!info

use catalog c1;
!output
[INFO] Execute statement succeed.
!info

use db1;
!output
[INFO] Execute statement succeed.
!info

show tables;
!output
+------------+
| table name |
+------------+
|   MyTable3 |
|   MyTable4 |
|    MyView3 |
|    MyView4 |
+------------+
4 rows in set
!ok

show views;
!output
+-----------+
| view name |
+-----------+
|   MyView3 |
|   MyView4 |
+-----------+
2 rows in set
!ok

# test create with database name
create table `default`.MyTable5 (a int, b string) with ('connector' = 'values');
!output
[INFO] Execute statement succeed.
!info

create table `default`.MyTable6 (a int, b string) with ('connector' = 'values');
!output
[INFO] Execute statement succeed.
!info

create view `default`.MyView5 as select 1 + 1;
!output
[INFO] Execute statement succeed.
!info

create view `default`.MyView6 as select 1 + 1;
!output
[INFO] Execute statement succeed.
!info

use `default`;
!output
[INFO] Execute statement succeed.
!info

show tables;
!output
+------------+
| table name |
+------------+
|   MyTable5 |
|   MyTable6 |
|    MyView5 |
|    MyView6 |
+------------+
4 rows in set
!ok

show views;
!output
+-----------+
| view name |
+-----------+
|   MyView5 |
|   MyView6 |
+-----------+
2 rows in set
!ok

drop table db1.MyTable3;
!output
[INFO] Execute statement succeed.
!info

drop view db1.MyView3;
!output
[INFO] Execute statement succeed.
!info

use db1;
!output
[INFO] Execute statement succeed.
!info

show tables;
!output
+------------+
| table name |
+------------+
|   MyTable4 |
|    MyView4 |
+------------+
2 rows in set
!ok

show views;
!output
+-----------+
| view name |
+-----------+
|   MyView4 |
+-----------+
1 row in set
!ok

drop table c1.`default`.MyTable6;
!output
[INFO] Execute statement succeed.
!info

drop view c1.`default`.MyView6;
!output
[INFO] Execute statement succeed.
!info

use `default`;
!output
[INFO] Execute statement succeed.
!info

show tables;
!output
+------------+
| table name |
+------------+
|   MyTable5 |
|    MyView5 |
+------------+
2 rows in set
!ok

show views;
!output
+-----------+
| view name |
+-----------+
|   MyView5 |
+-----------+
1 row in set
!ok

# ==========================================================================
# test configuration is changed (trigger new ExecutionContext)
# ==========================================================================

SET 'sql-client.execution.result-mode' = 'changelog';
!output
[INFO] Execute statement succeed.
!info

create table MyTable7 (a int, b string) with ('connector' = 'values');
!output
[INFO] Execute statement succeed.
!info

show tables;
!output
+------------+
| table name |
+------------+
|   MyTable5 |
|   MyTable7 |
|    MyView5 |
+------------+
3 rows in set
!ok

reset;
!output
[INFO] Execute statement succeed.
!info

drop table MyTable5;
!output
[INFO] Execute statement succeed.
!info

show tables;
!output
+------------+
| table name |
+------------+
|   MyTable7 |
|    MyView5 |
+------------+
2 rows in set
!ok

# ==========================================================================
# test enhanced show tables
# ==========================================================================

create catalog catalog1 with ('type'='generic_in_memory');
!output
[INFO] Execute statement succeed.
!info

create database catalog1.db1;
!output
[INFO] Execute statement succeed.
!info

create table catalog1.db1.person (a int, b string) with ('connector' = 'datagen');
!output
[INFO] Execute statement succeed.
!info

create table catalog1.db1.dim (a int, b string) with ('connector' = 'datagen');
!output
[INFO] Execute statement succeed.
!info

create table catalog1.db1.address (a int, b string) with ('connector' = 'datagen');
!output
[INFO] Execute statement succeed.
!info

create view catalog1.db1.v_person as select * from catalog1.db1.person;
!output
[INFO] Execute statement succeed.
!info

show tables from catalog1.db1;
!output
+------------+
| table name |
+------------+
|    address |
|        dim |
|     person |
|   v_person |
+------------+
4 rows in set
!ok

show tables from catalog1.db1 like '%person%';
!output
+------------+
| table name |
+------------+
|     person |
|   v_person |
+------------+
2 rows in set
!ok

show tables in catalog1.db1 not like '%person%';
!output
+------------+
| table name |
+------------+
|    address |
|        dim |
+------------+
2 rows in set
!ok

use catalog catalog1;
!output
[INFO] Execute statement succeed.
!info

show tables from db1 like 'p_r%';
!output
+------------+
| table name |
+------------+
|     person |
+------------+
1 row in set
!ok
