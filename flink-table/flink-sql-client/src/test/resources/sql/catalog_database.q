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
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: A database with name [non_existing_db] does not exist in the catalog: [default_catalog].
!error

use catalog non_existing_catalog;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: A catalog with name [non_existing_catalog] does not exist.
!error

create catalog invalid.cat with ('type'='generic_in_memory');
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.sql.parser.impl.ParseException: Encountered "." at line 1, column 23.
Was expecting one of:
    <EOF>
    "WITH" ...
    ";" ...
!error

create database my.db;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Catalog my does not exist
!error

# ==========================================================================
# test catalog
# ==========================================================================

create catalog c1 with ('type'='generic_in_memory');
[INFO] Execute statement succeeded.
!info

show catalogs;
+-----------------+
|    catalog name |
+-----------------+
|              c1 |
| default_catalog |
+-----------------+
2 rows in set
!ok

show current catalog;
+----------------------+
| current catalog name |
+----------------------+
|      default_catalog |
+----------------------+
1 row in set
!ok

use catalog c1;
[INFO] Execute statement succeeded.
!info

show current catalog;
+----------------------+
| current catalog name |
+----------------------+
|                   c1 |
+----------------------+
1 row in set
!ok

drop catalog default_catalog;
[INFO] Execute statement succeeded.
!info

drop catalog c1;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: Cannot drop a catalog which is currently in use.
!error

# ==========================================================================
# test database
# ==========================================================================

create database db1;
[INFO] Execute statement succeeded.
!info

show databases;
+---------------+
| database name |
+---------------+
|           db1 |
|       default |
+---------------+
2 rows in set
!ok

create database db2;
[INFO] Execute statement succeeded.
!info

show databases like 'db%';
+---------------+
| database name |
+---------------+
|           db1 |
|           db2 |
+---------------+
2 rows in set
!ok

show databases ilike 'db%';
+---------------+
| database name |
+---------------+
|           db1 |
|           db2 |
+---------------+
2 rows in set
!ok

show databases like 'db1%';
+---------------+
| database name |
+---------------+
|           db1 |
+---------------+
1 row in set
!ok

show databases ilike 'db1%';
+---------------+
| database name |
+---------------+
|           db1 |
+---------------+
1 row in set
!ok

show databases like 'db2%';
+---------------+
| database name |
+---------------+
|           db2 |
+---------------+
1 row in set
!ok

show databases ilike 'db2%';
+---------------+
| database name |
+---------------+
|           db2 |
+---------------+
1 row in set
!ok

drop database db2;
[INFO] Execute statement succeeded.
!info

show current database;
+-----------------------+
| current database name |
+-----------------------+
|               default |
+-----------------------+
1 row in set
!ok

use db1;
[INFO] Execute statement succeeded.
!info

show current database;
+-----------------------+
| current database name |
+-----------------------+
|                   db1 |
+-----------------------+
1 row in set
!ok

create database db2 comment 'db2_comment' with ('k1' = 'v1');
[INFO] Execute statement succeeded.
!info

alter database db2 set ('k1' = 'a', 'k2' = 'b');
[INFO] Execute statement succeeded.
!info

# TODO: show database properties when we support DESCRIBE DATABSE

show databases;
+---------------+
| database name |
+---------------+
|           db1 |
|           db2 |
|       default |
+---------------+
3 rows in set
!ok

create catalog `c0` with ('type'='generic_in_memory');
[INFO] Execute statement succeeded.
!info

create database c0.db0a;
[INFO] Execute statement succeeded.
!info

create database c0.db0b;
[INFO] Execute statement succeeded.
!info

show databases from c0;
+---------------+
| database name |
+---------------+
|          db0a |
|          db0b |
|       default |
+---------------+
3 rows in set
!ok

show databases from c0 like 'db0a%';
+---------------+
| database name |
+---------------+
|          db0a |
+---------------+
1 row in set
!ok

show databases from c0 ilike 'db0a%';
+---------------+
| database name |
+---------------+
|          db0a |
+---------------+
1 row in set
!ok

show databases from c0 not like 'db0a%';
+---------------+
| database name |
+---------------+
|          db0b |
|       default |
+---------------+
2 rows in set
!ok

show databases from c0 not ilike 'db0a%';
+---------------+
| database name |
+---------------+
|          db0b |
|       default |
+---------------+
2 rows in set
!ok

show databases from c0 like 'db%';
+---------------+
| database name |
+---------------+
|          db0a |
|          db0b |
+---------------+
2 rows in set
!ok

show databases from c0 ilike 'db%';
+---------------+
| database name |
+---------------+
|          db0a |
|          db0b |
+---------------+
2 rows in set
!ok

show databases in c0.t;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.sql.parser.impl.ParseException: Show databases from/in identifier [ c0.t ] format error, catalog must be a single part identifier.
!error

drop catalog `c0`;
[INFO] Execute statement succeeded.
!info

show databases from c0;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Catalog c0 does not exist
!error

drop database if exists db2;
[INFO] Execute statement succeeded.
!info

show databases;
+---------------+
| database name |
+---------------+
|           db1 |
|       default |
+---------------+
2 rows in set
!ok

# ==========================================================================
# test playing with keyword identifiers
# ==========================================================================

create catalog `mod` with ('type'='generic_in_memory');
[INFO] Execute statement succeeded.
!info

use catalog `mod`;
[INFO] Execute statement succeeded.
!info

use `default`;
[INFO] Execute statement succeeded.
!info

drop database `default`;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Cannot drop a database which is currently in use.
!error

drop catalog `mod`;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: Cannot drop a catalog which is currently in use.
!error

use catalog `c1`;
[INFO] Execute statement succeeded.
!info

drop catalog `mod`;
[INFO] Execute statement succeeded.
!info

SET 'execution.runtime-mode' = 'batch';
[INFO] Execute statement succeeded.
!info

SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeeded.
!info

# ==========================================================================
# test create/drop table with catalog
# ==========================================================================

create catalog c2 with ('type'='generic_in_memory');
[INFO] Execute statement succeeded.
!info

use catalog `c2`;
[INFO] Execute statement succeeded.
!info

create table MyTable1 (a int, b string) with ('connector' = 'values');
[INFO] Execute statement succeeded.
!info

create table MyTable2 (a int, b string) with ('connector' = 'values');
[INFO] Execute statement succeeded.
!info

# hive catalog is case-insensitive
show tables;
+------------+
| table name |
+------------+
|   MyTable1 |
|   MyTable2 |
+------------+
2 rows in set
!ok

show views;
Empty set
!ok

create view MyView1 as select 1 + 1;
[INFO] Execute statement succeeded.
!info

create view MyView2 as select 1 + 1;
[INFO] Execute statement succeeded.
!info

show views;
+-----------+
| view name |
+-----------+
|   MyView1 |
|   MyView2 |
+-----------+
2 rows in set
!ok

# test create with full qualified name
create table c1.db1.MyTable3 (a int, b string) with ('connector' = 'values');
[INFO] Execute statement succeeded.
!info

create table c1.db1.MyTable4 (a int, b string) with ('connector' = 'values');
[INFO] Execute statement succeeded.
!info

create view c1.db1.MyView3 as select 1 + 1;
[INFO] Execute statement succeeded.
!info

create view c1.db1.MyView4 as select 1 + 1;
[INFO] Execute statement succeeded.
!info

use catalog c1;
[INFO] Execute statement succeeded.
!info

use db1;
[INFO] Execute statement succeeded.
!info

show tables;
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
[INFO] Execute statement succeeded.
!info

create table `default`.MyTable6 (a int, b string) with ('connector' = 'values');
[INFO] Execute statement succeeded.
!info

create view `default`.MyView5 as select 1 + 1;
[INFO] Execute statement succeeded.
!info

create view `default`.MyView6 as select 1 + 1;
[INFO] Execute statement succeeded.
!info

use `default`;
[INFO] Execute statement succeeded.
!info

show tables;
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
+-----------+
| view name |
+-----------+
|   MyView5 |
|   MyView6 |
+-----------+
2 rows in set
!ok

drop table db1.MyTable3;
[INFO] Execute statement succeeded.
!info

drop view db1.MyView3;
[INFO] Execute statement succeeded.
!info

use db1;
[INFO] Execute statement succeeded.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable4 |
|    MyView4 |
+------------+
2 rows in set
!ok

show views;
+-----------+
| view name |
+-----------+
|   MyView4 |
+-----------+
1 row in set
!ok

drop table c1.`default`.MyTable6;
[INFO] Execute statement succeeded.
!info

drop view c1.`default`.MyView6;
[INFO] Execute statement succeeded.
!info

use `default`;
[INFO] Execute statement succeeded.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable5 |
|    MyView5 |
+------------+
2 rows in set
!ok

show views;
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
[INFO] Execute statement succeeded.
!info

create table MyTable7 (a int, b string) with ('connector' = 'values');
[INFO] Execute statement succeeded.
!info

show tables;
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
[INFO] Execute statement succeeded.
!info

drop table MyTable5;
[INFO] Execute statement succeeded.
!info

show tables;
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
[INFO] Execute statement succeeded.
!info

create database catalog1.db1;
[INFO] Execute statement succeeded.
!info

create table catalog1.db1.person (a int, b string) with ('connector' = 'datagen');
[INFO] Execute statement succeeded.
!info

create table catalog1.db1.dim (a int, b string) with ('connector' = 'datagen');
[INFO] Execute statement succeeded.
!info

create table catalog1.db1.address (a int, b string) with ('connector' = 'datagen');
[INFO] Execute statement succeeded.
!info

create view catalog1.db1.v_person as select * from catalog1.db1.person;
[INFO] Execute statement succeeded.
!info

show tables from catalog1.db1;
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
+------------+
| table name |
+------------+
|     person |
|   v_person |
+------------+
2 rows in set
!ok

show tables in catalog1.db1 not like '%person%';
+------------+
| table name |
+------------+
|    address |
|        dim |
+------------+
2 rows in set
!ok

use catalog catalog1;
[INFO] Execute statement succeeded.
!info

show tables from db1 like 'p_r%';
+------------+
| table name |
+------------+
|     person |
+------------+
1 row in set
!ok

# ==========================================================================
# test catalog
# ==========================================================================

create catalog cat2 WITH ('type'='generic_in_memory', 'default-database'='db');
[INFO] Execute statement succeeded.
!info

show create catalog cat2;
+---------------------------------------------------------------------------------------------+
|                                                                                      result |
+---------------------------------------------------------------------------------------------+
| CREATE CATALOG `cat2` WITH (
  'default-database' = 'db',
  'type' = 'generic_in_memory'
)
 |
+---------------------------------------------------------------------------------------------+
1 row in set
!ok
