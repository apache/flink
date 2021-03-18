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

!error

create database my.db;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Catalog my does not exist
!error

# ==========================================================================
# test catalog
# ==========================================================================

create catalog c1 with ('type'='generic_in_memory');
[INFO] Catalog has been created.
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
[INFO] Catalog changed.
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
[INFO] Catalog has been removed.
!info

# ==========================================================================
# test database
# ==========================================================================

create database db1;
[INFO] Database has been created.
!info

show databases;
+---------------+
| database name |
+---------------+
|       default |
|           db1 |
+---------------+
2 rows in set
!ok

show current database;
+-----------------------+
| current database name |
+-----------------------+
|               default |
+-----------------------+
1 row in set
!ok

use db1;
[INFO] Database changed.
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
[INFO] Database has been created.
!info

alter database db2 set ('k1' = 'a', 'k2' = 'b');
[INFO] Alter database succeeded!
!info

# TODO: show database properties when we support DESCRIBE DATABSE

show databases;
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
[INFO] Database has been removed.
!info

show databases;
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
[INFO] Catalog has been created.
!info

use catalog `mod`;
[INFO] Catalog changed.
!info

use `default`;
[INFO] Database changed.
!info

drop database `default`;
[INFO] Database has been removed.
!info

drop catalog `mod`;
[INFO] Catalog has been removed.
!info

# ==========================================================================
# test hive catalog
# ==========================================================================

create catalog hivecatalog with (
 'type' = 'hive',
 'test' = 'test',  -- this makes sure we use TestHiveCatalogFactory
 'hive-version' = '2.3.4'
);
[INFO] Catalog has been created.
!info

use catalog hivecatalog;
[INFO] Catalog changed.
!info

show current catalog;
+----------------------+
| current catalog name |
+----------------------+
|          hivecatalog |
+----------------------+
1 row in set
!ok

show databases;
+--------------------------+
|            database name |
+--------------------------+
| additional_test_database |
|                  default |
+--------------------------+
2 rows in set
!ok

show tables;
+-------------------+
|        table name |
+-------------------+
| param_types_table |
+-------------------+
1 row in set
!ok

use additional_test_database;
[INFO] Database changed.
!info

show tables;
+------------+
| table name |
+------------+
| test_table |
+------------+
1 row in set
!ok

show current database;
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
+------+-----------------+------+-----+--------+-----------+
| name |            type | null | key | extras | watermark |
+------+-----------------+------+-----+--------+-----------+
|  dec | DECIMAL(10, 10) | true |     |        |           |
|   ch |         CHAR(5) | true |     |        |           |
|  vch |     VARCHAR(15) | true |     |        |           |
+------+-----------------+------+-----+--------+-----------+
3 rows in set
!ok

SET execution.runtime-mode = batch;
[INFO] Session property has been set.
!info

SET sql-client.execution.result-mode = tableau;
[INFO] Session property has been set.
!info

# test the SELECT query can run successfully, even result is empty
select * from hivecatalog.`default`.param_types_table;
+--------------+----------------------+----------------------+
|          dec |                   ch |                  vch |
+--------------+----------------------+----------------------+
Received a total of 0 row
!ok

# ==========================================================================
# test create/drop table with catalog
# ==========================================================================

use catalog hivecatalog;
[INFO] Catalog changed.
!info

create table MyTable1 (a int, b string);
[INFO] Table has been created.
!info

create table MyTable2 (a int, b string);
[INFO] Table has been created.
!info

# hive catalog is case-insensitive
show tables;
+------------+
| table name |
+------------+
|   mytable1 |
|   mytable2 |
| test_table |
+------------+
3 rows in set
!ok

# test create with full qualified name
create table c1.db1.MyTable3 (a int, b string);
[INFO] Table has been created.
!info

create table c1.db1.MyTable4 (a int, b string);
[INFO] Table has been created.
!info

use catalog c1;
[INFO] Catalog changed.
!info

use db1;
[INFO] Database changed.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable3 |
|   MyTable4 |
+------------+
2 rows in set
!ok

# test create with database name
create table `default`.MyTable5 (a int, b string);
[INFO] Table has been created.
!info

create table `default`.MyTable6 (a int, b string);
[INFO] Table has been created.
!info

use `default`;
[INFO] Database changed.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable5 |
|   MyTable6 |
+------------+
2 rows in set
!ok

drop table db1.MyTable3;
[INFO] Table has been removed.
!info

use db1;
[INFO] Database changed.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable4 |
+------------+
1 row in set
!ok

drop table c1.`default`.MyTable6;
[INFO] Table has been removed.
!info

use `default`;
[INFO] Database changed.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable5 |
+------------+
1 row in set
!ok

# ==========================================================================
# test configuration is changed (trigger new ExecutionContext)
# ==========================================================================

SET sql-client.execution.result-mode = changelog;
[INFO] Session property has been set.
!info

create table MyTable7 (a int, b string);
[INFO] Table has been created.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable5 |
|   MyTable7 |
+------------+
2 rows in set
!ok

reset;
[INFO] All session properties have been set to their default values.
!info

drop table MyTable5;
[INFO] Table has been removed.
!info

show tables;
+------------+
| table name |
+------------+
|   MyTable7 |
+------------+
1 row in set
!ok
