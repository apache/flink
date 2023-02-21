# view.q - CREATE/DROP/SHOW/DESCRIBE VIEW
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

# register a base table first
CREATE TABLE orders (
 `user` BIGINT NOT NULl,
 product VARCHAR(32),
 amount INT,
 ts TIMESTAMP(3),
 ptime AS PROCTIME(),
 PRIMARY KEY(`user`) NOT ENFORCED,
 WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS
) with (
 'connector' = 'datagen'
);
[INFO] Execute statement succeed.
!info

# ==== test temporary view =====

create temporary view v1 as select * from orders;
[INFO] Execute statement succeed.
!info

create temporary view v1 as select * from orders;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Temporary table '`default_catalog`.`default_database`.`v1`' already exists
!error

# TODO: warning users the view already exists
create temporary view if not exists v1 as select * from orders;
[INFO] Execute statement succeed.
!info

# test query a view with hint
select * from v1 /*+ OPTIONS('number-of-rows' = '1') */;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: View '`default_catalog`.`default_database`.`v1`' cannot be enriched with new options. Hints can only be applied to tables.
!error

# test create a view reference another view
create temporary view if not exists v2 as select * from v1;
[INFO] Execute statement succeed.
!info

# test show create a temporary view
show create view v1;
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                     result |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`v1`(`user`, `product`, `amount`, `ts`, `ptime`) as
SELECT *
FROM `default_catalog`.`default_database`.`orders` |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set
!ok

# test show create a temporary view reference another view
show create view v2;
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                 result |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`v2`(`user`, `product`, `amount`, `ts`, `ptime`) as
SELECT *
FROM `default_catalog`.`default_database`.`v1` |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set
!ok

show tables;
+------------+
| table name |
+------------+
|     orders |
|         v1 |
|         v2 |
+------------+
3 rows in set
!ok

show views;
+-----------+
| view name |
+-----------+
|        v1 |
|        v2 |
+-----------+
2 rows in set
!ok

# test SHOW CREATE TABLE for views
show create table v1;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.TableException: SHOW CREATE TABLE is only supported for tables, but `default_catalog`.`default_database`.`v1` is a view. Please use SHOW CREATE VIEW instead.
!error

# ==== test permanent view =====

# register a permanent view with the duplicate name with temporary view
create view v1 as select * from orders;
[INFO] Execute statement succeed.
!info

# test create duplicate view
create view v1 as select * from orders;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.TableAlreadyExistException: Table (or view) default_database.v1 already exists in Catalog default_catalog.
!error

# test show create a permanent view
create view permanent_v1 as select * from orders;
[INFO] Execute statement succeed.
!info

# test show create a permanent view
show create view permanent_v1;
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                     result |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CREATE VIEW `default_catalog`.`default_database`.`permanent_v1`(`user`, `product`, `amount`, `ts`, `ptime`) as
SELECT *
FROM `default_catalog`.`default_database`.`orders` |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set
!ok

# remove permanent_v1 view
drop view permanent_v1;
[INFO] Execute statement succeed.
!info

# we didn't distinguish the temporary v1 and permanent v1 for now
show views;
+-----------+
| view name |
+-----------+
|        v1 |
|        v2 |
+-----------+
2 rows in set
!ok

# test describe view
describe v1;
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
|    user |                      BIGINT | FALSE |     |        |           |
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
5 rows in set
!ok

# test SHOW COLUMNS
show columns from v1;
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
|    user |                      BIGINT | FALSE |     |        |           |
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
5 rows in set
!ok

show columns in v1;
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
|    user |                      BIGINT | FALSE |     |        |           |
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
5 rows in set
!ok

show columns from v1 like '%u';
Empty set
!ok

show columns in v1 like '%u';
Empty set
!ok

show columns from v1 not like '%u';
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
|    user |                      BIGINT | FALSE |     |        |           |
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
5 rows in set
!ok

show columns in v1 not like '%u';
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
|    user |                      BIGINT | FALSE |     |        |           |
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
5 rows in set
!ok

show columns from v1 like '%r';
+------+--------+-------+-----+--------+-----------+
| name |   type |  null | key | extras | watermark |
+------+--------+-------+-----+--------+-----------+
| user | BIGINT | FALSE |     |        |           |
+------+--------+-------+-----+--------+-----------+
1 row in set
!ok

show columns in v1 like '%r';
+------+--------+-------+-----+--------+-----------+
| name |   type |  null | key | extras | watermark |
+------+--------+-------+-----+--------+-----------+
| user | BIGINT | FALSE |     |        |           |
+------+--------+-------+-----+--------+-----------+
1 row in set
!ok

show columns from v1 not like  '%r';
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
4 rows in set
!ok

show columns in v1 not like  '%r';
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
4 rows in set
!ok

show columns from v1 like '%u%';
+---------+-------------+-------+-----+--------+-----------+
|    name |        type |  null | key | extras | watermark |
+---------+-------------+-------+-----+--------+-----------+
|    user |      BIGINT | FALSE |     |        |           |
| product | VARCHAR(32) |  TRUE |     |        |           |
|  amount |         INT |  TRUE |     |        |           |
+---------+-------------+-------+-----+--------+-----------+
3 rows in set
!ok

show columns in v1 like '%u%';
+---------+-------------+-------+-----+--------+-----------+
|    name |        type |  null | key | extras | watermark |
+---------+-------------+-------+-----+--------+-----------+
|    user |      BIGINT | FALSE |     |        |           |
| product | VARCHAR(32) |  TRUE |     |        |           |
|  amount |         INT |  TRUE |     |        |           |
+---------+-------------+-------+-----+--------+-----------+
3 rows in set
!ok

show columns from v1 not like '%u%';
+-------+-----------------------------+-------+-----+--------+-----------+
|  name |                        type |  null | key | extras | watermark |
+-------+-----------------------------+-------+-----+--------+-----------+
|    ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
| ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+-------+-----------------------------+-------+-----+--------+-----------+
2 rows in set
!ok

show columns in v1 not like '%u%';
+-------+-----------------------------+-------+-----+--------+-----------+
|  name |                        type |  null | key | extras | watermark |
+-------+-----------------------------+-------+-----+--------+-----------+
|    ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
| ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+-------+-----------------------------+-------+-----+--------+-----------+
2 rows in set
!ok

show columns from v1 like 'use_';
+------+--------+-------+-----+--------+-----------+
| name |   type |  null | key | extras | watermark |
+------+--------+-------+-----+--------+-----------+
| user | BIGINT | FALSE |     |        |           |
+------+--------+-------+-----+--------+-----------+
1 row in set
!ok

show columns in v1 like 'use_';
+------+--------+-------+-----+--------+-----------+
| name |   type |  null | key | extras | watermark |
+------+--------+-------+-----+--------+-----------+
| user | BIGINT | FALSE |     |        |           |
+------+--------+-------+-----+--------+-----------+
1 row in set
!ok

show columns from v1 not like 'use_';
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
4 rows in set
!ok

show columns in v1 not like 'use_';
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
4 rows in set
!ok

# we can't drop permanent view if there is temporary view with the same name
drop view v1;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Temporary view with identifier '`default_catalog`.`default_database`.`v1`' exists. Drop it first before removing the permanent view.
!error

# although temporary v2 needs temporary v1, dropping v1 first does not throw exception
drop temporary view v1;
[INFO] Execute statement succeed.
!info

# now we can drop permanent view v1
drop view v1;
[INFO] Execute statement succeed.
!info

# test drop invalid table
drop view non_exist;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: View with identifier 'default_catalog.default_database.non_exist' does not exist.
!error

# ===== test playing with keyword identifiers =====

create view `mod` as select * from orders;
[INFO] Execute statement succeed.
!info

describe `mod`;
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
|    user |                      BIGINT | FALSE |     |        |           |
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
5 rows in set
!ok

desc `mod`;
+---------+-----------------------------+-------+-----+--------+-----------+
|    name |                        type |  null | key | extras | watermark |
+---------+-----------------------------+-------+-----+--------+-----------+
|    user |                      BIGINT | FALSE |     |        |           |
| product |                 VARCHAR(32) |  TRUE |     |        |           |
|  amount |                         INT |  TRUE |     |        |           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |        |           |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |        |           |
+---------+-----------------------------+-------+-----+--------+-----------+
5 rows in set
!ok

drop view `mod`;
[INFO] Execute statement succeed.
!info

show tables;
+------------+
| table name |
+------------+
|     orders |
|         v2 |
+------------+
2 rows in set
!ok
