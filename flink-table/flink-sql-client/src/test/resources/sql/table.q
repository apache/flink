# table.q - CREATE/DROP/SHOW/ALTER/DESCRIBE TABLE
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

create table tbl(a int, b as invalid_function());
[ERROR] Could not execute SQL statement. Reason:
org.apache.calcite.sql.validate.SqlValidatorException: No match found for function signature invalid_function()
!error

drop table non_exist;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Table with identifier 'default_catalog.default_database.non_exist' does not exist.
!error

describe non_exist;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Tables or views with the identifier 'default_catalog.default_database.non_exist' doesn't exist
!error

desc non_exist;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Tables or views with the identifier 'default_catalog.default_database.non_exist' doesn't exist
!error

alter table non_exist rename to non_exist2;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Table `default_catalog`.`default_database`.`non_exist` doesn't exist or is a temporary table.
!error

# ==========================================================================
# test create table
# ==========================================================================

# test create a table with computed column, primary key, watermark
CREATE TABLE IF NOT EXISTS orders (
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

# test SHOW TABLES
show tables;
+------------+
| table name |
+------------+
|     orders |
+------------+
1 row in set
!ok

# test SHOW CREATE TABLE
show create table orders;
CREATE TABLE `default_catalog`.`default_database`.`orders` (
  `user` BIGINT NOT NULL,
  `product` VARCHAR(32),
  `amount` INT,
  `ts` TIMESTAMP(3),
  `ptime` AS PROCTIME(),
  WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND,
  CONSTRAINT `PK_3599338` PRIMARY KEY (`user`) NOT ENFORCED
) WITH (
  'connector' = 'datagen'
)

!ok

# test SHOW COLUMNS
show columns from orders;
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |
|  amount |                         INT |  TRUE |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

show columns in orders;
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |
|  amount |                         INT |  TRUE |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

show columns from orders like '%u';
Empty set
!ok

show columns in orders like '%u';
Empty set
!ok

show columns from orders not like '%u';
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |
|  amount |                         INT |  TRUE |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

show columns in orders not like '%u';
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |
|  amount |                         INT |  TRUE |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

show columns from orders like '%r';
+------+--------+-------+-----------+--------+-----------+
| name |   type |  null |       key | extras | watermark |
+------+--------+-------+-----------+--------+-----------+
| user | BIGINT | FALSE | PRI(user) |        |           |
+------+--------+-------+-----------+--------+-----------+
1 row in set
!ok

show columns in orders like '%r';
+------+--------+-------+-----------+--------+-----------+
| name |   type |  null |       key | extras | watermark |
+------+--------+-------+-----------+--------+-----------+
| user | BIGINT | FALSE | PRI(user) |        |           |
+------+--------+-------+-----------+--------+-----------+
1 row in set
!ok

show columns from orders not like  '%r';
+---------+-----------------------------+-------+-----+---------------+----------------------------+
|    name |                        type |  null | key |        extras |                  watermark |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
| product |                 VARCHAR(32) |  TRUE |     |               |                            |
|  amount |                         INT |  TRUE |     |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
4 rows in set
!ok

show columns in orders not like  '%r';
+---------+-----------------------------+-------+-----+---------------+----------------------------+
|    name |                        type |  null | key |        extras |                  watermark |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
| product |                 VARCHAR(32) |  TRUE |     |               |                            |
|  amount |                         INT |  TRUE |     |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
4 rows in set
!ok

show columns from orders like '%u%';
+---------+-------------+-------+-----------+--------+-----------+
|    name |        type |  null |       key | extras | watermark |
+---------+-------------+-------+-----------+--------+-----------+
|    user |      BIGINT | FALSE | PRI(user) |        |           |
| product | VARCHAR(32) |  TRUE |           |        |           |
|  amount |         INT |  TRUE |           |        |           |
+---------+-------------+-------+-----------+--------+-----------+
3 rows in set
!ok

show columns in orders like '%u%';
+---------+-------------+-------+-----------+--------+-----------+
|    name |        type |  null |       key | extras | watermark |
+---------+-------------+-------+-----------+--------+-----------+
|    user |      BIGINT | FALSE | PRI(user) |        |           |
| product | VARCHAR(32) |  TRUE |           |        |           |
|  amount |         INT |  TRUE |           |        |           |
+---------+-------------+-------+-----------+--------+-----------+
3 rows in set
!ok

show columns from orders not like '%u%';
+-------+-----------------------------+-------+-----+---------------+----------------------------+
|  name |                        type |  null | key |        extras |                  watermark |
+-------+-----------------------------+-------+-----+---------------+----------------------------+
|    ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |               | `ts` - INTERVAL '1' SECOND |
| ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     | AS PROCTIME() |                            |
+-------+-----------------------------+-------+-----+---------------+----------------------------+
2 rows in set
!ok

show columns in orders not like '%u%';
+-------+-----------------------------+-------+-----+---------------+----------------------------+
|  name |                        type |  null | key |        extras |                  watermark |
+-------+-----------------------------+-------+-----+---------------+----------------------------+
|    ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |               | `ts` - INTERVAL '1' SECOND |
| ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     | AS PROCTIME() |                            |
+-------+-----------------------------+-------+-----+---------------+----------------------------+
2 rows in set
!ok

show columns from orders like 'use_';
+------+--------+-------+-----------+--------+-----------+
| name |   type |  null |       key | extras | watermark |
+------+--------+-------+-----------+--------+-----------+
| user | BIGINT | FALSE | PRI(user) |        |           |
+------+--------+-------+-----------+--------+-----------+
1 row in set
!ok

show columns in orders like 'use_';
+------+--------+-------+-----------+--------+-----------+
| name |   type |  null |       key | extras | watermark |
+------+--------+-------+-----------+--------+-----------+
| user | BIGINT | FALSE | PRI(user) |        |           |
+------+--------+-------+-----------+--------+-----------+
1 row in set
!ok

show columns from orders not like 'use_';
+---------+-----------------------------+-------+-----+---------------+----------------------------+
|    name |                        type |  null | key |        extras |                  watermark |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
| product |                 VARCHAR(32) |  TRUE |     |               |                            |
|  amount |                         INT |  TRUE |     |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
4 rows in set
!ok

show columns in orders not like 'use_';
+---------+-----------------------------+-------+-----+---------------+----------------------------+
|    name |                        type |  null | key |        extras |                  watermark |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
| product |                 VARCHAR(32) |  TRUE |     |               |                            |
|  amount |                         INT |  TRUE |     |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |     |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
4 rows in set
!ok

# ==========================================================================
# test alter table rename
# ==========================================================================

alter table orders rename to orders2;
[INFO] Execute statement succeed.
!info

# ==========================================================================
# test alter table set
# ==========================================================================

# test alter table properties
alter table orders2 set ('connector' = 'kafka', 'scan.startup.mode' = 'earliest-offset');
[INFO] Execute statement succeed.
!info

# verify table options using SHOW CREATE TABLE
show create table orders2;
CREATE TABLE `default_catalog`.`default_database`.`orders2` (
  `user` BIGINT NOT NULL,
  `product` VARCHAR(32),
  `amount` INT,
  `ts` TIMESTAMP(3),
  `ptime` AS PROCTIME(),
  WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND,
  CONSTRAINT `PK_3599338` PRIMARY KEY (`user`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'scan.startup.mode' = 'earliest-offset'
)

!ok

# change connector to 'datagen' without removing 'scan.startup.mode' for the fix later
alter table orders2 set ('connector' = 'datagen');
[INFO] Execute statement succeed.
!info

# verify table options are problematic
show create table orders2;
CREATE TABLE `default_catalog`.`default_database`.`orders2` (
  `user` BIGINT NOT NULL,
  `product` VARCHAR(32),
  `amount` INT,
  `ts` TIMESTAMP(3),
  `ptime` AS PROCTIME(),
  WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND,
  CONSTRAINT `PK_3599338` PRIMARY KEY (`user`) NOT ENFORCED
) WITH (
  'connector' = 'datagen',
  'scan.startup.mode' = 'earliest-offset'
)

!ok

# test SHOW CREATE VIEW for tables
show create view orders2;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.TableException: SHOW CREATE VIEW is only supported for views, but `default_catalog`.`default_database`.`orders2` is a table. Please use SHOW CREATE TABLE instead.
!error

# test explain plan to verify the table source cannot be created
explain plan for select * from orders2;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Unsupported options found for 'datagen'.

Unsupported options:

scan.startup.mode

Supported options:

connector
fields.amount.kind
fields.amount.max
fields.amount.min
fields.product.kind
fields.product.length
fields.ts.kind
fields.ts.max-past
fields.user.kind
fields.user.max
fields.user.min
number-of-rows
rows-per-second
!error

# ==========================================================================
# test alter table reset
# ==========================================================================

# test alter table reset to remove invalid key
alter table orders2 reset ('scan.startup.mode');
[INFO] Execute statement succeed.
!info

# verify table options using SHOW CREATE TABLE
show create table orders2;
CREATE TABLE `default_catalog`.`default_database`.`orders2` (
  `user` BIGINT NOT NULL,
  `product` VARCHAR(32),
  `amount` INT,
  `ts` TIMESTAMP(3),
  `ptime` AS PROCTIME(),
  WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND,
  CONSTRAINT `PK_3599338` PRIMARY KEY (`user`) NOT ENFORCED
) WITH (
  'connector' = 'datagen'
)

!ok

# test alter table reset emtpy key
alter table orders2 reset ();
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: ALTER TABLE RESET does not support empty key
!error

# ==========================================================================
# test describe table
# ==========================================================================

describe orders2;
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |
|  amount |                         INT |  TRUE |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

# test desc table
desc orders2;
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |
|  amount |                         INT |  TRUE |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

# ==========================================================================
# test drop table
# ==========================================================================

drop table orders2;
[INFO] Execute statement succeed.
!info

# verify table is dropped
show tables;
Empty set
!ok

# ==========================================================================
# test alter table add table schema components.
# ==========================================================================

# test alter table add watermark
CREATE TABLE `default_catalog`.`default_database`.`t_user` (
  `user` BIGINT NOT NULL,
  `product` VARCHAR(32),
  `amount` INT,
  `ts` TIMESTAMP(3),
  `ptime` AS PROCTIME(),
  CONSTRAINT `PK_3599338` PRIMARY KEY (`user`) NOT ENFORCED
) WITH (
  'connector' = 'datagen'
);
[INFO] Execute statement succeed.
!info

# test alter table add watermark for non_exist table field
alter table t_user add watermark for `non_exist_field` as `non_exist_field` - interval '1' second;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: The rowtime attribute field 'non_exist_field' is not defined in the table schema, at line 1, column 38
Available fields: ['user', 'product', 'amount', 'ts']
!error

# test alter table add watermark for normal case
alter table t_user add watermark for `ts` as `ts` - interval '1' second;
[INFO] Execute statement succeed.
!info

# check the add watermark result
desc t_user;
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |
|  amount |                         INT |  TRUE |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

# test alter table add watermark for duplicated watermark
alter table t_user add watermark for `ts` as `ts` - interval '2' second;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: There already exists a watermark spec for column 'ts' in the base table. You might want to specify EXCLUDING WATERMARKS or OVERWRITING WATERMARKS when you are using 'create table ... like ...' clause.
!error

# test alter table add metadata column
alter table t_user add mc1 timestamp_ltz(3) not null metadata from 'timestamp' virtual comment 'mc_cmt';
[INFO] Execute statement succeed.
!info

alter table t_user add mc2 timestamp_ltz(3)  metadata from 'timestamp' virtual comment 'mc_cmt' first;
[INFO] Execute statement succeed.
!info

alter table t_user add mc3 timestamp_ltz(3) not null metadata from 'timestamp' virtual comment 'mc_cmt' after mc2;
[INFO] Execute statement succeed.
!info

# check alter table add metadata column result
desc t_user;
+---------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
|    name |                        type |  null |       key |                            extras |                  watermark |
+---------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
|     mc2 |            TIMESTAMP_LTZ(3) |  TRUE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|     mc3 |            TIMESTAMP_LTZ(3) | FALSE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|    user |                      BIGINT | FALSE | PRI(user) |                                   |                            |
| product |                 VARCHAR(32) |  TRUE |           |                                   |                            |
|  amount |                         INT |  TRUE |           |                                   |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |                                   | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           |                     AS PROCTIME() |                            |
|     mc1 |            TIMESTAMP_LTZ(3) | FALSE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
+---------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
8 rows in set
!ok

# test for alter table add metadata column error cases
alter table t_user add amount timestamp_ltz(3) not null metadata from 'timestamp' virtual comment 'mc_cmt';
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: A column named 'amount' already exists in the table. Duplicate columns exist in the metadata column and regular column.
!error

alter table t_user add (amount timestamp_ltz(3) not null metadata from 'timestamp' virtual comment 'mc_cmt');
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: A column named 'amount' already exists in the table. Duplicate columns exist in the metadata column and regular column.
!error

# test alter table add computed column
alter table t_user add cost bigint not null;
[INFO] Execute statement succeed.
!info

alter table t_user add cmp1 as (cost - amount) comment 'c';
[INFO] Execute statement succeed.
!info

alter table t_user add cmp2 as (cost - amount) comment 'c' first;
[INFO] Execute statement succeed.
!info

alter table t_user add cmp3 as (cost - 1) comment 'c' after cmp2;
[INFO] Execute statement succeed.
!info

# check alter table add computed column result
desc t_user;
+---------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
|    name |                        type |  null |       key |                            extras |                  watermark |
+---------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
|    cmp2 |                      BIGINT |  TRUE |           |              AS `cost` - `amount` |                            |
|    cmp3 |                      BIGINT | FALSE |           |                     AS `cost` - 1 |                            |
|     mc2 |            TIMESTAMP_LTZ(3) |  TRUE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|     mc3 |            TIMESTAMP_LTZ(3) | FALSE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|    user |                      BIGINT | FALSE | PRI(user) |                                   |                            |
| product |                 VARCHAR(32) |  TRUE |           |                                   |                            |
|  amount |                         INT |  TRUE |           |                                   |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |                                   | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           |                     AS PROCTIME() |                            |
|     mc1 |            TIMESTAMP_LTZ(3) | FALSE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|    cost |                      BIGINT | FALSE |           |                                   |                            |
|    cmp1 |                      BIGINT |  TRUE |           |              AS `cost` - `amount` |                            |
+---------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
12 rows in set
!ok

# test alter table add computed column error cases
alter table t_user add cmp1 as (cost - amount) comment 'c';
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: A generated column named 'cmp1' already exists in the base table. You might want to specify EXCLUDING GENERATED or OVERWRITING GENERATED when you are using 'create table ... like ...' clause.
!error

alter table t_user add (cmp1 as (cost - amount) comment 'c');
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: A generated column named 'cmp1' already exists in the base table. You might want to specify EXCLUDING GENERATED or OVERWRITING GENERATED when you are using 'create table ... like ...' clause.
!error

# computed unknown referenced
alter table t_user add cmp4 as (cost - amount) comment 'c' after non_col;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ColumnPosition$ReferencedColumnNotFoundException: The column 'non_col' referenced by column 'cmp4' was not found.
!error

# test alter table add physical column
alter table t_user add num int not null comment 'num' first;
[INFO] Execute statement succeed.
!info

alter table t_user add cost1 int not null comment 'cost1' after num;
[INFO] Execute statement succeed.
!info

alter table t_user add num1 int not null comment 'num1';
[INFO] Execute statement succeed.
!info

alter table t_user drop constraint PK_3599338;
[INFO] Execute statement succeed.
!info

alter table t_user add physical_constraint_col int not null constraint test_pk primary key not enforced first;
[INFO] Execute statement succeed.
!info

# check alter table add physical column result
desc t_user;
+-------------------------+-----------------------------+-------+------------------------------+-----------------------------------+----------------------------+
|                    name |                        type |  null |                          key |                            extras |                  watermark |
+-------------------------+-----------------------------+-------+------------------------------+-----------------------------------+----------------------------+
| physical_constraint_col |                         INT | FALSE | PRI(physical_constraint_col) |                                   |                            |
|                     num |                         INT | FALSE |                              |                                   |                            |
|                   cost1 |                         INT | FALSE |                              |                                   |                            |
|                    cmp2 |                      BIGINT |  TRUE |                              |              AS `cost` - `amount` |                            |
|                    cmp3 |                      BIGINT | FALSE |                              |                     AS `cost` - 1 |                            |
|                     mc2 |            TIMESTAMP_LTZ(3) |  TRUE |                              | METADATA FROM 'timestamp' VIRTUAL |                            |
|                     mc3 |            TIMESTAMP_LTZ(3) | FALSE |                              | METADATA FROM 'timestamp' VIRTUAL |                            |
|                    user |                      BIGINT | FALSE |                              |                                   |                            |
|                 product |                 VARCHAR(32) |  TRUE |                              |                                   |                            |
|                  amount |                         INT |  TRUE |                              |                                   |                            |
|                      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |                              |                                   | `ts` - INTERVAL '1' SECOND |
|                   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |                              |                     AS PROCTIME() |                            |
|                     mc1 |            TIMESTAMP_LTZ(3) | FALSE |                              | METADATA FROM 'timestamp' VIRTUAL |                            |
|                    cost |                      BIGINT | FALSE |                              |                                   |                            |
|                    cmp1 |                      BIGINT |  TRUE |                              |              AS `cost` - `amount` |                            |
|                    num1 |                         INT | FALSE |                              |                                   |                            |
+-------------------------+-----------------------------+-------+------------------------------+-----------------------------------+----------------------------+
16 rows in set
!ok

# test add physical column duplicated
alter table t_user add num int not null comment 'num1';
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: A column named 'num' already exists in the base table.
!error

# test add physical column reference unknown column
alter table t_user add num3 int not null comment 'num3' after non_col;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ColumnPosition$ReferencedColumnNotFoundException: The column 'non_col' referenced by column 'num3' was not found.
!error

alter table t_user drop constraint test_pk;
[INFO] Execute statement succeed.
!info

# test alter table add column components with parentthese
alter table t_user add (
    num5 int not null comment 'num5' first,
    num6 int not null comment 'num6',
    mc4 timestamp_ltz(3) not null metadata from 'timestamp' virtual comment 'c' first,
    cmp4 as (`user` - 1) comment 'c' after mc4,
    constraint ck1 primary key(num1)
);
[INFO] Execute statement succeed.
!info

# test alter table add column components with parentthese result.
desc t_user;
+-------------------------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
|                    name |                        type |  null |       key |                            extras |                  watermark |
+-------------------------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
|                     mc4 |            TIMESTAMP_LTZ(3) | FALSE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|                    cmp4 |                      BIGINT | FALSE |           |                     AS `user` - 1 |                            |
|                    num5 |                         INT | FALSE |           |                                   |                            |
| physical_constraint_col |                         INT | FALSE |           |                                   |                            |
|                     num |                         INT | FALSE |           |                                   |                            |
|                   cost1 |                         INT | FALSE |           |                                   |                            |
|                    cmp2 |                      BIGINT |  TRUE |           |              AS `cost` - `amount` |                            |
|                    cmp3 |                      BIGINT | FALSE |           |                     AS `cost` - 1 |                            |
|                     mc2 |            TIMESTAMP_LTZ(3) |  TRUE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|                     mc3 |            TIMESTAMP_LTZ(3) | FALSE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|                    user |                      BIGINT | FALSE |           |                                   |                            |
|                 product |                 VARCHAR(32) |  TRUE |           |                                   |                            |
|                  amount |                         INT |  TRUE |           |                                   |                            |
|                      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |                                   | `ts` - INTERVAL '1' SECOND |
|                   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           |                     AS PROCTIME() |                            |
|                     mc1 |            TIMESTAMP_LTZ(3) | FALSE |           | METADATA FROM 'timestamp' VIRTUAL |                            |
|                    cost |                      BIGINT | FALSE |           |                                   |                            |
|                    cmp1 |                      BIGINT |  TRUE |           |              AS `cost` - `amount` |                            |
|                    num1 |                         INT | FALSE | PRI(num1) |                                   |                            |
|                    num6 |                         INT | FALSE |           |                                   |                            |
+-------------------------+-----------------------------+-------+-----------+-----------------------------------+----------------------------+
20 rows in set
!ok

# delete t_user
drop table t_user;
[INFO] Execute statement succeed.
!info

# ==========================================================================
# test temporary table
# ==========================================================================

create temporary table tbl1 (
 `user` BIGINT NOT NULl,
 product VARCHAR(32),
 amount INT
) with (
 'connector' = 'datagen'
);
[INFO] Execute statement succeed.
!info

# TODO: warning users the table already exists
create temporary table if not exists tbl1 (
   `user` BIGINT NOT NULl,
   product VARCHAR(32),
   amount INT
) with (
 'connector' = 'datagen'
);
[INFO] Execute statement succeed.
!info

# list permanent and temporary tables together
show tables;
+------------+
| table name |
+------------+
|       tbl1 |
+------------+
1 row in set
!ok

# SHOW CREATE TABLE for temporary table
show create table tbl1;
CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`tbl1` (
  `user` BIGINT NOT NULL,
  `product` VARCHAR(32),
  `amount` INT
) WITH (
  'connector' = 'datagen'
)

!ok

drop temporary table tbl1;
[INFO] Execute statement succeed.
!info

# ==========================================================================
# test playing with keyword identifiers
# ==========================================================================

create table `mod` (`table` string, `database` string) with ('connector' = 'values');
[INFO] Execute statement succeed.
!info

describe `mod`;
+----------+--------+------+-----+--------+-----------+
|     name |   type | null | key | extras | watermark |
+----------+--------+------+-----+--------+-----------+
|    table | STRING | TRUE |     |        |           |
| database | STRING | TRUE |     |        |           |
+----------+--------+------+-----+--------+-----------+
2 rows in set
!ok

desc `mod`;
+----------+--------+------+-----+--------+-----------+
|     name |   type | null | key | extras | watermark |
+----------+--------+------+-----+--------+-----------+
|    table | STRING | TRUE |     |        |           |
| database | STRING | TRUE |     |        |           |
+----------+--------+------+-----+--------+-----------+
2 rows in set
!ok

drop table `mod`;
[INFO] Execute statement succeed.
!info

show tables;
Empty set
!ok

# ==========================================================================
# test explain
# ==========================================================================

CREATE TABLE IF NOT EXISTS orders (
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

CREATE TABLE IF NOT EXISTS orders2 (
 `user` BIGINT NOT NULl,
 product VARCHAR(32),
 amount INT,
 ts TIMESTAMP(3),
 PRIMARY KEY(`user`) NOT ENFORCED
) with (
 'connector' = 'blackhole'
);
[INFO] Execute statement succeed.
!info

# test explain plan for select
explain plan for select `user`, product from orders;
== Abstract Syntax Tree ==
LogicalProject(user=[$0], product=[$1])
+- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
   +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Optimized Execution Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

!ok

# test explain plan for insert
explain plan for insert into orders2 select `user`, product, amount, ts from orders;
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3])
   +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
      +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
         +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)])
   +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Optimized Execution Plan ==
Sink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

!ok

# test explain select
explain select `user`, product from orders;
== Abstract Syntax Tree ==
LogicalProject(user=[$0], product=[$1])
+- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
   +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Optimized Execution Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

!ok

# test explain insert
explain insert into orders2 select `user`, product, amount, ts from orders;
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3])
   +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
      +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
         +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)])
   +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Optimized Execution Plan ==
Sink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

!ok

# test explain insert with json format
explain json_execution_plan insert into orders2 select `user`, product, amount, ts from orders;
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3])
   +- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
      +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
         +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)])
   +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Optimized Execution Plan ==
Sink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Physical Execution Plan ==
{
  "nodes" : [ {
    "id" : ,
    "type" : "Source: orders[]",
    "pact" : "Data Source",
    "contents" : "[]:TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])",
    "parallelism" : 1
  }, {
    "id" : ,
    "type" : "WatermarkAssigner[]",
    "pact" : "Operator",
    "contents" : "[]:WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : ,
    "type" : "ConstraintEnforcer[]",
    "pact" : "Operator",
    "contents" : "[]:ConstraintEnforcer[NotNullEnforcer(fields=[user])]",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : ,
    "type" : "Sink: orders2[]",
    "pact" : "Data Sink",
    "contents" : "[]:Sink(table=[default_catalog.default_database.orders2], fields=[user, product, amount, ts])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  } ]
}
!ok

# test explain select with json format
explain json_execution_plan select `user`, product from orders;
== Abstract Syntax Tree ==
LogicalProject(user=[$0], product=[$1])
+- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
   +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Optimized Execution Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Physical Execution Plan ==
{
  "nodes" : [ {
    "id" : ,
    "type" : "Source: orders[]",
    "pact" : "Data Source",
    "contents" : "[]:TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])",
    "parallelism" : 1
  }, {
    "id" : ,
    "type" : "Calc[]",
    "pact" : "Operator",
    "contents" : "[]:Calc(select=[user, product, ts])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : ,
    "type" : "WatermarkAssigner[]",
    "pact" : "Operator",
    "contents" : "[]:WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : ,
    "type" : "Calc[]",
    "pact" : "Operator",
    "contents" : "[]:Calc(select=[user, product])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  } ]
}
!ok

# test explain select with ESTIMATED_COST
explain estimated_cost select `user`, product from orders;
== Abstract Syntax Tree ==
LogicalProject(user=[$0], product=[$1])
+- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
   +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[user, product]): rowcount = 1.0E8, cumulative cost = {4.0E8 rows, 2.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)]): rowcount = 1.0E8, cumulative cost = {3.0E8 rows, 2.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}
   +- Calc(select=[user, product, ts]): rowcount = 1.0E8, cumulative cost = {2.0E8 rows, 1.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts]): rowcount = 1.0E8, cumulative cost = {1.0E8 rows, 1.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}

== Optimized Execution Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

!ok

# test explain select with CHANGELOG_MODE
explain changelog_mode select `user`, product from orders;
== Abstract Syntax Tree ==
LogicalProject(user=[$0], product=[$1])
+- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
   +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[user, product], changelogMode=[I])
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)], changelogMode=[I])
   +- Calc(select=[user, product, ts], changelogMode=[I])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts], changelogMode=[I])

== Optimized Execution Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

!ok

# test explain select with all details
explain changelog_mode, estimated_cost, json_execution_plan select `user`, product from orders;
== Abstract Syntax Tree ==
LogicalProject(user=[$0], product=[$1])
+- LogicalWatermarkAssigner(rowtime=[ts], watermark=[-($3, 1000:INTERVAL SECOND)])
   +- LogicalProject(user=[$0], product=[$1], amount=[$2], ts=[$3], ptime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[user, product], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {4.0E8 rows, 2.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}
+- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {3.0E8 rows, 2.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}
   +- Calc(select=[user, product, ts], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {2.0E8 rows, 1.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {1.0E8 rows, 1.0E8 cpu, 3.6E9 io, 0.0 network, 0.0 memory}

== Optimized Execution Plan ==
Calc(select=[user, product])
+- WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])
   +- Calc(select=[user, product, ts])
      +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])

== Physical Execution Plan ==
{
  "nodes" : [ {
    "id" : ,
    "type" : "Source: orders[]",
    "pact" : "Data Source",
    "contents" : "[]:TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[user, product, amount, ts])",
    "parallelism" : 1
  }, {
    "id" : ,
    "type" : "Calc[]",
    "pact" : "Operator",
    "contents" : "[]:Calc(select=[user, product, ts])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : ,
    "type" : "WatermarkAssigner[]",
    "pact" : "Operator",
    "contents" : "[]:WatermarkAssigner(rowtime=[ts], watermark=[(ts - 1000:INTERVAL SECOND)])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : ,
    "type" : "Calc[]",
    "pact" : "Operator",
    "contents" : "[]:Calc(select=[user, product])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : ,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  } ]
}
!ok
