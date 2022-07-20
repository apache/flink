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

CREATE TABLE IF NOT EXISTS daily_orders (
 `user` BIGINT NOT NULl,
 product STRING,
 amount INT,
 dt STRING NOT NULL,
 PRIMARY KEY(dt, `user`) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
 'connector' = 'filesystem',
 'path' = '$VAR_BATCH_PATH',
 'format' = 'csv'
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

# test explain plan for insert into with static partition
explain plan for insert into daily_orders partition (dt = '2022-06-12') values (123, 'toothpick', 1);
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.daily_orders], fields=[user, product, amount, dt])
+- LogicalProject(user=[123:BIGINT], product=[_UTF-16LE'toothpick':VARCHAR(2147483647) CHARACTER SET "UTF-16LE"], amount=[1], dt=[_UTF-16LE'2022-06-12':VARCHAR(2147483647) CHARACTER SET "UTF-16LE"])
   +- LogicalValues(tuples=[[{ 0 }]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.daily_orders], fields=[user, product, amount, dt])
+- Calc(select=[123:BIGINT AS user, _UTF-16LE'toothpick':VARCHAR(2147483647) CHARACTER SET "UTF-16LE" AS product, 1 AS amount, _UTF-16LE'2022-06-12':VARCHAR(2147483647) CHARACTER SET "UTF-16LE" AS dt])
   +- Values(type=[RecordType(INTEGER ZERO)], tuples=[[{ 0 }]])

== Optimized Execution Plan ==
Sink(table=[default_catalog.default_database.daily_orders], fields=[user, product, amount, dt])
+- Calc(select=[123 AS user, 'toothpick' AS product, 1 AS amount, '2022-06-12' AS dt])
   +- Values(tuples=[[{ 0 }]])

!ok

# test explain plan for insert overwrite with static partition
explain plan for insert into daily_orders partition (dt = '2022-06-12') select `user`, product, amount from daily_orders where dt = '2022-06-12';
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.daily_orders], fields=[user, product, amount, EXPR$3])
+- LogicalProject(user=[$0], product=[$1], amount=[$2], EXPR$3=[_UTF-16LE'2022-06-12':VARCHAR(2147483647) CHARACTER SET "UTF-16LE"])
   +- LogicalFilter(condition=[=($3, _UTF-16LE'2022-06-12')])
      +- LogicalTableScan(table=[[default_catalog, default_database, daily_orders]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.daily_orders], fields=[user, product, amount, EXPR$3])
+- Calc(select=[user, product, amount, _UTF-16LE'2022-06-12':VARCHAR(2147483647) CHARACTER SET "UTF-16LE" AS EXPR$3])
   +- TableSourceScan(table=[[default_catalog, default_database, daily_orders, partitions=[], project=[user, product, amount], metadata=[]]], fields=[user, product, amount])

== Optimized Execution Plan ==
Sink(table=[default_catalog.default_database.daily_orders], fields=[user, product, amount, EXPR$3])
+- Calc(select=[user, product, amount, '2022-06-12' AS EXPR$3])
   +- TableSourceScan(table=[[default_catalog, default_database, daily_orders, partitions=[], project=[user, product, amount], metadata=[]]], fields=[user, product, amount])

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
