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
[INFO] Table has been created.
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

# ==========================================================================
# test alter table
# ==========================================================================

# test alter table name
alter table orders rename to orders2;
[INFO] Alter table succeeded!
!info

# test alter table properties
alter table orders2 set ('connector' = 'kafka');
[INFO] Alter table succeeded!
!info

# TODO: verify properties using SHOW CREATE TABLE in the future

# test describe table
describe orders2;
+---------+-------------------------+-------+-----------+---------------+----------------------------+
|    name |                    type |  null |       key |        extras |                  watermark |
+---------+-------------------------+-------+-----------+---------------+----------------------------+
|    user |                  BIGINT | false | PRI(user) |               |                            |
| product |             VARCHAR(32) |  true |           |               |                            |
|  amount |                     INT |  true |           |               |                            |
|      ts |  TIMESTAMP(3) *ROWTIME* |  true |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP(3) *PROCTIME* | false |           | AS PROCTIME() |                            |
+---------+-------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
!ok

# ==========================================================================
# test drop table
# ==========================================================================

drop table orders2;
[INFO] Table has been removed.
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
[INFO] Table has been created.
!info

# TODO: warning users the table already exists
create temporary table if not exists tbl1 (
   `user` BIGINT NOT NULl,
   product VARCHAR(32),
   amount INT
) with (
 'connector' = 'datagen'
);
[INFO] Table has been created.
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

drop temporary table tbl1;
[INFO] Table has been removed.
!info

# ==========================================================================
# test playing with keyword identifiers
# ==========================================================================

create table `mod` (`table` string, `database` string);
[INFO] Table has been created.
!info

describe `mod`;
+----------+--------+------+-----+--------+-----------+
|     name |   type | null | key | extras | watermark |
+----------+--------+------+-----+--------+-----------+
|    table | STRING | true |     |        |           |
| database | STRING | true |     |        |           |
+----------+--------+------+-----+--------+-----------+
2 rows in set
!ok

desc `mod`;
+----------+--------+------+-----+--------+-----------+
|     name |   type | null | key | extras | watermark |
+----------+--------+------+-----+--------+-----------+
|    table | STRING | true |     |        |           |
| database | STRING | true |     |        |           |
+----------+--------+------+-----+--------+-----------+
2 rows in set
!ok

drop table `mod`;
[INFO] Table has been removed.
!info

show tables;
Empty set
!ok
