# select.q - SELECT
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

CREATE TABLE dummy (
    a INT
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

INSERT INTO dummy VALUES (1);
!output
Job ID:
!info

# ==========================================================================
# test all types
# ==========================================================================

CREATE TABLE hive_types_table (
    f0 BOOLEAN,
    f1 TINYINT,
    f2 SMALLINT,
    f3 INT,
    f4 BIGINT,
    f5 FLOAT,
    f6 DOUBLE,
    f7 DECIMAL(20, 8),
    f8 STRING,
    f9 VARCHAR(20),
    f10 TIMESTAMP,
    f11 DATE,
    f12 BINARY,
    f13 MAP<STRING, STRING>,
    f14 ARRAY<INT>
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

INSERT INTO hive_types_table
SELECT true, 1, 2, 3, 4, 5.0, 6.0, 7.1111, 'Hello World', 'Flink Hive', '2112-12-12 00:00:05.006', '2002-12-13', 'byte', MAP('a', 'b'), ARRAY(1)
FROM dummy LIMIT 1;
!output
Job ID:
!info

SELECT * FROM hive_types_table;
!output
+------+----+----+----+----+-----+-----+--------+-------------+------------+-------------------------+------------+------+-----------+-----+
|   f0 | f1 | f2 | f3 | f4 |  f5 |  f6 |     f7 |          f8 |         f9 |                     f10 |        f11 |  f12 |       f13 | f14 |
+------+----+----+----+----+-----+-----+--------+-------------+------------+-------------------------+------------+------+-----------+-----+
| TRUE |  1 |  2 |  3 |  4 | 5.0 | 6.0 | 7.1111 | Hello World | Flink Hive | 2112-12-12 00:00:05.006 | 2002-12-13 | byte | {"a":"b"} | [1] |
+------+----+----+----+----+-----+-----+--------+-------------+------------+-------------------------+------------+------+-----------+-----+
1 row in set
!ok
