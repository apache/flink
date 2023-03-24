# statement-set.q - BEGIN STATEMENT SET, END
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

SET 'table.dml-sync' = 'true';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

create table src (
  id int,
  str string
) with (
  'connector' = 'values'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# ==========================================================================
# test statement set with streaming insert
# ==========================================================================

SET 'execution.runtime-mode' = 'streaming';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

create table StreamingTable (
  id int,
  str string
) with (
  'connector' = 'filesystem',
  'path' = '$VAR_STREAMING_PATH',
  'format' = 'csv'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok


create table StreamingTable2 (
  id int,
  str string
) with (
  'connector' = 'filesystem',
  'path' = '$VAR_STREAMING_PATH2',
  'format' = 'csv'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

EXPLAIN STATEMENT SET BEGIN
INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
END;
!output
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.StreamingTable], fields=[EXPR$0, EXPR$1])
+- LogicalProject(EXPR$0=[$0], EXPR$1=[$1])
   +- LogicalValues(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]])

LogicalSink(table=[default_catalog.default_database.StreamingTable], fields=[EXPR$0, EXPR$1])
+- LogicalProject(EXPR$0=[$0], EXPR$1=[$1])
   +- LogicalValues(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.StreamingTable], fields=[EXPR$0, EXPR$1])
+- Values(type=[RecordType(INTEGER EXPR$0, VARCHAR(11) EXPR$1)], tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]])

Sink(table=[default_catalog.default_database.StreamingTable], fields=[EXPR$0, EXPR$1])
+- Values(type=[RecordType(INTEGER EXPR$0, VARCHAR(11) EXPR$1)], tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]])

== Optimized Execution Plan ==
Values(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]])(reuse_id=[1])

Sink(table=[default_catalog.default_database.StreamingTable], fields=[EXPR$0, EXPR$1])
+- Reused(reference_id=[1])

Sink(table=[default_catalog.default_database.StreamingTable], fields=[EXPR$0, EXPR$1])
+- Reused(reference_id=[1])
!ok

EXECUTE STATEMENT SET BEGIN
INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
INSERT INTO StreamingTable2 SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
END;
!output
Job ID:
!info

SELECT * FROM StreamingTable;
!output
+----+----+-------------+
| op | id |         str |
+----+----+-------------+
| +I |  1 | Hello World |
| +I |  2 |          Hi |
| +I |  2 |          Hi |
| +I |  3 |       Hello |
| +I |  3 |       World |
| +I |  4 |         ADD |
| +I |  5 |        LINE |
+----+----+-------------+
7 rows in set
!ok

SELECT * FROM StreamingTable2;
!output
+----+----+-------------+
| op | id |         str |
+----+----+-------------+
| +I |  1 | Hello World |
| +I |  2 |          Hi |
| +I |  2 |          Hi |
| +I |  3 |       Hello |
| +I |  3 |       World |
| +I |  4 |         ADD |
| +I |  5 |        LINE |
+----+----+-------------+
7 rows in set
!ok

EXPLAIN STATEMENT SET BEGIN
END;
!output
org.apache.flink.sql.parser.impl.ParseException: Encountered "END" at line 2, column 1.
Was expecting one of:
    "INSERT" ...
    "UPSERT" ...
!error

EXECUTE STATEMENT SET BEGIN
END;
!output
org.apache.flink.sql.parser.impl.ParseException: Encountered "END" at line 2, column 1.
Was expecting one of:
    "INSERT" ...
    "UPSERT" ...
!error

# ==========================================================================
# test statement set with batch inserts
# ==========================================================================

SET 'execution.runtime-mode' = 'batch';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

create table BatchTable (
id int,
str string
) with (
'connector' = 'filesystem',
'path' = '$VAR_BATCH_PATH',
'format' = 'csv'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

create table BatchTable2 (
id int,
str string
) with (
'connector' = 'filesystem',
'path' = '$VAR_BATCH_PATH2',
'format' = 'csv'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

EXPLAIN STATEMENT SET
BEGIN
INSERT INTO BatchTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
INSERT INTO BatchTable2 SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
END;
!output
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.BatchTable], fields=[EXPR$0, EXPR$1])
+- LogicalProject(EXPR$0=[$0], EXPR$1=[$1])
   +- LogicalValues(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]])

LogicalSink(table=[default_catalog.default_database.BatchTable2], fields=[EXPR$0, EXPR$1])
+- LogicalProject(EXPR$0=[$0], EXPR$1=[$1])
   +- LogicalValues(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.BatchTable], fields=[EXPR$0, EXPR$1])
+- Values(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]], values=[EXPR$0, EXPR$1])

Sink(table=[default_catalog.default_database.BatchTable2], fields=[EXPR$0, EXPR$1])
+- Values(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]], values=[EXPR$0, EXPR$1])

== Optimized Execution Plan ==
Values(tuples=[[{ 1, _UTF-16LE'Hello World' }, { 2, _UTF-16LE'Hi' }, { 2, _UTF-16LE'Hi' }, { 3, _UTF-16LE'Hello' }, { 3, _UTF-16LE'World' }, { 4, _UTF-16LE'ADD' }, { 5, _UTF-16LE'LINE' }]], values=[EXPR$0, EXPR$1])(reuse_id=[1])

Sink(table=[default_catalog.default_database.BatchTable], fields=[EXPR$0, EXPR$1])
+- Reused(reference_id=[1])

Sink(table=[default_catalog.default_database.BatchTable2], fields=[EXPR$0, EXPR$1])
+- Reused(reference_id=[1])
!ok

EXECUTE STATEMENT SET
BEGIN
INSERT INTO BatchTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
INSERT INTO BatchTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
END;
!output
Job ID:
!info

SELECT * FROM BatchTable;
!output
+----+-------------+
| id |         str |
+----+-------------+
|  1 | Hello World |
|  2 |          Hi |
|  2 |          Hi |
|  3 |       Hello |
|  3 |       World |
|  4 |         ADD |
|  5 |        LINE |
|  1 | Hello World |
|  2 |          Hi |
|  2 |          Hi |
|  3 |       Hello |
|  3 |       World |
|  4 |         ADD |
|  5 |        LINE |
+----+-------------+
14 rows in set
!ok

SELECT * FROM BatchTable2;
!output
Empty set
!ok

EXPLAIN STATEMENT SET BEGIN
END;
!output
org.apache.flink.sql.parser.impl.ParseException: Encountered "END" at line 2, column 1.
Was expecting one of:
    "INSERT" ...
    "UPSERT" ...
!error

EXECUTE STATEMENT SET BEGIN
END;
!output
org.apache.flink.sql.parser.impl.ParseException: Encountered "END" at line 2, column 1.
Was expecting one of:
    "INSERT" ...
    "UPSERT" ...
!error
