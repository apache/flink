---
title: "INSERT Statements"
weight: 3
type: docs
aliases:
- /dev/table/hiveCompatibility/hiveDialect/create.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# INSERT Statements

## INSERT TABLE

### Description

The `INSERT TABLE` statement is used to insert rows into a table or overwrite the existing data in the table. The row to be inserted
can be specified by value expressions or result from query.

### Syntax

```sql
-- Stardard syntax
INSERT [OVERWRITE] TABLE tablename1
 [PARTITION (partcol1=val1, partcol2=val2 ...) [IF NOT EXISTS]]
   { VALUES ( value [, ..] ) [, ( ... ) ] | select_statement1 FROM from_statement };
   
INSERT INTO TABLE tablename1
 [PARTITION (partcol1=val1, partcol2=val2 ...) [IF NOT EXISTS]]
   { VALUES ( value [, ..] ) [, ( ... ) ] | select_statement1 FROM from_statement };
   
-- Hive extension (multiple inserts):
FROM from_statement
INSERT [OVERWRITE] TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2 ...) [IF NOT EXISTS]] select_statement1,
INSERT [OVERWRITE] TABLE tablename2 [PARTITION ... [IF NOT EXISTS]] select_statement2
[, ... ];

FROM from_statement
INSERT INTO TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2 ...) [IF NOT EXISTS]] select_statement1,
INSERT INTO TABLE tablename2 [PARTITION ... [IF NOT EXISTS]] select_statement2
[, ... ];

-- Hive extension (dynamic partition inserts):
INSERT [OVERWRITE] TABLE tablename PARTITION (partcol1[=val1], partcol2[=val2] ...)
  { VALUES ( value [, ..] ) [, ( ... ) ] | select_statement FROM from_statement };
  
INSERT INTO TABLE tablename PARTITION (partcol1[=val1], partcol2[=val2] ...)
  { VALUES ( value [, ..] ) [, ( ... ) ] | select_statement FROM from_statement };
```

### Parameters

- `OVERWRITE`

  If specify `OVERWRITE`, it will overwrite any existing data in the table or partition.

- `PARTITION ( ... )`

  An option to specify insert data into table's specific partitions.
  If the `PARTITION` clause is specified, the table should be a partitioned table.

- `VALUES ( value [, ..] ) [, ( ... ) ]`

  Specifies the values to be inserted explicitly. A common must be used to separate each value in the clause.
  More than one set of values can be specified to insert multiple rows.

- select_statement

  A statement for query.
  See more details in [queries]({{< ref "docs/dev/table/hiveCompatibility/hiveDialect/Queries/overview" >}}).

### Synopsis 

#### Multiple Inserts

In the Hive extension syntax - multiple inserts, Flink will minimize the number of data scans requires. Flink can insert data into multiple
tables by scanning the input data just once.

#### Dynamic Partition Inserts

In the Hive extension syntax - dynamic partition inserts, users can give partial partition specifications, which means just specifying the list of partition column names in the `PARTITION` clause with optional column values.
If all the partition columns' value are given, we call this a static partition, otherwise it is a dynamic partition.

Each dynamic partition column has a corresponding input column from the select statement. This means that the dynamic partition creation is determined by the value of the input column.

The dynamic partition columns must be specified last among the columns in the `SELECT` statement and in the same order in which they appear in the `PARTITION()` clause.

{{< hint warning >}}
**Note:**

In Hive, by default, the user mush specify at least one static partition in case the user accidentally overwrites all partition, and user can
set the configuration `hive.exec.dynamic.partition.mode` to `nonstrict` to to allow all partitions to be dynamic.

But in Flink's Hive dialect, it'll always be `nonstrict` mode which means all partitions are allowed to be dynamic.
{{< /hint >}}

### Examples

```sql
-- insert into table using values
INSERT INTO t1 VALUES ('k1', 'v1'), ('k2', 'v2');

-- insert overwrite
INSERT OVERWRITE t1 VALUES ('k1', 'v1'), ('k2', 'v2');;

-- insert into table using select statement
INSERT INTO TABLE t1 SELECT * FROM t2;

-- insert into  partition
--- static partition
INSERT INTO t1 PARTITION (year = 2022, month = 12) SELECT value FROM t2;

--- dynamic partition 
INSERT INTO t1 PARTITION (year = 2022, month) SELECT month, value FROM t2;

INSERT INTO t1 PARTITION (year, month) SELECT 2022, month, value FROM t2;

-- multi-insert statements
FROM (SELECT month, value from t1)
    INSERT OVERWRITE TABLE t1_1 SELECT value WHERE month <= 6
    INSERT OVERWRITE TABLE t1_1 SELECT value WHERE month > 6;
```


## INSERT OVERWRITE DIRECTORY

### Description

Query results can be inserted into filesystem directories by using a slight variation of the syntax above:
```sql
-- Standard syntax:
INSERT OVERWRITE [LOCAL] DIRECTORY directory_path
  [ROW FORMAT row_format] [STORED AS file_format] 
  { VALUES ( value [, ..] ) [, ( ... ) ] | select_statement1 FROM from_statement };

-- Hive extension (multiple inserts):
FROM from_statement
INSERT OVERWRITE [LOCAL] DIRECTORY directory1_path select_statement1
[INSERT OVERWRITE [LOCAL] DIRECTORY directory2_path select_statement2] ...

row_format:
  : DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]] [COLLECTION ITEMS TERMINATED BY char]
      [MAP KEYS TERMINATED BY char] [LINES TERMINATED BY char]
      [NULL DEFINED AS char]
  | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, ...)]
```

### Parameters

- directory_path

  The path for the directory to be inserted can be a full URI. If scheme or authority are not specified,
  it'll use the scheme and authority from the hadoop configuration variable `fs.default.name` that specifies the Namenode URI.

- `LOCAL`

   The `LOCAL` keyword is optional. If `LOCAL` keyword is used, Flink will write data to the directory on the local file system.

- `VALUES ( value [, ..] ) [, ( ... ) ]`
   Specifies the values to be inserted explicitly. A common must be used to separate each value in the clause.
   More than one set of values can be specified to insert multiple rows.

- select_statement

  A statement for query. 
  See more details in [queries]({{< ref "docs/dev/table/hiveCompatibility/hiveDialect/Queries/overview" >}}).

- `STORED AS file_format`

   Specifies the file format to use for the insert. The data will be stored as specific file format.
   The valid value are `TEXTFILE`, `ORC`, `PARQUET`,  `AVRO`, `RCFILE`, `SEQUENCEFILE`, `JSONFILE`.
   For more details, please refer to Hive's doc [Storage Formats](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormat,StorageFormat,andSerDe).
   

- row_format

  Specifies the row format for this insert. The data will be serialized to file with the specific property.
  For more details, please refer to Hive's doc [RowFormat](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormat,StorageFormat,andSerDe).

### Synopsis

#### Multiple Inserts

In the Hive extension syntax - multiple inserts, Flink will minimize the number of data scans requires. Flink can insert data into multiple
tables by scanning the input data just once.

### EXAMPLES

```sql
--- insert directory with specific format
INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/t1' STORED AS ORC SELECT * FROM t1;

-- insert directory with specific row format

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/t1'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ':'
  COLLECTION ITEMS TERMINATED BY '#'
  MAP KEYS TERMINATED BY '=' SELECT * FROM t1;
  
-- multiple insert
FROM (SELECT month, value from t1)
    INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/t1/month1' SELECT value WHERE month <= 6
    INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/t1/month2' SELECT value WHERE month > 6;
```
