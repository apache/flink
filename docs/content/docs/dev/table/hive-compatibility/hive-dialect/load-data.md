---
title: "Load Data Statements"
weight: 4
type: docs
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

# Load Data Statements

## Description

The `LOAD DATA` statement is used to load the data into a Hive table from the user specified directory or file.
The load operation are currently pure copy/move operations that move data files into locations corresponding to Hive tables.

## Syntax

```sql
LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)];
```

## Parameters

- filepath

  The `filepath` can be:
    - a relative path, such as `warehouse/data1`
    - an absolute path, such as `/user/hive/warehouse/data1`
    - a full URL with schema and (optionally) an authority, such as `hdfs://namenode:9000/user/hive/warehouse/data1`

  The `filepath` can refer to a file (in which case, only the single file is loaded) or it can be a directory (in which case, all the files from
  the directory are loaded).

- `LOCAL`

  If specify `LOCAL` keyword, then:
    - it will look for `filepath` in the local file system. If a relative path is specified, it will be interpreted relative to the users' current working directory.
      The user can specify a full URI for local files as well - for example: file:///user/hive/warehouse/data1
    - it will try to **copy** all the files addressed by `filepath` to the target file system.
      The target file system is inferred by looking at the location attribution. The copied data files will then be moved to the location of the table.

  If not, then:
    - if schema or authority are not specified, it'll use the schema and authority from the hadoop configuration variable `fs.default.name` that
      specifies the NameNode URI.
    - if the path is not absolute, then it'll be interpreted relative to /user/<username>
    - It will try to **move** the files addressed by `filepath` into the table (or partition).

- `OVERWRITE`

  By default, the files addressed by `filepath` will be appended to the table (or partition).
  If specific `OVERWRITE`, the original data will be replaced by the files.

- `PARTITION ( ... )`

  An option to specify load data into table's specific partitions. If the `PARTITION` clause is specified, the table should be a partitioned table.

**NOTE:**

For loading data into partition, the partition specifications must be full partition specifications.
Partial partition specification is not supported yet.

## Examples

```sql
-- load data into table
LOAD DATA LOCAL INPATH '/user/warehouse/hive/t1' OVERWRITE INTO TABLE t1;

-- load data into partition
LOAD DATA LOCAL INPATH '/user/warehouse/hive/t1/p1=1' INTO TABLE t1 PARTITION (p1=1);
```
