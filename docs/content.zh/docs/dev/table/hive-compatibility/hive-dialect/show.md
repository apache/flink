---
title: "SHOW Statements"
weight: 5
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

# SHOW Statements

With Hive dialect, the following SHOW statements are supported for now:

- SHOW DATABASES
- SHOW TABLES
- SHOW VIEWS
- SHOW PARTITIONS
- SHOW FUNCTIONS

## SHOW DATABASES

### Description

`SHOW DATABASES` statement is used to list all the databases defined in the metastore.

### Syntax

```sql
SHOW (DATABASES|SCHEMAS);
```
The use of `SCHEMA` and `DATABASE` are interchangeable - they mean the same thing.


## SHOW TABLES

### Description

`SHOW TABLES` statement lists all the base tables and views in the current database.

### Syntax

```sql
SHOW TABLES;
```

## SHOW VIEWS

### Description

`SHOW VIEWS` statement lists all the views in the current database.

### Syntax

```sql
SHOW VIEWS;
```

## SHOW PARTITIONS

### Description

`SHOW PARTITIONS` lists all the existing partitions or the partitions matching the specified partition spec for a given base table.

### Syntax

```sql
SHOW PARTITIONS table_name [ partition_spec ];
partition_spec:
  : (partition_column = partition_col_value, partition_column = partition_col_value, ...)
```

### Parameter

- partition_spec

  The optional `partition_spec` is used to what kind of partition should be returned.
  When specified, the partitions that match the `partition_spec` specification are returned.
  The `partition_spec` can be partial which means you can specific only part of partition columns for listing the partitions.

### Examples

```sql
-- list all partitions
SHOW PARTITIONS t1;

-- specific a full partition partition spec to list specific partition
SHOW PARTITIONS t1 PARTITION (year = 2022, mohth = 12);

-- specific a partial partition spec to list all the specifc partitions
SHOW PARTITIONS t1 PARTITION (year = 2022);
```

## SHOW FUNCTIONS

### Description

`SHOW FUNCTIONS` statement is used to list all the user defined and builtin functions.

### Syntax

```sql
SHOW FUNCTIONS;
```
