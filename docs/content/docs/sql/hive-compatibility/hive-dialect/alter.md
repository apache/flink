---
title: "ALTER Statements"
weight: 3
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

# ALTER Statements

With Hive dialect, the following ALTER statements are supported for now:

- ALTER DATABASE
- ALTER TABLE
- ALTER VIEW

## ALTER DATABASE

### Description

`ALTER DATABASE` statement is used to change the properties or location of a database.

### Syntax

```sql
-- alter database's properties
ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...);

-- alter database's localtion
ALTER (DATABASE|SCHEMA) database_name SET LOCATION hdfs_path;
```

### Synopsis

- The uses of `SCHEMA` and `DATABASE` are interchangeable - they mean the same thing.
- The `ALTER DATABASE .. SET LOCATION` statement is only supported in Hive-2.4.0 or later. The statement doesn't move the contents of the database's current directory to the newly specified location.
  It does not change the locations associated with any tables/partitions under the specified database.
  It only changes the default parent-directory where new tables will be added for this database.
  This behaviour is analogous to how changing a table-directory does not move existing partitions to a different location.

### Examples

```sql
-- alter database's properties
ALTER DATABASE d1 SET DBPROPERTIES ('p1' = 'v1', 'p2' = 'v2');

-- alter database's location
ALTER DATABASE d1 SET LOCATION '/new/path';
```

## ALTER TABLE

### Description

`ALTER TABLE` statement changes the schema or properties of a table.

### Rename Table

#### Description

The `RENAME TABLE` statement allows user to change the name of a table to a different name.

#### Syntax

```sql
ALTER TABLE table_name RENAME TO new_table_name;
```

#### Examples

```sql
ALTER TABLE t1 RENAME TO t2;
```

### Alter Table Properties

#### Description

The `ALTER TABLE PROPERTIES` statement allows user add own metadata to tables. Currently, last_modified_user, last_modified_time properties are automatically added and managed by Hive.

#### Syntax

```sql
ALTER TABLE table_name SET TBLPROPERTIES table_properties;
 
table_properties:
  : (property_name = property_value, property_name = property_value, ... )
```

#### Examples

```sql
ALTER TABLE t1 SET TBLPROPERTIES ('p1' = 'v1', 'p2' = 'v2');
```

### Add / Remove SerDe Properties

#### Description

The statement enable user to change a table's SerDe or add/move user-defined metadata to the table's SerDe Object.
The SerDe properties are passed to the table's SerDe to serialize and deserialize data. So users can store any information required for their custom SerDe here.
Refer to the Hive's [SerDe docs](https://cwiki.apache.org/confluence/display/Hive/SerDe) and [Hive SerDe](https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide#DeveloperGuide-HiveSerDe) for more details.

#### Syntax

Add SerDe Properties:
```sql
ALTER TABLE table_name [PARTITION partition_spec] SET SERDE serde_class_name [WITH SERDEPROPERTIES serde_properties];
 
ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;
 
serde_properties:
  : (property_name = property_value, property_name = property_value, ... )
```

Remove SerDe Properties:
```sql
ALTER TABLE table_name [PARTITION partition_spec] UNSET SERDEPROPERTIES (property_name, ... );
```

#### Examples

```sql
-- add serde properties
ALTER TABLE t1 SET SERDEPROPERTIES ('field.delim' = ',');

-- remove serde properties
ALTER TABLE t1 UNSET SERDEPROPERTIES ('field.delim');
```

### Alter Partition

`ALTER TABLE ... PARTITION ..` statement is used to add/rename/drop partitions.

#### Add Partitions

`ALTER TABLE .. ADD PARTITION` statement is used to add partitions.
Partition values should be quoted only if they are strings.
The location must be a directory inside which data files reside. (ADD PARTITION changes the table metadata, but does not load data. If the data does not exist in the partition's location, queries will not return any results.)
An error is thrown if the partition_spec for the table already exists. You can use IF NOT EXISTS to skip the error.

##### Syntax

```sql
ALTER TABLE table_name ADD [IF NOT EXISTS]
 PARTITION partition_spec [LOCATION 'location']
 [, PARTITION partition_spec [LOCATION 'location'], ...];
partition_spec:
  : (partition_column = partition_col_value, partition_column = partition_col_value, ...)
```

##### Examples

```sql
ALTER TABLE t1 ADD PARTITION (dt='2022-08-08', country='china') location '/path/to/us/part080808'
                   PARTITION (dt='2022-08-09', country='china') location '/path/to/us/part080809';
```

#### Rename Partitions

`ALTER TABLE .. PARTITION ... RENAME TO ...` statement is used to rename partition.

##### Syntax

```sql
ALTER TABLE table_name PARTITION partition_spec RENAME TO PARTITION partition_spec;
```

##### Examples

```sql
ALTER TABLE t1 PARTITION (dt='2022-08-08', country='china')
     RENAME TO PARTITION (dt='2023-08-08', country='china');
```

#### Drop Partitions

`ALTER TABLE .. DROP PARTITION ...` statement is used to drop partition.
This removes the data and metadata for this partition. The data is actually moved to the `.Trash/Current` directory if Trash is configured, but the
metadata is completed lost.

##### Syntax

```sql
ALTER TABLE table_name DROP [IF EXISTS] PARTITION partition_spec[, PARTITION partition_spec, ...]
```

##### Examples

```sql
ALTER TABLE t1 DROP IF EXISTS PARTITION (dt='2022-08-08', country='china');
```

#### Alter Location / File Format

`ALTER TABLE SET` command can also be used for changing the file location and file format for existing tables.

##### Syntax

```sql 
--- Alter File Location
ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION "new location";

--- Alter File Format
ALTER TABLE table_name [PARTITION partition_spec] SET FILEFORMAT file_format;
```

##### Examples

```sql
-- alter file localtion
ALTER TABLE t1 PARTITION (dt='2022-08-08', country='china') SET LOCATION "/user/warehouse/t2/dt=2022-08-08/country=china";

-- alter file format
ALTER TABLE t1 PARTITION (dt='2022-08-08', country='china') SET FILEFORMAT ORC;
```

### Alter Column

#### Rules for Column Names

Column names are case-insensitive. Backtick quotation enables the use of reserved keywords for column names, as well as table names.

#### Change Column's Definition

The statement allow users to change a column's name, data type, comment, or position, or an arbitrary combination of them.

##### Syntax

```sql
ALTER TABLE table_name [PARTITION partition_spec] CHANGE [COLUMN] col_old_name col_new_name column_type
  [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT];
```

##### Examples

```sql
ALTER TABLE t1 CHANGE COLUMN c1 new_c1 STRING FIRST;
ALTER TABLE t1 CHANGE COLUMN c1 new_c1 STRING AFRER c2;
```

#### Add/Replace Columns

The statement allow users to add new columns or replace the existing columns with the new columns.

##### Syntax

```sql
ALTER TABLE table_name 
  [PARTITION partition_spec]                
  ADD|REPLACE COLUMNS (col_name data_type [COMMENT col_comment], ...)
  [CASCADE|RESTRICT]
```

`ADD COLUMNS` will add new columns to the end of the existing columns before the partition columns.

`REPLACE COLUMNS` will remove all existing columns and add the new set of columns.

##### Synopsis

`ALTER TABLE ... COLUMNS` with `CASCADE` command changes the columns of a table's metadata,
and cascades the same change to all the partition metadata.
`RESTRICT` is the default, limiting column changes only to table metadata.

##### Examples

```sql
-- add column
ALTER TABLE t1 ADD COLUMNS (ch CHAR(5), name STRING) CASCADE;

-- replace column
ALTER TABLE t1 REPLACE COLUMNS (t1 TINYINT, d DECIMAL) CASCADE;
```

## ALTER VIEW

### Alter View Properties

`ALTER VIEW ... SET TBLPROPERTIES ..` allow user to add own metadata to a view.

#### Syntax

```sql
ALTER VIEW [db_name.]view_name SET TBLPROPERTIES table_properties;
 
table_properties:
  : (property_name = property_value, property_name = property_value, ...)
```

#### Examples

```sql
ALTER VIEW v1 SET TBLPROPERTIES ('p1' = 'v1');
```

### Alter View As Select

`ALTER VIEW ... AS ..` allow user to change the definition of a view, which must exist.

#### Syntax

```sql
ALTER VIEW [db_name.]view_name AS select_statement;
```

#### Examples

```sql
ALTER VIEW v1 AS SELECT * FROM t2;
```
