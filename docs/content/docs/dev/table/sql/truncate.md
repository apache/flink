---
title: "TRUNCATE TABLE Statement"
weight: 8
type: docs
aliases:
- /dev/table/sql/truncate.html
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

# TRUNCATE TABLE Statement

TRUNCATE TABLE statement are used to delete all rows from a table without dropping the table itself.
TRUNCATE TABLE statement is only supported in batch execution mode now.


## Run a TRUNCATE TABLE statement

{{< tabs "truncate" >}}
{{< tab "Java" >}}
TRUNCATE TABLE statement can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns nothing for a successful TRUNCATE TABLE operation, otherwise will throw an exception.

The following examples show how to run a TRUNCATE TABLE statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "SQL CLI" >}}

TRUNCATE TABLE statement can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a TRUNCATE TABLE statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(...);

// register a table named "Orders"
tableEnv.executeSql(
        "CREATE TABLE Orders (" +
        " `user` BIGINT NOT NULl comment 'this is primary key'," +
        " product VARCHAR(32)," +
        " amount INT," +
        " ts TIMESTAMP(3) comment 'notice: watermark'," +
        " ptime AS PROCTIME() comment 'this is a computed column'," +
        " PRIMARY KEY(`user`) NOT ENFORCED," +
        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS" +
        ") with (...)");

// print the schema
tableEnv.executeSql("TRUNCATE TABLE Orders");
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (
>  `user` BIGINT NOT NULl comment 'this is primary key',
>  product VARCHAR(32),
>  amount INT,
>  ts TIMESTAMP(3) comment 'notice: watermark',
>  ptime AS PROCTIME() comment 'this is a computed column',
>  PRIMARY KEY(`user`) NOT ENFORCED,
>  WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS
> ) with (
>  ...
> );
[INFO] Table has been created.

Flink SQL> TRUNCATE TABLE Orders;
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Syntax

```sql
TRUNCATE TABLE [catalog_name.][db_name.]table_name
```
