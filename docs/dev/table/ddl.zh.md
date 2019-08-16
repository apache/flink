---
title: "DDL"
nav-parent_id: tableapi
nav-pos: 0
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

The Table API and SQL are integrated in a joint API. The central concept of this API is a `Table` which serves as input and output of queries. This document shows all the DDL grammar Flink support, how to register a `Table`(or view) through DDL, how to drop a `Table`(or view) through DDL.

* This will be replaced by the TOC
{:toc}

Create Table
---------------------------------------
{% highlight sql %}
CREATE [OR REPLACE] TABLE [catalog_name.][db_name.]table_name
  [(col_name1 col_type1 [COMMENT col_comment1], ...)]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name1, col_name2, ...)]
  [WITH (key1=val1, key2=val2, ...)]
{% endhighlight %}

Create a table with the given table properties. If a table with the same name already exists in the database, an exception is thrown.

**PARTITIONED BY**

Partition the created table by the specified columns. A directory is created for each partition if this table is used as a filesystem sink.

**WITH OPTIONS**

Table properties used to create a table source/sink. The properties are usually used to find and create the underlying connector. **Notes:** the key and value of expression `key1=val1` should both be string literal.

See details in [Connect to External Systems](connect.html) for all the supported table properties of different connectors.

**Notes:** The table name can be of two formats: 1. `catalog_name.db_name.table_name` 2. `table_name`. For `catalog_name.db_name.table_name`, the table would be registered into metastore with catalog named "catalog_name" and database named "db_name"; for `table_name`, the table would be registered into the current catalog and database of the execution table environment.

{% top %}

Drop Table
---------------------------------------
{% highlight sql %}
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
{% endhighlight %}

Drop a table with the given table name. If the table to drop does not exist, an exception is thrown.

**IF EXISTS**

If the table does not exist, nothing happens.

{% top %}

DDL Data Types
---------------------------------------
For DDLs, we support full data types defined in page [Data Types]({{ site.baseurl }}/dev/table/types.html).

**Notes:** Some of the data types are not supported in the sql query(the cast expression or literals). E.G. `STRING`, `BYTES`, `TIME(p) WITHOUT TIME ZONE`, `TIME(p) WITH LOCAL TIME ZONE`, `TIMESTAMP(p) WITHOUT TIME ZONE`, `TIMESTAMP(p) WITH LOCAL TIME ZONE`, `ARRAY`, `MULTISET`, `ROW`.

{% top %}
