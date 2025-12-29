---
title: "User-Defined Functions"
weight: 10
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

# User-Defined Functions

Flink SQL supports user-defined functions (UDFs) to extend the built-in functionality. You can create custom scalar functions, table functions, and aggregate functions to use in your SQL queries.

## Registering UDFs in SQL

Once you have developed a UDF, you can register it in SQL using the `CREATE FUNCTION` statement:

```sql
CREATE FUNCTION myudf AS 'com.example.MyScalarFunction';
```

See the [CREATE FUNCTION]({{< ref "docs/sql/reference/create" >}}#create-function) documentation for the full syntax.

## Developing UDFs

Writing user-defined functions requires Java, Scala, or Python programming. For detailed information on how to develop UDFs, see the Table API documentation:

* [User-Defined Functions Overview]({{< ref "docs/dev/table/functions/udfs" >}}): How to implement and register scalar, table, and aggregate functions.
* [Process Table Functions]({{< ref "docs/dev/table/functions/ptfs" >}}): Advanced table functions for complex event processing.

{{< top >}}
