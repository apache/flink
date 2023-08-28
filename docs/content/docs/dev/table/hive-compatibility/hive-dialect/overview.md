---
title: "Overview"
weight: 1
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

# Hive Dialect

Flink allows users to write SQL statements in Hive syntax when Hive dialect is used.
By providing compatibility with Hive syntax, we aim to improve the interoperability with Hive and reduce the scenarios when users need to switch between Flink and Hive in order to execute different statements.

## Use Hive Dialect

Flink currently supports two SQL dialects: `default` and `hive`. You need to switch to Hive dialect
before you can write in Hive syntax. The following describes how to set dialect using
SQL Client, SQL Gateway configured with HiveServer2 Endpoint and Table API. Also notice that you can dynamically switch dialect for each
statement you execute. There's no need to restart a session to use a different dialect.

{{< hint warning >}}
**Note:**

- To use Hive dialect, you have to add dependencies related to Hive. Please refer to [Hive dependencies]({{< ref "docs/connectors/table/hive/overview" >}}#dependencies) for how to add the dependencies.
- Please make sure the current catalog is [HiveCatalog]({{< ref "docs/connectors/table/hive/hive_catalog" >}}). Otherwise, it will fall back to Flink's `default` dialect.
  When using SQL Gateway configured with [HiveServer2 Endpoint]({{< ref "docs/dev/table/hive-compatibility/hiveserver2" >}}), the current catalog will be a HiveCatalog by default.
- In order to have better syntax and semantic compatibility, it’s highly recommended to load [HiveModule]({{< ref "docs/connectors/table/hive/hive_functions" >}}#use-hive-built-in-functions-via-hivemodule) and
  place it first in the module list, so that Hive built-in functions can be picked up during function resolution.
  Please refer [here]({{< ref "docs/dev/table/modules" >}}#how-to-load-unload-use-and-list-modules) for how to change resolution order.
  But when using SQL Gateway configured with HiveServer2 Endpoint, the Hive module will be loaded automatically.
- Hive dialect only supports 2-part identifiers, so you can't specify catalog for an identifier.
- While all Hive versions support the same syntax, whether a specific feature is available still depends on the
  [Hive version]({{< ref "docs/connectors/table/hive/overview" >}}#supported-hive-versions) you use. For example, updating database
  location is only supported in Hive-2.4.0 or later.
- The Hive dialect is mainly used in batch mode. Some Hive's syntax ([Sort/Cluster/Distributed BY]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sort-cluster-distribute-by" >}}), [Transform]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/transform" >}}), etc.)  haven't been supported in streaming mode yet.
{{< /hint >}}

### SQL Client

SQL dialect can be specified via the `table.sql-dialect` property.
Therefore，you can set the dialect after the SQL Client has launched. 

```bash
Flink SQL> SET table.sql-dialect = hive; -- to use Hive dialect
[INFO] Session property has been set.

Flink SQL> SET table.sql-dialect = default; -- to use Flink default dialect
[INFO] Session property has been set.
```

### SQL Gateway Configured With HiveServer2 Endpoint

When using the SQL Gateway configured with HiveServer2 Endpoint, the dialect will be Hive dialect by default, so you don't need to do anything if you want to use Hive dialect. But you can still
change the dialect to Flink default dialect.

```bash
# assuming has connected to SQL Gateway with beeline
jdbc:hive2> SET table.sql-dialect = default; -- to use Flink default dialect

jdbc:hive2> SET table.sql-dialect = hive; -- to use Hive dialect
```

### Table API

You can set dialect for your TableEnvironment with Table API.

{{< tabs "f19e5e09-c58d-424d-999d-275106d1d5b3" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tableEnv = TableEnvironment.create(settings);

// to use hive dialect
tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

// to use default dialect
tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *
settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(settings)

# to use Hive dialect
t_env.get_config().set_sql_dialect(SqlDialect.HIVE)

# to use Flink default dialect
t_env.get_config().set_sql_dialect(SqlDialect.DEFAULT)
```
{{< /tab >}}
{{< /tabs >}}
