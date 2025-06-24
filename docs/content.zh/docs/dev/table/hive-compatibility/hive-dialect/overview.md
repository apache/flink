---
title: "概览"
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

# Hive 方言

从 1.11.0 开始，在使用 Hive 方言时，Flink 允许用户用 Hive 语法来编写 SQL 语句。
通过提供与 Hive 语法的兼容性，我们旨在改善与 Hive 的互操作性，并减少用户需要在 Flink 和 Hive 之间切换来执行不同语句的情况。

## 使用 Hive 方言

Flink 目前支持两种 SQL 方言: `default` 和 `hive`。你需要先切换到 Hive 方言，然后才能使用 Hive 语法编写。下面介绍如何使用 SQL 客户端，启动了 HiveServer2 endpoint 的 SQL Gateway 和 Table API 设置方言。
还要注意，你可以为执行的每个语句动态切换方言。无需重新启动会话即可使用其他方言。

{{< hint warning >}}
**Note:**

- 为了使用 Hive 方言, 你必须首先添加和 Hive 相关的依赖. 请参考 [Hive dependencies]({{< ref "docs/connectors/table/hive/overview" >}}#dependencies) 如何添加这些依赖。
- 请确保当前的 Catalog 是 [HiveCatalog]({{< ref "docs/connectors/table/hive/hive_catalog" >}}). 否则, 将使用 Flink 的默认方言。
  在启动了 [HiveServer2 Endpoint]({{< ref "docs/dev/table/hive-compatibility/hiveserver2" >}}) 的 SQL Gateway 下，默认当前的 Catalog 就是 HiveCatalog。
- 为了实现更好的语法和语义的兼容，强烈建议首先加载 [HiveModule]({{< ref "docs/connectors/table/hive/hive_functions" >}}#use-hive-built-in-functions-via-hivemodule) 并将其放在 Module 列表的首位，以便在函数解析时优先使用 Hive 内置函数。
  请参考文档 [here]({{< ref "docs/dev/table/modules" >}}#how-to-load-unload-use-and-list-modules) 来将 HiveModule 放在 Module 列表的首。
  在启动了 HiveServer2 endpoint 的 SQL Gateway，HiveModule 已经被加载进来了。
- Hive 方言只支持 `db.table` 这种两级的标识符，不支持带有 Catalog 名字的标识符。
- 虽然所有 Hive 版本支持相同的语法，但是一些特定的功能是否可用仍取决于你使用的 [Hive 版本]({{< ref "docs/connectors/table/hive/overview" >}}#支持的hive版本)。例如，更新数据库位置
  只在 Hive-2.4.0 或更高版本支持。
- Hive 方言主要是在批模式下使用的，某些 Hive 的语法([Sort/Cluster/Distributed BY]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sort-cluster-distribute-by" >}}), [Transform]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/transform" >}}), 等)还没有在流模式下支持。
{{< /hint >}}

### SQL Client

SQL 方言可以通过 `table.sql-dialect` 属性指定。你可以在 SQL 客户端启动后设置方言。

```bash
Flink SQL> SET table.sql-dialect = hive; -- 使用 Hive 方言
[INFO] Session property has been set.

Flink SQL> SET table.sql-dialect = default; -- 使用 Flink 默认 方言
[INFO] Session property has been set.
```

### SQL Gateway Configured With HiveServer2 Endpoint

在启动了 HiveServer2 endpoint 的 SQL Gateway中，会默认使用 Hive 方言，所以如果你想使用 Hive 方言的话，你不需要手动切换至 Hive 方言，直接就能使用。但是如果你想使用 Flink 的默认方言，你也手动进行切换。

```bash
# 假设已经通过 beeline 连接上了 SQL Gateway
jdbc:hive2> SET table.sql-dialect = default; -- 使用 Flink 默认 方言

jdbc:hive2> SET table.sql-dialect = hive; -- 使用 Hive 方言
```

### Table API

你可以使用 Table API 为 TableEnvironment 设置方言。

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

# to use hive dialect
t_env.get_config().set_sql_dialect(SqlDialect.HIVE)

# to use default dialect
t_env.get_config().set_sql_dialect(SqlDialect.DEFAULT)
```
{{< /tab >}}
{{< /tabs >}}
