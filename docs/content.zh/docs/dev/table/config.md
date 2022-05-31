---
title: "配置"
weight: 111
type: docs
aliases:
  - /zh/dev/table/config.html
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

# 配置

Table 和 SQL API 的默认配置能够确保结果准确，同时也提供可接受的性能。

根据 Table 程序的需求，可能需要调整特定的参数用于优化。例如，无界流程序可能需要保证所需的状态是有限的(请参阅 [流式概念]({{< ref "docs/dev/table/concepts/overview" >}})).



### 概览

当实例化一个 `TableEnvironment` 时，可以使用 `EnvironmentSettings` 来传递用于当前会话的所期望的配置项 —— 传递一个 `Configuration` 对象到 `EnvironmentSettings`。

此外，在每个 TableEnvironment 中，`TableConfig` 提供用于当前会话的配置项。

对于常见或者重要的配置项，`TableConfig` 提供带有详细注释的 `getters` 和 `setters` 方法。

对于更加高级的配置，用户可以直接访问底层的 key-value 配置项。以下章节列举了所有可用于调整 Flink Table 和 SQL API 程序的配置项。

<span class="label label-danger">注意</span> 因为配置项会在执行操作的不同时间点被读取，所以推荐在实例化 TableEnvironment 后尽早地设置配置项。

{{< tabs "ec2c3d9c-2ecd-4017-9c77-fb32cd6966cf" >}}
{{< tab "Java" >}}
```java
// instantiate table environment
Configuration configuration = new Configuration();
// set low-level key-value options
configuration.setString("table.exec.mini-batch.enabled", "true");
configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
configuration.setString("table.exec.mini-batch.size", "5000");
EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode().withConfiguration(configuration).build();
TableEnvironment tEnv = TableEnvironment.create(settings);

// access flink configuration after table environment instantiation
TableConfig tableConfig = tEnv.getConfig();
// set low-level key-value options
tableConfig.set("table.exec.mini-batch.enabled", "true");
tableConfig.set("table.exec.mini-batch.allow-latency", "5 s");
tableConfig.set("table.exec.mini-batch.size", "5000");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// instantiate table environment
val configuration = new Configuration;
// set low-level key-value options
configuration.setString("table.exec.mini-batch.enabled", "true")
configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
configuration.setString("table.exec.mini-batch.size", "5000")
val settings = EnvironmentSettings.newInstance
  .inStreamingMode.withConfiguration(configuration).build
val tEnv: TableEnvironment = TableEnvironment.create(settings)

// access flink configuration after table environment instantiation
val tableConfig = tEnv.getConfig()
// set low-level key-value options
tableConfig.set("table.exec.mini-batch.enabled", "true")
tableConfig.set("table.exec.mini-batch.allow-latency", "5 s")
tableConfig.set("table.exec.mini-batch.size", "5000")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# instantiate table environment
configuration = Configuration()
configuration.set("table.exec.mini-batch.enabled", "true")
configuration.set("table.exec.mini-batch.allow-latency", "5 s")
configuration.set("table.exec.mini-batch.size", "5000")
settings = EnvironmentSettings.new_instance() \
...     .in_streaming_mode() \
...     .with_configuration(configuration) \
...     .build()

t_env = TableEnvironment.create(settings)

# access flink configuration after table environment instantiation
table_config = t_env.get_config()
# set low-level key-value options
table_config.set("table.exec.mini-batch.enabled", "true")
table_config.set("table.exec.mini-batch.allow-latency", "5 s")
table_config.set("table.exec.mini-batch.size", "5000")
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```
Flink SQL> SET 'table.exec.mini-batch.enabled' = 'true';
Flink SQL> SET 'table.exec.mini-batch.allow-latency' = '5s';
Flink SQL> SET 'table.exec.mini-batch.size' = '5000';
```
{{< /tab >}}
{{< /tabs >}}

### 执行配置

以下选项可用于优化查询执行的性能。

{{< generated/execution_config_configuration >}}

### 优化器配置

以下配置可以用于调整查询优化器的行为以获得更好的执行计划。

{{< generated/optimizer_config_configuration >}}

### Planner 配置

以下配置可以用于调整 planner 的行为。

{{< generated/table_config_configuration >}}

### SQL Client 配置

以下配置可以用于调整 sql client 的行为。

{{< generated/sql_client_configuration >}}
