---
title: "Configuration"
weight: 111
type: docs
aliases:
  - /dev/table/config.html
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

# Configuration

By default, the Table & SQL API is preconfigured for producing accurate results with acceptable
performance.

Depending on the requirements of a table program, it might be necessary to adjust
certain parameters for optimization. For example, unbounded streaming programs may need to ensure
that the required state size is capped (see [streaming concepts]({{< ref "docs/dev/table/concepts/overview" >}})).

### Overview

When instantiating a `TableEnvironment`, `EnvironmentSettings` can be used to pass the desired
configuration for the current session, by passing a `Configuration` object to the 
`EnvironmentSettings`.

Additionally, in every table environment, the `TableConfig` offers options for configuring the
current session.

For common or important configuration options, the `TableConfig` provides getters and setters methods
with detailed inline documentation.

For more advanced configuration, users can directly access the underlying key-value map. The following
sections list all available options that can be used to adjust Flink Table & SQL API programs.

<span class="label label-danger">Attention</span> Because options are read at different point in time
when performing operations, it is recommended to set configuration options early after instantiating a
table environment.

{{< tabs "44bae726-b416-4fa0-8d5d-7aa9a5409e00" >}}
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

{{< hint info >}}
**Note:** All of the following configuration options can also be set globally in 
`conf/flink-conf.yaml` (see [configuration]({{< ref "docs/deployment/config" >}}) and can be later
on overridden in the application, through `EnvironmentSettings`, before instantiating
the `TableEnvironment`, or through the `TableConfig` of the `TableEnvironment`.
{{< /hint >}}

### Execution Options

The following options can be used to tune the performance of the query execution.

{{< generated/execution_config_configuration >}}

### Optimizer Options

The following options can be used to adjust the behavior of the query optimizer to get a better execution plan.

{{< generated/optimizer_config_configuration >}}

### Table Options

The following options can be used to adjust the behavior of the table planner.

{{< generated/table_config_configuration >}}

### SQL Client Options

The following options can be used to adjust the behavior of the sql client.

{{< generated/sql_client_configuration >}}
