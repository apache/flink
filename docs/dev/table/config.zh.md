---
title: "配置"
nav-parent_id: tableapi
nav-pos: 110
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

Table 和 SQL API 的默认配置能够确保结果准确，同时也提供可接受的性能。

根据 Table 程序的需求，可能需要调整特定的参数用于优化。例如，无界流程序可能需要保证所需的状态是有限的(请参阅 [流式概念](./streaming/query_configuration.html)).

* This will be replaced by the TOC
{:toc}

### 概览

在每个 TableEnvironment 中，`TableConfig` 提供用于当前会话的配置项。

对于常见或者重要的配置项，`TableConfig` 提供带有详细注释的 `getters` 和 `setters` 方法。

对于更加高级的配置，用户可以直接访问底层的 key-value 配置项。以下章节列举了所有可用于调整 Flink Table 和 SQL API 程序的配置项。

<span class="label label-danger">注意</span> 因为配置项会在执行操作的不同时间点被读取，所以推荐在实例化 TableEnvironment 后尽早地设置配置项。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// instantiate table environment
TableEnvironment tEnv = ...

// access flink configuration
Configuration configuration = tEnv.getConfig().getConfiguration();
// set low-level key-value options
configuration.setString("table.exec.mini-batch.enabled", "true");
configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
configuration.setString("table.exec.mini-batch.size", "5000");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// instantiate table environment
val tEnv: TableEnvironment = ...

// access flink configuration
val configuration = tEnv.getConfig().getConfiguration()
// set low-level key-value options
configuration.setString("table.exec.mini-batch.enabled", "true")
configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
configuration.setString("table.exec.mini-batch.size", "5000")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# instantiate table environment
t_env = ...

# access flink configuration
configuration = t_env.get_config().get_configuration();
# set low-level key-value options
configuration.set_string("table.exec.mini-batch.enabled", "true");
configuration.set_string("table.exec.mini-batch.allow-latency", "5 s");
configuration.set_string("table.exec.mini-batch.size", "5000");
{% endhighlight %}
</div>
</div>

<span class="label label-danger">注意</span> 目前，key-value 配置项仅被 Blink planner 支持。

### 执行配置

以下选项可用于优化查询执行的性能。

{% include generated/execution_config_configuration.html %}

### 优化器配置

以下配置可以用于调整查询优化器的行为以获得更好的执行计划。

{% include generated/optimizer_config_configuration.html %}

### Planner 配置

以下配置可以用于调整 planner 的行为。

{% include generated/table_config_configuration.html %}
