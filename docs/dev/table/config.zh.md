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

默认情况下，Table & SQL API 已经预先配置为在产生准确结果的同时性能也可接受。

根据 Table 程序的要求，可能需要调整特定的参数用于优化。例如，无界流程序可能需要保证所需的状态大小是有上限的(请参阅 [流式概念](./streaming/query_configuration.html)).

* This will be replaced by the TOC
{:toc}

### 概览

在每个 TableEnvironment 中，`TableConfig` 提供用于配置当前会话的选项。

对于常见或者重要的配置选项，`TableConfig` 提供带有详细注释的 `getters` 和 `setters` 方法。

对于更加高级的配置，用户可以直接访问底层的键值 map 对象。以下章节列举了所有可用于调整 Flink Table & SQL API 程序的选项。

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

<span class="label label-danger">注意</span> 目前，键值选项仅被Blink计划器支持。

### 执行选项

以下选项可用于调整查询执行的性能。

{% include generated/execution_config_configuration.html %}

### 优化器选项

以下选项可以用于调整查询优化器的行为以获得更好的执行计划。

{% include generated/optimizer_config_configuration.html %}
