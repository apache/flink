---
title: "配置"
nav-parent_id: tableapi
nav-pos: 150
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

By default, the Table & SQL API is preconfigured for producing accurate results with acceptable
performance.

Depending on the requirements of a table program, it might be necessary to adjust
certain parameters for optimization. For example, unbounded streaming programs may need to ensure
that the required state size is capped (see [streaming concepts](./streaming/query_configuration.html)).

* This will be replaced by the TOC
{:toc}

### Overview

In every table environment, the `TableConfig` offers options for configuring the current session.

For common or important configuration options, the `TableConfig` provides getters and setters methods
with detailed inline documentation.

For more advanced configuration, users can directly access the underlying key-value map. The following
sections list all available options that can be used to adjust Flink Table & SQL API programs.

<span class="label label-danger">Attention</span> Because options are read at different point in time
when performing operations, it is recommended to set configuration options early after instantiating a
table environment.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// instantiate table environment
TableEnvironment tEnv = ...

tEnv.getConfig()        // access high-level configuration
  .getConfiguration()   // set low-level key-value options
  .setString("table.exec.mini-batch.enabled", "true")
  .setString("table.exec.mini-batch.allow-latency", "5 s")
  .setString("table.exec.mini-batch.size", "5000");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// instantiate table environment
val tEnv: TableEnvironment = ...

tEnv.getConfig         // access high-level configuration
  .getConfiguration    // set low-level key-value options
  .setString("table.exec.mini-batch.enabled", "true")
  .setString("table.exec.mini-batch.allow-latency", "5 s")
  .setString("table.exec.mini-batch.size", "5000")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# instantiate table environment
t_env = ...

t_env.get_config()        # access high-level configuration
  .get_configuration()    # set low-level key-value options
  .set_string("table.exec.mini-batch.enabled", "true")
  .set_string("table.exec.mini-batch.allow-latency", "5 s")
  .set_string("table.exec.mini-batch.size", "5000");
{% endhighlight %}
</div>
</div>

<span class="label label-danger">Attention</span> Currently, key-value options are only supported
for the Blink planner.

### Execution Options

The following options can be used to tune the performance of the query execution.

{% include generated/execution_config_configuration.html %}

### Optimizer Options

The following options can be used to adjust the behavior of the query optimizer to get a better execution plan.

{% include generated/optimizer_config_configuration.html %}
