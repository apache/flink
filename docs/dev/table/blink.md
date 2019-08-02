---
title: "Configuration"
nav-id: "config"
nav-parent_id: tableapi
nav-pos: 4
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

This page lists all the options that are used to tune Flink Table API/SQL jobs. You can set options via `TableConfig`:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// environment configuration
TableEnvironment tEnv = ...

// set key-value options
tEnv.getConfig()
  .getConfiguration()
  .setString("sql.exec.mini-batch.enabled", "true")
  .setString("sql.exec.mini-batch.allow-latency", "5 s")
  .setString("sql.exec.mini-batch.size", "5000");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// environment configuration
val tEnv: TableEnvironment = ...

// set key-value options
tEnv.getConfig
  .getConfiguration
  .setString("sql.exec.mini-batch.enabled", "true")
  .setString("sql.exec.mini-batch.allow-latency", "5 s")
  .setString("sql.exec.mini-batch.size", "5000")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# environment configuration
tEnv = ...

# set key-value options
t_env.get_config()
  .get_configuration()
  .set_string("sql.exec.mini-batch.enabled", "true")
  .set_string("sql.exec.mini-batch.allow-latency", "5 s")
  .set_string("sql.exec.mini-batch.size", "5000");
{% endhighlight %}
</div>
</div>

**NOTE: currently, all the options are only works in blink planner. See [how to use blink planner](index.html).**

* This will be replaced by the TOC
{:toc}

### Execution Options

The following options can be used to tune the performance of query execution.

{% include generated/execution_config_configuration.html %}

### Optimizer Options

The following options can be used to tune the behavior of query optimizer to get a better execution plan.

{% include generated/optimizer_config_configuration.html %}
