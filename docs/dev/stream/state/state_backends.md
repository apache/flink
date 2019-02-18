---
title: "State Backends"
nav-parent_id: streaming_state
nav-pos: 5
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



Flink提供了不同的状态后端，用于指定状态的存储方式和位置。

根据你的状态后端，State可以位于Java的heap或者off-heap。Flink还可以管理应用程序的状态，这意味着Flink在处理内存管理（如果需要可能会溢出到磁盘）允许应用程序保持非常大的状态。在默认情况下，配置文件* flink-conf.yaml *可以确定所有Flink作业的状态后端。

但是，也可以基于每个作业覆盖默认的状态后端，如下所示。

有关可用状态后端，其优点，限制和配置参数的详细信息，请参阅[Deployment & Operations](https://ci.apache.org/projects/flink/flink-docs-master/ops/state/state_backends.html)中的相关部分。


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(...);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setStateBackend(...)
{% endhighlight %}
</div>
</div>

{% top %}
