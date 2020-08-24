---
title: "执行计划"
nav-parent_id: execution
nav-pos: 40
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

Flink 的优化器会根据诸如数据量或集群机器数等不同的参数自动地为你的程序选择执行策略。但在大多数情况下，准确地了解 Flink 会如何执行你的程序是很有帮助的。

__执行计划可视化工具__

Flink 为执行计划提供了[可视化工具](https://flink.apache.org/visualizer/)，它可以把用 JSON 格式表示的作业执行计划以图的形式展现，并且其中会包含完整的执行策略标注。

以下代码展示了如何在你的程序中打印 JSON 格式的执行计划：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

...

System.out.println(env.getExecutionPlan());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

...

println(env.getExecutionPlan())
{% endhighlight %}
</div>
</div>

可以通过如下步骤可视化执行计划：

1. 使用你的浏览器**打开**[可视化工具网站](https://flink.apache.org/visualizer/)，
2. 将 JSON 字符串拷贝并**粘贴**到文本框中，
3. **点击** draw 按钮。

完成后，详细的执行计划图会在网页中呈现。

<img alt="A flink job execution graph." src="{{ site.baseurl }}/fig/plan_visualizer.png" width="80%">

__Web 界面__

Flink 提供了用于提交和执行任务的 Web 界面。该界面是 JobManager Web 界面的一部分，起到管理监控的作用，默认情况下运行在 8081 端口。

可视化工具可以在执行 Flink 作业之前展示执行计划图，你可以据此来指定程序的参数。

{% top %}
