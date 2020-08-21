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

Flink 的优化器会根据数据大小或集群中的机器数量等各种参数自动地为你的程序选择执行的策略。在多数情况下，了解 Flink 如何执行你的程序是很有用的。

__执行计划（Plan）可视化工具__

Flink 提供了用于查看执行计划的[可视化工具](https://flink.apache.org/visualizer/)，该工具使用作业（Job）执行计划的 JSON 表示形式，并将其可视化为带有执行策略完整注释的图。

以下代码显示了如何从你的程序中打印执行计划的 JSON：

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


要可视化执行计划，请执行以下操作：

1. 使用 Web 浏览器**打开**[可视化工具](https://flink.apache.org/visualizer/)，
2. 将 JSON 字符串**粘贴**到可视化工具的文本区，
3. **点击** draw 按钮。

完成这些步骤后，就可以查看详细的执行计划效果图了。

<img alt="A flink job execution graph." src="{% link /fig/plan_visualizer.png %}" width="80%">


__Web 界面__

Flink 提供用于提交和执行作业的 Web 界面。该界面是 JobManager 用于监视的 Web 界面的一部分，默认情况下在端口 8081 上运行。

你可以在执行作业之前指定程序参数。执行计划可视化工具能够在执行 Flink 作业之前显示执行计划。

{% top %}
