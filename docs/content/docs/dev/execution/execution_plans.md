---
title: "Execution Plans"
weight: 41
type: docs
bookToc: false
aliases:
  - /dev/execution_plans.html
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

# Execution Plans

Depending on various parameters such as data size or number of machines in the cluster, Flink's
optimizer automatically chooses an execution strategy for your program. In many cases, it can be
useful to know how exactly Flink will execute your program.

__Plan Visualization Tool__

Flink provides a [visualization tool](https://flink.apache.org/visualizer/) for execution plans, which takes a JSON
representation of the job execution plan and visualizes it as a graph with complete annotations of execution strategies.

The following code shows how to print the execution plan JSON from your program:

{{< tabs "c0c7133e-3c2a-4f53-a27c-6fba561e3aad" >}}
{{< tab "Java" >}}
```java
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

...

System.out.println(env.getExecutionPlan());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = ExecutionEnvironment.getExecutionEnvironment

...

println(env.getExecutionPlan())
```
{{< /tab >}}
{{< /tabs >}}


To visualize the execution plan, do the following:

1. **Open** the [visualizer](https://flink.apache.org/visualizer/) with your web browser,
2. **Paste** the JSON string into the text field, and
3. **Press** the draw button.

After these steps, a detailed execution plan will be visualized.

{{< img alt="A flink job execution graph." src="/fig/plan_visualizer.png" width="80%" >}}

__Web Interface__

Flink offers a web interface for submitting and executing jobs. The interface is part of the JobManager's
web interface for monitoring, per default running on port 8081.

You may specify program arguments before the job is executed. The plan visualization enables you to show
the execution plan before executing the Flink job.

{{< top >}}
