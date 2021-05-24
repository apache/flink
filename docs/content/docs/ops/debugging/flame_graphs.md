---
title: "Flame Graphs"
weight: 3
type: docs
aliases:
  - /ops/debugging/flame_graphs.html
  - /ops/debugging/flame_graphs
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

# Flame Graphs

[Flame Graphs](http://www.brendangregg.com/flamegraphs.html) are a visualization that effectively surfaces answers to questions like:
- Which methods are currently consuming CPU resources?
- How does consumption by one method compare to the others?
- Which series of calls on the stack led to executing a particular method?

{{< img src="/fig/flame_graph_on_cpu.png" class="img-fluid" width="90%" >}}
{{% center %}}
Flame Graph
{{% /center %}}

Flame Graphs are constructed by sampling stack traces a number of times. Each method call is presented by a bar, where the length of the bar is proportional to the number of times it is present in the samples.

Starting with Flink 1.13, Flame Graphs are natively supported in Flink. In order to produce a Flame Graph, navigate to the job graph of a running job, select an operator of interest and in the menu to the right click on the Flame Graph tab:  

{{< img src="/fig/flame_graph_operator.png" class="img-fluid" width="90%" >}}
{{% center %}}
Operator's On-CPU Flame Graph
{{% /center %}}

{{< hint warning >}}

Any measurement process in and of itself inevitably affects the subject of measurement (see the [double-split experiment](https://en.wikipedia.org/wiki/Double-slit_experiment#Relational_interpretation)). Sampling CPU stack traces is no exception. In order to prevent unintended impacts on production environments, Flame Graphs are currently available as an opt-in feature. To enable it, you'll need to set [`rest.flamegraph.enabled: true`]({{< ref "docs/deployment/config">}}#rest-flamegraph-enabled) in `conf/flink-conf.yaml`. We recommend enabling it in development and pre-production environments, but you should treat it as an experimental feature in production.

{{< /hint >}}

Apart from the On-CPU Flame Graphs, [Off-CPU](http://www.brendangregg.com/FlameGraphs/offcpuflamegraphs.html) and Mixed visualizations are available and can be switched between by using the selector at the top of the pane:

{{< img src="/fig/flame_graph_selector.png" class="img-fluid" width="30%" >}}

The Off-CPU Flame Graph visualizes blocking calls found in the samples. A distinction is made as follows:
- On-CPU: `Thread.State` in **[RUNNABLE, NEW]**
- Off-CPU: `Thread.State` in **[TIMED_WAITING, WAITING, BLOCKED]**

{{< img src="/fig/flame_graph_off_cpu.png" class="img-fluid" width="90%" >}}
{{% center %}}
Off-CPU Flame Graph
{{% /center %}}

Mixed mode Flame Graphs are constructed from stack traces of threads in all possible states.

{{< img src="/fig/flame_graph_mixed.png" class="img-fluid" width="90%" >}}
{{% center %}}
Flame Graph in Mixed Mode
{{% /center %}}

##  Sampling process

The collection of stack traces is done purely within the JVM, so only method calls within the Java runtime are visible (no system calls).

Flame Graph construction is performed at the level of an individual [operator]({{< ref "docs/concepts/glossary" >}}#operator), i.e. all [task]({{< ref "docs/concepts/glossary" >}}#task) threads of that operator are sampled in parallel and their stack traces are combined. 



{{< hint info >}}
**Note:** 
Stack trace samples from all threads of an operator are combined together. If a method call consumes 100% of the resources in one of the parallel tasks but none in the others, the bottleneck might be obscured by being averaged out.   

There are plans to address this limitation in the future by providing "drill down" visualizations to the task level.
{{< /hint >}}
