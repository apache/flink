---
title: "Zipping Elements in a DataSet"
# Sub-level navigation
sub-nav-group: batch
sub-nav-parent: dataset_api
sub-nav-pos: 2
sub-nav-title: Zipping Elements
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

In certain algorithms, one may need to assign unique identifiers to data set elements.
This document shows how {% gh_link /flink-java/src/main/java/org/apache/flink/api/java/utils/DataSetUtils.java "DataSetUtils" %} can be used for that purpose.

* This will be replaced by the TOC
{:toc}

### Zip with a Dense Index
For assigning consecutive labels to the elements, the `zipWithIndex` method should be called. It receives a data set as input and returns a new data set of unique id, initial value tuples.
For example, the following code:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F");

DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithIndex(in);

result.writeAsCsv(resultPath, "\n", ",");
env.execute();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
env.setParallelism(1)
val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F")

val result: DataSet[(Long, String)] = input.zipWithIndex

result.writeAsCsv(resultPath, "\n", ",")
env.execute()
{% endhighlight %}
</div>

</div>

will yield the tuples: (0,A), (1,B), (2,C), (3,D), (4,E), (5,F)

[Back to top](#top)

### Zip with an Unique Identifier
In many cases, one may not need to assign consecutive labels.
`zipWIthUniqueId` works in a pipelined fashion, speeding up the label assignment process. This method receives a data set as input and returns a new data set of unique id, initial value tuples.
For example, the following code:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F");

DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithUniqueId(in);

result.writeAsCsv(resultPath, "\n", ",");
env.execute();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
env.setParallelism(1)
val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F")

val result: DataSet[(Long, String)] = input.zipWithUniqueId

result.writeAsCsv(resultPath, "\n", ",")
env.execute()
{% endhighlight %}
</div>

</div>

will yield the tuples: (0,A), (2,B), (4,C), (6,D), (8,E), (10,F)

[Back to top](#top)
