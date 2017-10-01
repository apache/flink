---
title: "Zipping Elements in a DataSet"
nav-title: Zipping Elements
nav-parent_id: batch
nav-pos: 2
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
`zipWithIndex` assigns consecutive labels to the elements, receiving a data set as input and returning a new data set of `(unique id, initial value)` 2-tuples.
This process requires two passes, first counting then labeling elements, and cannot be pipelined due to the synchronization of counts.
The alternative `zipWithUniqueId` works in a pipelined fashion and is preferred when a unique labeling is sufficient.
For example, the following code:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(2);
DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");

DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithIndex(in);

result.writeAsCsv(resultPath, "\n", ",");
env.execute();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
env.setParallelism(2)
val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H")

val result: DataSet[(Long, String)] = input.zipWithIndex

result.writeAsCsv(resultPath, "\n", ",")
env.execute()
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from flink.plan.Environment import get_environment

env = get_environment()
env.set_parallelism(2)
input = env.from_elements("A", "B", "C", "D", "E", "F", "G", "H")

result = input.zip_with_index()

result.write_text(result_path)
env.execute()
{% endhighlight %}
</div>

</div>

may yield the tuples: (0,G), (1,H), (2,A), (3,B), (4,C), (5,D), (6,E), (7,F)

[Back to top](#top)

### Zip with a Unique Identifier
In many cases one may not need to assign consecutive labels.
`zipWithUniqueId` works in a pipelined fashion, speeding up the label assignment process. This method receives a data set as input and returns a new data set of `(unique id, initial value)` 2-tuples.
For example, the following code:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(2);
DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");

DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithUniqueId(in);

result.writeAsCsv(resultPath, "\n", ",");
env.execute();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
env.setParallelism(2)
val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H")

val result: DataSet[(Long, String)] = input.zipWithUniqueId

result.writeAsCsv(resultPath, "\n", ",")
env.execute()
{% endhighlight %}
</div>

</div>

may yield the tuples: (0,G), (1,A), (2,H), (3,B), (5,C), (7,D), (9,E), (11,F)

[Back to top](#top)

{% top %}
