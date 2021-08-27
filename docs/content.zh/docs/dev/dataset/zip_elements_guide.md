---
title: Zipping Elements
nav-title: Zipping Elements
weight: 3
type: docs
aliases:
  - /zh/dev/batch/zip_elements_guide.html
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

<a name="zipping-elements-in-a-dataset"></a>

# 给 DataSet 中的元素编号

在一些算法中，可能需要为数据集元素分配唯一标识符。
本文档阐述了如何将 {{< gh_link file="/flink-java/src/main/java/org/apache/flink/api/java/utils/DataSetUtils.java" name="DataSetUtils" >}} 用于此目的。

<a name="zip-with-a-dense-index"></a>

### 以密集索引编号

`zipWithIndex` 为元素分配连续的标签，接收数据集作为输入并返回一个新的 `(unique id, initial value)` 二元组的数据集。
这个过程需要分为两个（子）过程，首先是计数，然后是标记元素，由于计数操作的同步性，这个过程不能被 pipelined（流水线化）。

可供备选的 `zipWithUniqueId` 是以 pipelined 的方式进行工作的。当唯一标签足够时，首选 `zipWithUniqueId` 。
例如，下面的代码：

{{< tabs "083bbdc6-b9f9-4989-86a8-f32f0ac53111" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(2);
DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");

DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithIndex(in);

result.writeAsCsv(resultPath, "\n", ",");
env.execute();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
env.setParallelism(2)
val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H")

val result: DataSet[(Long, String)] = input.zipWithIndex

result.writeAsCsv(resultPath, "\n", ",")
env.execute()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from flink.plan.Environment import get_environment

env = get_environment()
env.set_parallelism(2)
input = env.from_elements("A", "B", "C", "D", "E", "F", "G", "H")

result = input.zip_with_index()

result.write_text(result_path)
env.execute()
```
{{< /tab >}}
{{< /tabs >}}

可能会生成这些元组：(0,G)，(1,H)，(2,A)，(3,B)，(4,C)，(5,D)，(6,E)，(7,F)

{{< top >}}

<a name="zip-with-a-unique-identifier"></a>

### 以唯一标识符编号

在许多情况下，可能不需要指定连续的标签。
`zipWithUniqueId` 以 pipelined 的方式工作，加快了标签分配的过程。该方法接收一个数据集作为输入，并返回一个新的 `(unique id, initial value)` 二元组的数据集。
例如，下面的代码：

{{< tabs "49a5535f-7835-4204-afd4-40bb1cbfa404" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(2);
DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");

DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithUniqueId(in);

result.writeAsCsv(resultPath, "\n", ",");
env.execute();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
env.setParallelism(2)
val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H")

val result: DataSet[(Long, String)] = input.zipWithUniqueId

result.writeAsCsv(resultPath, "\n", ",")
env.execute()
```
{{< /tab >}}
{{< /tabs >}}

可能会产生这些元组：(0,G)，(1,A)，(2,H)，(3,B)，(5,C)，(7,D)，(9,E)，(11,F)

{{< top >}}
