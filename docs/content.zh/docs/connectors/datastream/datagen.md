---
title: DataGen
weight: 3
type: docs
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

# DataGen 连接器

DataGen 连接器提供的 `Source` 方法可以为 Flink 管道生成输入数据，这在本地开发或演示过程中无法访问外部系统（如 Kafka）时非常有用。DataGen 连接器是内置的，无需额外依赖。

用法
-----

`DataGeneratorSource` 可以并行地生成 N 个数据点。 该数据源将序列分为与并行源子任务数量相同的并行子序列，并通过用户提供的 {{< javadoc name="GeneratorFunction" file="org/apache/flink/connector/datagen/source/GeneratorFunction.html" >}} 发送 `Long`类型的 “index” 值来驱动数据生成过程。

然后通过 `GeneratorFunction` 将 `Long` 类型的（子）序列数据映射为任意数据类型的生成事件。例如，以下代码可生成
`["Number: 0", "Number: 2", ... , "Number: 999"]` 数据序列。

```java
GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
long numberOfRecords = 1000;

DataGeneratorSource<String> source =
        new DataGeneratorSource<>(generatorFunction, numberOfRecords, Types.STRING);

DataStreamSource<String> stream =
        env.fromSource(source,
        WatermarkStrategy.noWatermarks(),
        "Generator Source");
```

生成的元素顺序取决于并行度，每个子序列内部的元素是有序的。因此，如果并行度设置为1，生成的元素会依次从 `"Number: 0"` 到 `"Number: 999"`。

速度限制
-----

`DataGeneratorSource` 内置了速度限制的功能。以下代码会以不超过每秒100个事件的总速率（含所有源子任务）生成 `Long` 类型的数据流。

```java
GeneratorFunction<Long, Long> generatorFunction = index -> index;
double recordsPerSecond = 100;

DataGeneratorSource<String> source =
        new DataGeneratorSource<>(
             generatorFunction,
             Long.MAX_VALUE,
             RateLimiterStrategy.perSecond(recordsPerSecond),
             Types.STRING);
```

可以在 {{< javadoc name="RateLimiterStrategy" file="org/apache/flink/api/connector/source/util/ratelimit/RateLimiterStrategy.html">}} 找到其他速度限制策略，例如限制每个checkpoint发出的记录数量。

有界性
-----
`DataGeneratorSource` 始终是有界的。然而，从实际来看，将记录数设置为 `Long.MAX_VALUE` 会将其转化为无界数据源（永远不会达到该长度）。对于有界序列，用户可能希望在 [`BATCH` 运行模式]({{< ref "docs/dev/datastream/execution_mode" >}}#when-canshould-i-use-batch-execution-mode) 运行程序。
.

注意事项
-----

{{< hint info >}}
**注意：**  `DataGeneratorSource` 可以实现具有 `at-least-once` 和端到端`exactly-once` 处理保证的Flink作业，前提是 `GeneratorFunction` 的输出相对于其输入是确定的。也就是说，输入相同的 `Long` 数值始终会生成相同的输出。
{{< /hint >}}

{{< hint info >}}
**注意：**  可以基于生成的事件和自定义的 {{< javadoc name="WatermarkStrategy" file="org/apache/flink/api/common/eventtime/WatermarkStrategy.html">}} 在数据源生成确定性 watermark。
{{< /hint >}}
