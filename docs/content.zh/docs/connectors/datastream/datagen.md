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

# DataGen Connector

`DataGen`连接器提供了原生实现，可以为`Flink`生成输入端数据。
当在本地开发或演示时，如果无法访问外部系统（如`Kafka`），它就显得非常有用。`DataGen`连接器是内置的，无需额外的依赖项。

## 用法

`DataGeneratorSource` 以并行方式生成N条数据。然后将该序列拆分为与并行源subtask数量相同的并行子序列。
它通过向用户提供的{{< javadoc name="GeneratorFunction" file="org/apache/flink/connector/datagen/source/GeneratorFunction.html" >}}提供`Long`类型的“index”值来驱动数据生成过程。

`GeneratorFunction`之后被用于将`Long`长度的值的（子）序列映射到生成的任意数据类型的事件。
例如，以下代码将生成序列`["Number: 0", "Number: 2", ... , "Number: 999"]`。

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

元素的顺序取决于并行度。每个子序列将按顺序生成。因此，如果并行度限制为 1，则将按顺序生成一个从 `Number: 0` 到 `Number: 999` 的序列。

## 速率限制

`DataGeneratorSource` 支持速率限制参数。以下代码将生成一个`Long`值数据流，其整体速率（包括所有数据源子任务）不超过每秒 100 个事件。

```java
GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;

DataGeneratorSource<String> source =
        new DataGeneratorSource<>(
             generatorFunction,
             Long.MAX_VALUE,
             RateLimiterStrategy.perSecond(100),
             Types.STRING);
```

其他速率限制策略，例如限制每个检查点发出的记录数量，可以在{{< javadoc name="RateLimiterStrategy" file="org/apache/flink/api/connector/source/util/ratelimit/RateLimiterStrategy.html">}}中找到。

## 有界性
这个源始终是有界的。然而，从实际的角度来看，将记录数设置为 Long.MAX_VALUE 会使其变成一个有效的无界源（结束点永远无法触达）。对于有限序列，用户可能会考虑以批处理执行模式运行应用程序。


## 注意事项

{{< hint info >}}
**注意事项1:**  `DataGeneratorSource` 可以用于实现具有至少一次和端到端精确一次语义的 Flink 作业，前提是 `GeneratorFunction` 的输出对于其输入是确定性的，换句话说，提供相同的`Long`值总是会生成相同的输出。
{{< /hint >}}

{{< hint info >}}
**注意事项2:**  还可以根据生成的事件和自定义{{< javadoc name="WatermarkStrategy" file="org/apache/flink/api/common/eventtime/WatermarkStrategy.html">}}，在源文件中生成确定性`Watermark`。
{{< /hint >}}



