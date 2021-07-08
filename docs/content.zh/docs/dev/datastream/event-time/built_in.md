---
title: "内置 Watermark 生成器"
weight: 3
type: docs
aliases:
  - /zh/dev/event_timestamp_extractors.html
  - /zh/apis/streaming/event_timestamp_extractors.html
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

# 内置 Watermark 生成器

如[生成 Watermark]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}) 小节中所述，Flink 提供的抽象方法可以允许用户自己去定义时间戳分配方式和 watermark 生成的方式。你可以通过实现 `WatermarkGenerator` 接口来实现上述功能。

为了进一步简化此类任务的编程工作，Flink 框架预设了一些时间戳分配器。本节后续内容有举例。除了开箱即用的已有实现外，其还可以作为自定义实现的示例以供参考。

<a name="monotonously-increasing-timestamps"></a>

## 单调递增时间戳分配器

*周期性* watermark 生成方式的一个最简单特例就是你给定的数据源中数据的时间戳升序出现。在这种情况下，当前时间戳就可以充当 watermark，因为后续到达数据的时间戳不会比当前的小。

注意：在 Flink 应用程序中，如果是并行数据源，则只要求并行数据源中的每个*单分区数据源任务*时间戳递增。例如，设置每一个并行数据源实例都只读取一个 Kafka 分区，则时间戳只需在每个 Kafka 分区内递增即可。Flink 的 watermark 合并机制会在并行数据流进行分发（shuffle）、联合（union）、连接（connect）或合并（merge）时生成正确的 watermark。

{{< tabs "5fd544cc-922b-43e0-9f44-8619bf6424fd" >}}
{{< tab "Java" >}}
```java
WatermarkStrategy.forMonotonousTimestamps();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
WatermarkStrategy.forMonotonousTimestamps()
```
{{< /tab >}}
{{< /tabs >}}

<a name="fixed-amount-of-lateness"></a>

## 数据之间存在最大固定延迟的时间戳分配器

另一个周期性 watermark 生成的典型例子是，watermark 滞后于数据流中最大（事件时间）时间戳一个固定的时间量。该示例可以覆盖的场景是你预先知道数据流中的数据可能遇到的最大延迟，例如，在测试场景下创建了一个自定义数据源，并且这个数据源的产生的数据的时间戳在一个固定范围之内。Flink 针对上述场景提供了 `boundedOutfordernessWatermarks` 生成器，该生成器将 `maxOutOfOrderness` 作为参数，该参数代表在计算给定窗口的结果时，允许元素被忽略计算之前延迟到达的最长时间。其中延迟时长就等于 `t_w - t` ，其中 `t` 代表元素的（事件时间）时间戳，`t_w` 代表前一个 watermark 对应的（事件时间）时间戳。如果 `lateness > 0`，则认为该元素迟到了，并且在计算相应窗口的结果时默认会被忽略。有关使用延迟元素的详细内容，请参阅有关[允许延迟]({{< ref "docs/dev/datastream/operators/windows" >}}#allowed-lateness)的文档。

{{< tabs "9ef0eae9-f6ea-49f6-ab4c-7347a8b49197" >}}
{{< tab "Java" >}}
```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
WatermarkStrategy
  .forBoundedOutOfOrderness(Duration.ofSeconds(10))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
