---
title: "Builtin Watermark Generators"
weight: 3
type: docs
aliases:
  - /dev/event_timestamp_extractors.html
  - /apis/streaming/event_timestamp_extractors.html
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

# Builtin Watermark Generators

As described in [Generating Watermarks]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}), Flink provides abstractions that
allow the programmer to assign their own timestamps and emit their own
watermarks. More specifically, one can do so by implementing the
`WatermarkGenerator` interface.

In order to further ease the programming effort for such tasks, Flink comes
with some pre-implemented timestamp assigners.  This section provides a list of
them. Apart from their out-of-the-box functionality, their implementation can
serve as an example for custom implementations.

## Monotonously Increasing Timestamps

The simplest special case for *periodic* watermark generation is the when
timestamps seen by a given source task occur in ascending order. In that case,
the current timestamp can always act as a watermark, because no earlier
timestamps will arrive.

Note that it is only necessary that timestamps are ascending *per parallel data
source task*. For example, if in a specific setup one Kafka partition is read
by one parallel data source instance, then it is only necessary that timestamps
are ascending within each Kafka partition. Flink's watermark merging mechanism
will generate correct watermarks whenever parallel streams are shuffled,
unioned, connected, or merged.

{{< tabs "3c316a55-c596-49fd-9f80-3d3b4329415a" >}}
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

## Fixed Amount of Lateness

Another example of periodic watermark generation is when the watermark lags
behind the maximum (event-time) timestamp seen in the stream by a fixed amount
of time. This case covers scenarios where the maximum lateness that can be
encountered in a stream is known in advance, e.g. when creating a custom source
containing elements with timestamps spread within a fixed period of time for
testing. For these cases, Flink provides the `BoundedOutOfOrdernessWatermarks`
generator which takes as an argument the `maxOutOfOrderness`, i.e. the maximum
amount of time an element is allowed to be late before being ignored when
computing the final result for the given window. Lateness corresponds to the
result of `t - t_w`, where `t` is the (event-time) timestamp of an element, and
`t_w` that of the previous watermark.  If `lateness > 0` then the element is
considered late and is, by default, ignored when computing the result of the
job for its corresponding window. See the documentation about [allowed
lateness]({{< ref "docs/dev/datastream/operators/windows" >}}#allowed-lateness) for more information
about working with late elements.

{{< tabs "678f404c-d241-4e45-8e2e-846e34736d6f" >}}
{{< tab "Java" >}}
```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
