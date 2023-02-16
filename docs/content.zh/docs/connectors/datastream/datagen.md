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

The DataGen connector provides a `Source` implementation that allows for generating input data for
Flink pipelines.
It is useful when developing locally or demoing without access to external systems such as Kafka.
The DataGen connector is built-in, no additional dependencies are required.

Usage
-----

The `DataGeneratorSource` produces N data points in parallel. The source splits the sequence
into as many parallel sub-sequences as there are parallel source subtasks. It drives the data
generation process by supplying "index" values of type `Long` to the user-provided
{{< javadoc name="GeneratorFunction" file="org/apache/flink/connector/datagen/source/GeneratorFunction.html" >}}.

The `GeneratorFunction` is then used for mapping the (sub-)sequences of `Long` values
into the generated events of an arbitrary data type. For instance, the following code will produce the sequence of
`["Number: 0", "Number: 2", ... , "Number: 999"]` records.

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

The order of elements depends on the parallelism. Each sub-sequence will be produced in order.
Consequently, if the parallelism is limited to one, this will produce one sequence in order from
`"Number: 0"` to `"Number: 999"`.

Rate Limiting
-----

`DataGeneratorSource` has built-in support for rate limiting. The following code will produce a stream of
`Long` values at the overall source rate (across all source subtasks) not exceeding 100 events per second.

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

Additional rate limiting strategies, such as limiting the number of records emitted per checkpoint, can
be found in {{< javadoc name="RateLimiterStrategy" file="org/apache/flink/api/connector/source/util/ratelimit/RateLimiterStrategy.html">}}.

Boundedness
-----
This source is always bounded. From a practical perspective, however, setting the number of records
to `Long.MAX_VALUE` turns it into an effectively unbounded source (the end will never be reached). For finite sequences users may want to consider running the application in [`BATCH` execution mode]({{< ref "docs/dev/datastream/execution_mode" >}}#when-canshould-i-use-batch-execution-mode)
.

Notes
-----

{{< hint info >}}
**Note:**  `DataGeneratorSource` can be used to implement Flink jobs with at-least-once and
end-to-end exactly-once processing guarantees under the condition that the output of the `GeneratorFunction`
is deterministic with respect to its input, in other words supplying the same `Long` number always
leads to generating the same output.
{{< /hint >}}

{{< hint info >}}
**Note:**  it is possible to also produce deterministic watermarks right at the
source based on the generated events and a custom {{< javadoc name="WatermarkStrategy" file="org/apache/flink/api/common/eventtime/WatermarkStrategy.html">}}.  
{{< /hint >}}



