---
title: Hybrid Source
weight: 8
type: docs
aliases:
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

# Hybrid Source

{{< hint info >}}
This feature is available starting from release 1.13.3
{{< /hint >}}

`HybridSource` is a source that contains a list of concrete [sources]({{< ref "docs/dev/datastream/sources" >}}).
It solves the problem of sequentially reading input from heterogeneous sources to produce a single input stream.

For example, a bootstrap use case may need to read several days worth of bounded input from S3 before continuing with the latest unbounded input from Kafka.
`HybridSource` switches from `FileSource` to `KafkaSource` when the bounded file input finishes without  interrupting the application.

Prior to `HybridSource`, it was necessary to create a topology with multiple sources and define a switching mechanism in user land, which leads to operational complexity and inefficiency.

With `HybridSource` the multiple sources appear as a single source in the Flink job graph and from `DataStream` API perspective.

For more background see [FLIP-150](https://cwiki.apache.org/confluence/display/FLINK/FLIP-150%3A+Introduce+Hybrid+Source)

To use the connector, add the ```flink-connector-base``` dependency to your project:

{{< artifact flink-connector-base >}}

(Typically comes as transitive dependency with concrete sources.)

## Start position for next source

To arrange multiple sources in a `HybridSource`, all sources except the last one need to be bounded. Therefore, the sources typically need to be assigned a start and end position. The last source may be bounded in which case the `HybridSource` is bounded and unbounded otherwise.
Details depend on the specific source and the external storage systems.

Here we cover the most basic and then a more complex scenario, following the File/Kafka example. 

#### Fixed start position at graph construction time

Example: Read till pre-determined switch time from files and then continue reading from Kafka.
Each source covers an upfront known range and therefore the contained sources can be created upfront as if they were used directly:

```java
long switchTimestamp = ...; // derive from file input paths
FileSource<String> fileSource =
  FileSource.forRecordStreamFormat(new TextLineFormat(), Path.fromLocalFile(testDir)).build();
KafkaSource<String> kafkaSource =
          KafkaSource.<String>builder()
                  .setStartingOffsets(OffsetsInitializer.timestamp(switchTimestamp + 1))
                  .build();
HybridSource<String> hybridSource =
          HybridSource.builder(fileSource)
                  .addSource(kafkaSource)
                  .build();
```  

#### Dynamic start position at switch time

Example: File source reads a very large backlog, taking potentially longer than retention available for next source.
Switch needs to occur at "current time - X". This requires the start time for the next source to be set at switch time.
Here we require transfer of end position from the previous file enumerator for deferred construction of `KafkaSource`
by implementing `SourceFactory`.

Note that enumerators need to support getting the end timestamp. This may currently require a source customization.
Adding support for dynamic end position to `FileSource` is tracked in [FLINK-23633](https://issues.apache.org/jira/browse/FLINK-23633).

```java
FileSource<String> fileSource = CustomFileSource.readTillOneDayFromLatest();
HybridSource<String> hybridSource =
    HybridSource.<String, CustomFileSplitEnumerator>builder(fileSource)
        .addSource(
            switchContext -> {
              CustomFileSplitEnumerator previousEnumerator =
                  switchContext.getPreviousEnumerator();
              // how to get timestamp depends on specific enumerator
              long switchTimestamp = previousEnumerator.getEndTimestamp();
              KafkaSource<String> kafkaSource =
                  KafkaSource.<String>builder()
                      .setStartingOffsets(OffsetsInitializer.timestamp(switchTimestamp + 1))
                      .build();
              return kafkaSource;
            },
            Boundedness.CONTINUOUS_UNBOUNDED)
        .build();
```
