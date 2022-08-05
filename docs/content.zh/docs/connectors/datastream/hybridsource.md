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

`HybridSource` 是一个包含一系列数据源的 [sources]({{< ref "docs/dev/datastream/sources" >}})
它解决了从异构数据源顺序读取输入以产生单个输入流的问题。

例如，一个 bootstrap 用例可能需要从 S3 读取几天的有界输入，然后再继续从 Kafka 读取最新的无界输入。当有边界的文件输入结束时，
`HybridSource` 从 `FileSource` 切换到 `KafkaSource` 而不中断应用程序。

在使用 `HybridSource` 之前, 需要创建具有多个数据源的拓扑，并在用户域内定义切换机制，这导致了操作的复杂性和效率低下。

使用 `HybridSource` 多个数据源在 Flink 作业 DAG 中和从 `DataStream` API 的角度显示为单个数据源。

有关更多的背景信息，请参阅 [FLIP-150](https://cwiki.apache.org/confluence/display/FLINK/FLIP-150%3A+Introduce+Hybrid+Source)

想要使用 connector, 请将 ```flink-connector-base``` 依赖添加到项目中:

{{< artifact flink-connector-base >}}

(通常作为具体数据源的可传递依赖。)

## Start position for next source

为了在 `HybridSource` 中安排多个数据源， 除最后一个数据源外的所有数据源都需要被绑定。因此，通常需要为数据源分配一个开始和结束位置。最后一个数据源可能是有界的，在这种情况下，`HybridSource` 是有界的，否则是无界的。
具体情况取决于具体的存储数据源和外部存储系统。

在这里，我们将介绍最基本的场景，然后是一个更复杂的场景，下面是 File/Kafka 示例。 

#### Fixed start position at graph construction time

例如: 从文件中读取直到预定的切换时间，然后继续从 Kafka 中读取。每个数据源覆盖了一个预知范围，因此所包含的数据源可以预先创建，就好像它们是直接使用的。

```java
long switchTimestamp = ...; // derive from file input paths
FileSource<String> fileSource =
  FileSource.forRecordStreamFormat(new TextLineInputFormat(), Path.fromLocalFile(testDir)).build();
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

示例: 文件数据源读取一个非常大的积压数据，花费的时间可能比下一个数据源的保留时间更长。开关需要发生在 “current time- X”。这要求下一个数据源的开始时间设置在切换时间。在这里，我们通过实现 `SourceFactory` 来实现 `KafkaSource` 的延迟构造，需要从之前的文件枚举器转移结束位置。 

注意，枚举器需要支持获取结束时间戳。 目前可能需要对源代码进行定制。
在 [FLINK-23633](https://issues.apache.org/jira/browse/FLINK-23633) 中跟踪了向 `FileSource` 添加动态结束位置的支持。

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
