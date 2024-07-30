---
title: "Full Window Partition"
weight: 5
type: docs
aliases:
  - /dev/stream/operators/full_window_partition.html
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

# Full Window Partition Processing on DataStream

This page explains the use of full window partition processing API on DataStream.
Flink enables both keyed and non-keyed DataStream to directly transform into 
`PartitionWindowedStream` now. 
The `PartitionWindowedStream` represents collecting all records of each subtask separately 
into a full window.
The `PartitionWindowedStream` support four APIs: `mapPartition`, `sortPartition`, `aggregate`
and `reduce`.

Note: Details about the design and implementation of the full window partition processing can be
found in the proposal and design document
[FLIP-380: Support Full Partition Processing On Non-keyed DataStream](https://cwiki.apache.org/confluence/display/FLINK/FLIP-380%3A+Support+Full+Partition+Processing+On+Non-keyed+DataStream).

## MapPartition

`MapPartition` represents collecting all records of each subtask separately into a full window 
and process them using the given `MapPartitionFunction` within each subtask. The
`MapPartitionFunction` is called at the end of inputs.

An example of calculating the sum of the elements in each subtask is as follows:

```java
DataStream<Integer> dataStream = //...
PartitionWindowedStream<Integer> partitionWindowedDataStream = dataStream.fullWindowPartition();

DataStream<Integer> resultStream = partitionWindowedDataStream.mapPartition(
        new MapPartitionFunction<Integer, Integer>() {
            @Override
            public void mapPartition(
                    Iterable<Integer> values, Collector<Integer> out) {
                int result = 0;
                for (Integer value : values) {
                    result += value;
                }
                out.collect(result);
            }
        }
);
```

## SortPartition
`SortPartition` represents collecting all records of each subtask separately into a full window 
and sorts them by the given record comparator in each subtask at the end of inputs.

An example of sorting the records by the first element of tuple in each subtask is as follows:

```java
DataStream<Tuple2<Integer, Integer>> dataStream = //...
PartitionWindowedStream<Tuple2<Integer, Integer>> partitionWindowedDataStream = dataStream.fullWindowPartition();
DataStream<Integer> resultStream = partitionWindowedDataStream.sortPartition(0, Order.ASCENDING);
```

## Aggregate
`Aggregate` represents collecting all records of each subtask separately into a full window and 
applies the given `AggregateFunction` to the records of the window. The `AggregateFunction`
is called for each element, aggregating values incrementally within the window.

An example of aggregate the records in each subtask is as follows:

```java
DataStream<Tuple2<Integer, Integer>> dataStream = //...
PartitionWindowedStream<Tuple2<Integer, Integer>> partitionWindowedDataStream = dataStream.fullWindowPartition();
DataStream<Integer> resultStream = partitionWindowedDataStream.aggregate(new AggregateFunction<>{...});
```

## Reduce
`Reduce` represents applies a reduce transformation on all the records in the partition. 
The `ReduceFunction` will be called for every record in the window.
An example is as follows:

```java
DataStream<Tuple2<Integer, Integer>> dataStream = //...
PartitionWindowedStream<Tuple2<Integer, Integer>> partitionWindowedDataStream = dataStream.fullWindowPartition();
DataStream<Integer> resultStream = partitionWindowedDataStream.aggregate(new ReduceFunction<>{...});
```

{{< top >}}
