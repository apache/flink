---
title: "Joining"
weight: 9 
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

{{< hint warning >}}
**Note:** DataStream API V2 is a new set of APIs, to gradually replace the original DataStream API. It is currently in the experimental stage and is not fully available for production.
{{< /hint >}}

# Joining

Join is used to merge two data streams by matching elements from both streams based on a common key, 
and performing calculations on the matched elements. 

This section will introduce the Join operation in DataStream in detail.

Please note that currently, DataStream supports only non-window INNER joins. 
Other types of joins, such as interval joins, lookup joins, and window joins, will be supported in the future.

## Non-Window Join

To use the Join, users must specify the following:

1. The left and right streams that will be joined.
2. The `KeySelector` of two streams, which determines the join key.
3. The [`JoinFunction`](#joinfunction): the processing logic applied to the matched data.

We will first introduce the APIs for performing Join, and then introduce the JoinFunction.

### APIs for performing Join

There are two approaches to performing a Join operation:

1. Use `BuiltinFuncs.join` to connect two streams and execute the Join.
2. Convert the `JoinFunction` into a `ProcessFunction` and utilize `KeyedPartitionStream#connectAndProcess` to execute the Join.

In the first approach, users can choose between two options based on the types of the input data streams.

- If both input data streams are already Keyed Partition Stream, users can directly combine the two Keyed Partition 
Stream along with the `JoinFunction` to produce the joined data stream.

```java
NonKeyedPartitionStream joinedStream = BuiltinFuncs.join(
  keyedStream1,
  keyedStream2,
  new CustomJoinFunction()
);
```

- If both input data streams are Non-Keyed Partition Stream, users need to incorporate the corresponding 
Join key `KeySelector` and `JoinFunction` to convert the two Non-Keyed Partition Stream into the joined data stream.

```java
NonKeyedPartitionStream joinedStream = BuiltinFuncs.join(
  stream1,
  new CustomJoinKeySelector1(),
  stream2,
  new CustomJoinKeySelector2(),
  new CustomJoinFunction()
);
```

In the second approach, users can convert the `JoinFunction` into a `ProcessFunction`, which can then be processed by 
the DataStream API. An example of this conversion and how to use the converted `ProcessFunction` is provided below:

```java
TwoInputNonBroadcastStreamProcessFunction wrappedJoinFunction = BuiltinFuncs.join(new CustomJoinFunction());
NonKeyedPartitionStream joinedStream = keyedStream1.connectAndProcess(
  keyedstream2,
  wrappedJoinFunction
);
```

Please note that in the second approach, it is essential that the two input data streams are Keyed Partition Stream.


### JoinFunction

`JoinFunction` is an interface used to describe how to calculate the matched data. 
It has only one method `processRecord`. Users can get the matched elements in `processRecord` 
to perform calculations and then output the calculation results.

Below is an example demonstrating how to use a `JoinFunction` to connect student personal 
information with their exam scores.

```java
class JoinStudentInformationAndScore
        implements JoinFunction<StudentInfo, ExamScore, EnrichedStudentExamScore> {

    @Override
    public void processRecord(
            StudentInfo studentInfo,
            ExamScore examScore,
            Collector<EnrichedStudentExamScore> output,
            RuntimeContext ctx)
            throws Exception {
        // do some calculation logic and emit joined result
        EnrichedStudentExamScore studentExamScore = 
                new EnrichedStudentExamScore(studentInfo.getId(), studentInfo.getName(), examScore.getScore());
        output.collect(studentExamScore);
    }
}
```


{{< top >}}
