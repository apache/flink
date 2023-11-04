---
title: "How to Migrate from DataSet to DataStream"
weight: 302
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

# How to Migrate from DataSet to DataStream

The DataSet API has been formally deprecated and will no longer receive active maintenance and support. It will be removed in the
Flink 2.0 version. Flink users are recommended to migrate from the DataSet API to the DataStream API, Table API and SQL for their 
data processing requirements.

Noticed that APIs in DataStream do not always match those in DataSet exactly. The purpose of this document is to help users understand 
how to achieve the same data processing behaviors with DataStream APIs as using DataSet APIs.

According to the changes in coding and execution efficiency that are required for migration, we categorized DataSet APIs into 4 categories:

- Category 1: APIs that have exact equivalent in DataStream, which requires barely any changes to migrate.

- Category 2: APIs whose behavior can be achieved by other APIs with different semantics in DataStream, which might require some code changes for 
migration but will result in the same execution efficiency.

- Category 3: APIs whose behavior can be achieved by other APIs with different semantics in DataStream, with potentially additional cost in execution efficiency.

- Category 4: APIs whose behaviors are not supported by DataStream API.

The subsequent sections will first introduce how to set the execution environment and source/sink, then provide detailed explanations on how to migrate 
each category of DataSet APIs to the DataStream APIs, highlighting the specific considerations and challenges associated with each 
category.


## Setting the execution environment

The first step of migrating an application from DataSet API to DataStream API is to replace `ExecutionEnvironment` with `StreamExecutionEnvironment`.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">DataSet</th>
            <th class="text-left">DataStream</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                {{< highlight "java" >}}
// Create the execution environment
ExecutionEnvironment.getExecutionEnvironment();
// Create the local execution environment
ExecutionEnvironment.createLocalEnvironment();
// Create the collection environment
new CollectionEnvironment();
// Create the remote environment
ExecutionEnvironment.createRemoteEnvironment(String host, int port, String... jarFiles);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
// Create the execution environment
StreamExecutionEnvironment.getExecutionEnvironment();
// Create the local execution environment
StreamExecutionEnvironment.createLocalEnvironment();
// The collection environment is not supported.
// Create the remote environment
StreamExecutionEnvironment.createRemoteEnvironment(String host, int port, String... jarFiles);
                {{< /highlight >}}
            </td>
        </tr>
    </tbody>
</table>

Unlike DataSet, DataStream supports processing on both bounded and unbounded data streams. Thus, user needs to explicitly set the execution mode
to `RuntimeExecutionMode.BATCH` if that is expected.

```java
StreamExecutionEnvironment executionEnvironment = // [...];
executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
```

## Using the streaming sources and sinks

### Sources

The DataStream API uses `DataStreamSource` to read records from external system, while the DataSet API uses the `DataSource`.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">DataSet</th>
            <th class="text-left">DataStream</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                {{< highlight "java" >}}
// Read data from file
DataSource<> source = ExecutionEnvironment.readFile(inputFormat, filePath);
// Read data from collection
DataSource<> source = ExecutionEnvironment.fromCollection(data);
// Read data from inputformat
DataSource<> source = ExecutionEnvironment.createInput(inputFormat)
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
// Read data from file
DataStreamSource<> source = StreamExecutionEnvironment.readFile(inputFormat, filePath);
// Read data from collection
DataStreamSource<> source = StreamExecutionEnvironment.fromCollection(data);
// Read data from inputformat
DataStreamSource<> source = StreamExecutionEnvironment.createInput(inputFormat)
                {{< /highlight >}}
            </td>
        </tr>
    </tbody>
</table>

### Sinks

The DataStream API uses `DataStreamSink` to write records to external system, while the
DataSet API uses the `DataSink`.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">DataSet</th>
            <th class="text-left">DataStream</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                {{< highlight "java" >}}
// Write to outputformat
DataSink<> sink = dataSet.output(outputFormat);
// Write to csv file
DataSink<> sink = dataSet.writeAsCsv(filePath);
// Write to text file
DataSink<> sink = dataSet.writeAsText(filePath);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
// Write to sink
DataStreamSink<> sink = dataStream.sinkTo(sink)
// Write to csv file
DataStreamSink<> sink = dataStream.writeAsCsv(path);
// Write to text file
DataStreamSink<> sink = dataStream.writeAsText(path);
                {{< /highlight >}}
            </td>
        </tr>
    </tbody>
</table>

If you are looking for pre-defined source and sink connectors of DataStream, please check the [Connector Docs]({{< ref "docs/connectors/datastream/overview" >}})

## Migrating DataSet APIs

### Category 1

For Category 1, these DataSet APIs have exact equivalent in DataStream, which requires barely any changes to migrate.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">Operations</th>
            <th class="text-left">DataSet</th>
            <th class="text-left">DataStream</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Map</td>
            <td>
                {{< highlight "java" >}}
dataSet.map(new MapFunction<>(){
// implement user-defined map logic
});
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
dataStream.map(new MapFunction<>(){
// implement user-defined map logic
});
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>FlatMap</td>
            <td>
                {{< highlight "java" >}}
dataSet.flatMap(new FlatMapFunction<>(){
// implement user-defined flatmap logic
});
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
dataStream.flatMap(new FlatMapFunction<>(){
// implement user-defined flatmap logic
});
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Filter</td>
            <td>
                {{< highlight "java" >}}
dataSet.filter(new FilterFunction<>(){
// implement user-defined filter logic
});
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
dataStream.filter(new FilterFunction<>(){
// implement user-defined filter logic
});
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Union</td>
            <td>
                {{< highlight "java" >}}
dataSet1.union(dataSet2);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
dataStream1.union(dataStream2);
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Rebalance</td>
            <td>
                {{< highlight "java" >}}
dataSet.rebalance();
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
dataStream.rebalance();
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Project</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple3<>> dataSet = // [...]
dataSet.project(2,0);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple3<>> dataStream = // [...]
dataStream.project(2,0);
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Reduce on Grouped DataSet</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet = // [...]
dataSet.groupBy(value -> value.f0)
       .reduce(new ReduceFunction<>(){
        // implement user-defined reduce logic
        });
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple2<>> dataStream = // [...]
dataStream.keyBy(value -> value.f0)
          .reduce(new ReduceFunction<>(){
          // implement user-defined reduce logic
          });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Aggregate on Grouped DataSet</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet = // [...]
// compute sum of the second field
dataSet.groupBy(value -> value.f0)
       .aggregate(SUM, 1);
// compute min of the second field
dataSet.groupBy(value -> value.f0)
       .aggregate(MIN, 1);
// compute max of the second field
dataSet.groupBy(value -> value.f0)
       .aggregate(MAX, 1);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple2<>> dataStream = // [...]
// compute sum of the second field
dataStream.keyBy(value -> value.f0)
          .sum(1);
// compute min of the second field
dataStream.keyBy(value -> value.f0)
          .min(1);
// compute max of the second field
dataStream.keyBy(value -> value.f0)
          .max(1);
                {{< /highlight >}}
            </td>
        </tr>
    </tbody>
</table>

### Category 2

For category 2, the behavior of these DataSet APIs can be achieved by other APIs with different semantics in DataStream, which might require some code changes for
migration but will result in the same execution efficiency. 

Operations on a full DataSet correspond to the global window aggregation in DataStream with a custom window that is triggered at the end of the inputs. The `EndOfStreamWindows`
in the [Appendix]({{< ref "docs/dev/datastream/dataset_migration#endofstreamwindows" >}}) shows how such a window can be implemented. We will reuse it in the rest of this document.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">Operations</th>
            <th class="text-left">DataSet</th>
            <th class="text-left">DataStream</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Distinct</td>
            <td>
                {{< highlight "java" >}}
DataSet<Integer> dataSet = // [...]
dataSet.distinct();
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Integer> dataStream = // [...]
dataStream.keyBy(value -> value)
          .reduce((value1, value2) -> value1);
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Hash-Partition</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet = // [...]
dataSet.partitionByHash(value -> value.f0);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple2<>> dataStream = // [...]
// partition by the hashcode of key
dataStream.partitionCustom(
          (key, numSubpartition) -> key.hashCode() % numSubpartition,
          value -> value.f0);
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Reduce on Full DataSet</td>
            <td>
                {{< highlight "java" >}}
DataSet<String> dataSet = // [...]
dataSet.reduce(new ReduceFunction<>(){
        // implement user-defined reduce logic
        });
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<String> dataStream = // [...]
dataStream.windowAll(EndOfStreamWindows.get())
          .reduce(new ReduceFunction<>(){
          // implement user-defined reduce logic
          });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Aggregate on Full DataSet</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet = // [...]
// compute sum of the second field
dataSet.aggregate(SUM, 1);
// compute min of the second field
dataSet.aggregate(MIN, 1);
// compute max of the second field
dataSet.aggregate(MAX, 1);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple2<>> dataStream = // [...]
// compute sum of the second field
dataStream.windowAll(EndOfStreamWindows.get())
          .sum(1);
// compute min of the second field
dataStream.windowAll(EndOfStreamWindows.get())
          .min(1);
// compute max of the second field
dataStream.windowAll(EndOfStreamWindows.get())
          .max(1);
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>GroupReduce on Full DataSet</td>
            <td>
                {{< highlight "java" >}}
DataSet<Integer> dataSet = // [...]
dataSet.reduceGroup(new GroupReduceFunction<>(){
        // implement user-defined group reduce logic
        });
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Integer> dataStream = // [...]
dataStream.windowAll(EndOfStreamWindows.get())
          .apply(new WindowFunction<>(){
          // implement user-defined group reduce logic
          });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>GroupReduce on Grouped DataSet</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet = // [...]
dataSet.groupBy(value -> value.f0)
       .reduceGroup(new GroupReduceFunction<>(){
       // implement user-defined group reduce logic
       });
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple2<>> dataStream = // [...]
dataStream.keyBy(value -> value.f0)
          .window(EndOfStreamWindows.get())
          .apply(new WindowFunction<>(){
          // implement user-defined group reduce logic
          });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>First-n</td>
            <td>
                {{< highlight "java" >}}
dataSet.first(n)
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
dataStream.windowAll(EndOfStreamWindows.get())
          .apply(new AllWindowFunction<>(){
          // implement first-n logic
          });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Join</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet1 = // [...]
DataSet<Tuple2<>> dataSet2 = // [...]
dataSet1.join(dataSet2)
        .where(value -> value.f0)
        .equalTo(value -> value.f0)
        .with(new JoinFunction<>(){
        // implement user-defined join logic
        });
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple2<>> dataStream1 = // [...]
DataStream<Tuple2<>> dataStream2 = // [...]
dataStream1.join(dataStream2)
           .where(value -> value.f0)
           .equalTo(value -> value.f0)
           .window(EndOfStreamWindows.get()))
           .apply(new JoinFunction<>(){
           // implement user-defined join logic
           });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>CoGroup</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet1 = // [...]
DataSet<Tuple2<>> dataSet2 = // [...]
dataSet1.coGroup(dataSet2)
        .where(value -> value.f0)
        .equalTo(value -> value.f0)
        .with(new CoGroupFunction<>(){
        // implement user-defined co group logic
        });
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Tuple2<>> dataStream1 = // [...]
DataStream<Tuple2<>> dataStream2 = // [...]
dataStream1.coGroup(dataStream2)
           .where(value -> value.f0)
           .equalTo(value -> value.f0)
           .window(EndOfStreamWindows.get()))
           .apply(new CoGroupFunction<>(){
           // implement user-defined co group logic
           });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>OuterJoin</td>
            <td>
                {{< highlight "java" >}}
DataSet<Tuple2<>> dataSet1 = // [...]
DataSet<Tuple2<>> dataSet2 = // [...]
// left outer join
dataSet1.leftOuterJoin(dataSet2)
        .where(dataSet1.f0)
        .equalTo(dataSet2.f0)
        .with(new JoinFunction<>(){
        // implement user-defined left outer join logic
        });
// right outer join
dataSet1.rightOuterJoin(dataSet2)
        .where(dataSet1.f0)
        .equalTo(dataSet2.f0)
        .with(new JoinFunction<>(){
        // implement user-defined right outer join logic
        });
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
    DataStream<Tuple2<>> dataStream1 = // [...]
    DataStream<Tuple2<>> dataStream2 = // [...]
    // left outer join
    dataStream1.coGroup(dataStream2)
               .where(value -> value.f0)
               .equalTo(value -> value.f0)
               .window(EndOfStreamWindows.get())
               .apply((leftIterable, rightInterable, collector) -> {
                    if(!rightInterable.iterator().hasNext()){
                    // implement user-defined left outer join logic
                    }
                });
    // right outer join
    dataStream1.coGroup(dataStream2)
               .where(value -> value.f0)
               .equalTo(value -> value.f0)
               .window(EndOfStreamWindows.get())
               .apply((leftIterable, rightInterable, collector) -> {
                    if(!leftIterable.iterator().hasNext()){
                    // implement user-defined right outer join logic
                    }
                });
                {{< /highlight >}}
            </td>
        </tr>
    </tbody>
</table>

### Category 3

For category 3, the behavior of these DataSet APIs can be achieved by other APIs with different semantics in DataStream, with potentially additional cost in execution efficiency.

Currently, DataStream API does not directly support aggregations on non-keyed streams (subtask-scope aggregations). In order to do so, we need to first assign the subtask id 
to the records, then turn the stream into a keyed stream. The `AddSubtaskIdMapFunction` in the [Appendix]({{< ref "docs/dev/datastream/dataset_migration#addsubtaskidmapfunction" >}}) shows how 
to do that, and we will reuse it in the rest of this document.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">Operations</th>
            <th class="text-left">DataSet</th>
            <th class="text-left">DataStream</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>MapPartition/SortPartition</td>
            <td>
                {{< highlight "java" >}}
DataSet<Integer> dataSet = // [...]
// MapPartition
dataSet.mapPartition(new MapPartitionFunction<>(){
        // implement user-defined map partition logic
        });
// SortPartition
dataSet.sortPartition(0, Order.ASCENDING);
dataSet.sortPartition(0, Order.DESCENDING);
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
DataStream<Integer> dataStream = // [...]
// assign subtask ID to all records
DataStream<Tuple2<String, Integer>> dataStream1 = dataStream.map(new AddSubtaskIDMapFunction());
dataStream1.keyBy(value -> value.f0)
           .window(EndOfStreamWindows.get())
           .apply(new WindowFunction<>(){
           // implement user-defined map partition or sort partition logic
           });
                {{< /highlight >}}
            </td>
        </tr>
        <tr>
            <td>Cross</td>
            <td>
                {{< highlight "java" >}}
DataSet<Integer> dataSet1 = // [...]
DataSet<Integer> dataSet2 = // [...]
// Cross
dataSet1.cross(dataSet2)
        .with(new CrossFunction<>(){
        // implement user-defined cross logic
        })
                {{< /highlight >}}
            </td>
            <td>
                {{< highlight "java" >}}
// the parallelism of dataStream1 and dataStream2 should be same
DataStream<Integer> dataStream1 = // [...]
DataStream<Integer> dataStream2 = // [...]
DataStream<Tuple2<String, Integer>> datastream3 = dataStream1.broadcast().map(new AddSubtaskIDMapFunction());
DataStream<Tuple2<String, Integer>> datastream4 = dataStream2.map(new AddSubtaskIDMapFunction());
// join the two streams according to the subtask ID
dataStream3.join(dataStream4)
           .where(value -> value.f0)
           .equalTo(value -> value.f0)
           .window(EndOfStreamWindows.get())
           .apply(new JoinFunction<>(){
           // implement user-defined cross logic
           })
                {{< /highlight >}}
            </td>
        </tr>
    </tbody>
</table>

### Category 4

The behaviors of the following DataSet APIs are not supported by DataStream.

* RangePartition
* GroupCombine


## Appendix

#### EndOfStreamWindows

The following code shows the example of `EndOfStreamWindows`.

```java
public class EndOfStreamWindows extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private static final EndOfStreamWindows INSTANCE = new EndOfStreamWindows();

    private static final TimeWindow TIME_WINDOW_INSTANCE =
            new TimeWindow(Long.MIN_VALUE, Long.MAX_VALUE);

    private EndOfStreamWindows() {}

    public static EndOfStreamWindows get() {
        return INSTANCE;
    }

    @Override
    public Collection<TimeWindow> assignWindows(
            Object element, long timestamp, WindowAssignerContext context) {
        return Collections.singletonList(TIME_WINDOW_INSTANCE);
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new EndOfStreamTrigger();
    }

    @Override
    public String toString() {
        return "EndOfStreamWindows()";
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
    
    @Internal
    public static class EndOfStreamTrigger extends Trigger<Object, TimeWindow> {
        @Override
        public TriggerResult onElement(
                Object element, long timestamp, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }
    }
}
```

#### AddSubtaskIDMapFunction

The following code shows the example of `AddSubtaskIDMapFunction`.
```java
public static class AddSubtaskIDMapFunction<T> extends RichMapFunction<T, Tuple2<String, T>> {
    @Override
    public Tuple2<String, T> map(T value) {
        return Tuple2.of(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()), value);
    }
}
```

{{< top >}}
