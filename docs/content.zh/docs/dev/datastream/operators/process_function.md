---
title: "Process Function"
weight: 4 
type: docs
aliases:
  - /zh/dev/stream/operators/process_function.html
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

# Process Function

## ProcessFunction

`ProcessFunction` 是一种底层的流处理操作，基于它用户可以访问（无环）流应用程序的所有基本构建块：

 - 事件（流元素）
 - 状态（容错，一致性，仅在 keyed stream 上）
 - 定时器（事件时间和处理时间，仅在 keyed stream 上）

可以将 `ProcessFunction` 视为一种可以访问 keyed state 和定时器的 `FlatMapFunction`。Flink 为收到的输入流中的每个事件都调用该函数来进行处理。

对于容错，与其它有状态的函数类似，`ProcessFunction` 可以通过 `RuntimeContext` 访问 Flink 的 [keyed state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}})。

定时器允许应用程序对处理时间和 [事件时间]({{< ref "docs/concepts/time" >}}) 中的更改做出反应。
每次调用 `processElement(...)` 时参数中都会提供一个 `Context` 对象，该对象可以访问元素的事件时间戳和 *TimerService*。
`TimerService` 可用于为将来特定的事件时间/处理时间注册回调。
特定事件时间的 `onTimer(...)` 回调函数会在当前对齐的 watermark 超过所注册的时间戳时调用。
特定处理时间的 `onTimer(...)` 回调函数则会在系统物理时间超过所注册的时间戳时调用。
在该调用期间，所有状态会被再次绑定到创建定时器时的键上，从而允许定时器操作与之对应的 keyed state。

{{< hint info >}}
如果想要访问 keyed state 和定时器，需要在
keyed stream 上使用 `ProcessFunction`。
{{< /hint >}}

```java
stream.keyBy(...).process(new MyProcessFunction());
```

## 底层 Join

为了在两个输入上实现底层操作，应用程序可以使用 `CoProcessFunction` 或 `KeyedCoProcessFunction`。
这些函数绑定两个不同的输入，从两个不同的输入中获取元素并分别调用
`processElement1(...)` 和 `processElement2(...)` 进行处理。

实现底层 join 一般需要遵循以下模式：

  - 为一个输入（或两者）创建状态对象。
  - 从某个输入接收元素时更新状态。
  - 从另一个输入接收元素时，查询状态并生成 join 结果。

例如，你可能会将客户数据与金融交易进行 join，同时想要保留客户数据的状态。如果你希望即使在出现乱序事件时仍然可以得到完整且确定的 join 结果，你可以通过注册一个定时器在客户数据流的 watermark 已经超过当前这条金融交易记录时计算和发送 join 结果。

## 示例

在下面的例子中，`KeyedProcessFunction` 维护每个键的计数，并且每次超过一分钟（事件时间）没有更新时输出一次键/计数对。

  - 计数，键和最后修改时间存储在 `ValueState` 中，它由键隐式限定范围。
  - 对于每条记录，`KeyedProcessFunction` 递增计数器并设置最后修改时间。
  - 对于每条记录，该函数还会注册了一个一分钟后（事件时间）的回调函数。
  - 在每次回调时，它会根据注册的时间和最后修改时间进行比较，如果正好差一分钟则
    输出键/计数对（即，在该分钟内没有进一步更新）

{{< hint info >}}
这个简单的例子本身可以用会话窗口（session window）实现，
这里我们使用 `KeyedProcessFunction` 来展示使用它的基本模式。
{{< /hint >}}

{{< tabs "6c8c009c-4c12-4338-9eeb-3be83cfa9e37" >}}
{{< tab "Java" >}}

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;


// 源数据流
DataStream<Tuple2<String, String>> stream = ...;

// 使用 process function 来处理一个 Keyed Stream 
DataStream<Tuple2<String, Long>> result = stream
    .keyBy(value -> value.f0)
    .process(new CountWithTimeoutFunction());

/**
 * 在状态中保存的数据类型
 */
public class CountWithTimestamp {

    public String key;
    public long count;
    public long lastModified;
}

/**
 * 用来维护数量和超时的 ProcessFunction 实现
 */
public class CountWithTimeoutFunction 
        extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

    /** 由 process function 管理的状态 */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(OpenContext openContext) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            Tuple2<String, String> value, 
            Context ctx, 
            Collector<Tuple2<String, Long>> out) throws Exception {

        // 获得当前的数量
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // 更新状态中的数量
        current.count++;

        // 将状态中的最后修改时间改为记录的事件时间
        current.lastModified = ctx.timestamp();

        // 将更新后的状态写回
        state.update(current);

        // 注册一个 60s 之后的事件时间回调 
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(
            long timestamp, 
            OnTimerContext ctx, 
            Collector<Tuple2<String, Long>> out) throws Exception {

        // 获得注册该回调时使用的键对应的状态
        CountWithTimestamp result = state.value();

        // 检查当前回调时否是最新的回调还是后续注册了新的回调
        if (timestamp == result.lastModified + 60000) {
            // 超时后发送状态
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
import datetime

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment


class CountWithTimeoutFunction(KeyedProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(ValueStateDescriptor(
            "my_state", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # 获取当前计数
        current = self.state.value()
        if current is None:
            current = Row(value.f1, 0, 0)

        # 更新该状态的计数
        current[1] += 1

        # 把状态的时间戳设置为记录指定的事件时间戳
        current[2] = ctx.timestamp()

        # 将更新后的状态写回
        self.state.update(current)

        # 注册一个 60s 之后的事件时间回调
        ctx.timer_service().register_event_time_timer(current[2] + 60000)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        # 获取调度定时器的键状态
        result = self.state.value()

        # 检查这是一个过时的定时器还是最新的定时器
        if timestamp == result[2] + 60000:
            # emit the state on timeout
            yield result[0], result[1]


class MyTimestampAssigner(TimestampAssigner):

    def __init__(self):
        self.epoch = datetime.datetime.utcfromtimestamp(0)

    def extract_timestamp(self, value, record_timestamp) -> int:
        return int((value[0] - self.epoch).total_seconds() * 1000)


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql("""
            CREATE TABLE my_source (
              a TIMESTAMP(3),
              b VARCHAR,
              c VARCHAR
            ) WITH (
              'connector' = 'datagen',
              'rows-per-second' = '10'
            )
        """)

    stream = t_env.to_append_stream(
        t_env.from_path('my_source'),
        Types.ROW([Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING()]))
    watermarked_stream = stream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps()
                         .with_timestamp_assigner(MyTimestampAssigner()))

    # 将 process function 应用于 keyed stream 
    result = watermarked_stream.key_by(lambda value: value[1]) \
                               .process(CountWithTimeoutFunction()) \
                               .print()
    env.execute()
```
{{< /tab >}}
{{< /tabs >}}


{{< hint warning >}}
在 Flink 1.4.0 之前，在调用处理时间定时器时，`ProcessFunction.onTimer()` 方法将当前的处理时间设置为事件时间的时间戳。此行为非常不明显，用户可能不会注意到。
然而，这样做是有害的，因为处理时间的时间戳是不确定的，并且和 watermark 不一致。此外，用户依赖于此错误的时间戳来实现逻辑很有可能导致非预期的错误。
因此，我们决定对其进行修复。在 1.4.0 后，使用此错误的事件时间时间戳的 Flink 作业将失败，用户应将其作业更正为正确的逻辑。
{{< /hint >}}

## KeyedProcessFunction

`KeyedProcessFunction` 是 `ProcessFunction` 的一个扩展, 可以在其 `onTimer(...)` 方法中访问定时器的键。

{{< tabs "f8b6791f-023f-4e56-a6e4-8541dd0b3e1b" >}}
{{< tab "Java" >}}
```java
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
    K key = ctx.getCurrentKey();
    // ...
}

```
{{< /tab >}}
{{< tab "Python" >}}
```python
def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
    key = ctx.get_current_key()
    # ...
```
{{< /tab >}}
{{< /tabs >}}

## Timers

两种定时器（处理时间定时器和事件时间定时器）都在 `TimerService` 内部维护，并排队等待执行。

对于相同的键和时间戳，`TimerService` 会删除重复的定时器，即每个键和时间戳最多有一个定时器。如果为同一时间戳注册了多个定时器，则只调用一次 `onTimer()` 方法。

Flink 会同步 `onTimer()` 和 `processElement()` 的调用，因此用户不必担心状态的并发修改。

### Fault Tolerance

定时器支持容错，它会和应用程序的状态一起进行 checkpoint。当进行故障恢复或从保存点启动应用程序时，定时器也会被恢复。

{{< hint info >}}
当应用程序从故障中恢复或从保存点启动时，可能会发生这种情况。即：在恢复之前就应该触发的处理时间定时器会立即触发。
{{< /hint >}}

{{< hint info >}}
除了使用基于 RocksDB backend 的增量 snapshots 并使用基于 Heap 的定时器的情况外，Flink 总是会异步执行计算器的快照操作（前者将会在 `FLINK-10026` 解决）。
大量定时器会增加 checkpoint 的时间，因为定时器是需要 checkpoint 的状态的一部分。有关如何减少定时器数量，请参阅“Timer Coalescing”部分。
{{< /hint >}}

### Timer Coalescing

由于 Flink 中每个键和时间戳只保存一个定时器，因此可以通过降低定时器的精度来合并它们，从而减少定时器的数量。

对于精度为 1 秒（事件或处理时间）的定时器，可以将目标时间向下舍入为整秒。定时器最多会提前 1 秒，但不迟于要求的毫秒精度。
这样，每个键在每秒内最多有一个定时器。

{{< tabs "aa23eeb6-d15f-44f2-85ab-d130a4202d57" >}}
{{< tab "Java" >}}
```java
long coalescedTime = ((ctx.timestamp() + timeout) / 1000) * 1000;
ctx.timerService().registerProcessingTimeTimer(coalescedTime);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
coalesced_time = ((ctx.timestamp() + timeout) // 1000) * 1000
ctx.timer_service().register_processing_time_timer(coalesced_time)
```
{{< /tab >}}
{{< /tabs >}}

由于事件时间定时器仅在 watermark 到来时才触发，因此还可以将下一个 watermark 到达前的定时器与当前定时器合并：

{{< tabs "ef74a1da-c4cd-4fab-8035-d29ffd7039d4" >}}
{{< tab "Java" >}}
```java
long coalescedTime = ctx.timerService().currentWatermark() + 1;
ctx.timerService().registerEventTimeTimer(coalescedTime);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
coalesced_time = ctx.timer_service().current_watermark() + 1
ctx.timer_service().register_event_time_timer(coalesced_time)
```
{{< /tab >}}
{{< /tabs >}}

定时器也可以按照以下方式被停止或者删除：

停止处理时间定时器：

{{< tabs "5d0d1344-6f51-44f8-b500-ebe863cedba4" >}}
{{< tab "Java" >}}
```java
long timestampOfTimerToStop = ...;
ctx.timerService().deleteProcessingTimeTimer(timestampOfTimerToStop);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
timestamp_of_timer_to_stop = ...
ctx.timer_service().delete_processing_time_timer(timestamp_of_timer_to_stop)
```
{{< /tab >}}
{{< /tabs >}}

停止事件时间定时器：

{{< tabs "581e5996-503c-452e-8b2a-a4daeaf4ac88" >}}
{{< tab "Java" >}}
```java
long timestampOfTimerToStop = ...;
ctx.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
timestamp_of_timer_to_stop = ...
ctx.timer_service().delete_event_time_timer(timestamp_of_timer_to_stop)
```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
如果没有注册给定时间戳的定时器，则停止定时器不会产生影响。
{{< /hint >}}

{{< top >}}
