---
title: "Broadcast State 模式"
weight: 3
type: docs
aliases:
  - /zh/dev/stream/state/broadcast_state.html
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

# Broadcast State 模式

你将在本节中了解到如何实际使用 broadcast state。想了解更多有状态流处理的概念，请参考
[Stateful Stream Processing]({{< ref "docs/concepts/stateful-stream-processing" >}})。

## 提供的 API

在这里我们使用一个例子来展现 broadcast state 提供的接口。假设存在一个序列，序列中的元素是具有不同颜色与形状的图形，我们希望在序列里相同颜色的图形中寻找满足一定顺序模式的图形对（比如在红色的图形里，有一个长方形跟着一个三角形）。
同时，我们希望寻找的模式也会随着时间而改变。

在这个例子中，我们定义两个流，一个流包含`图形（Item）`，具有`颜色`和`形状`两个属性。另一个流包含特定的`规则（Rule）`，代表希望寻找的模式。

在`图形`流中，我们需要首先使用`颜色`将流进行进行分区（keyBy），这能确保相同颜色的图形会流转到相同的物理机上。

```java
// 将图形使用颜色进行划分
KeyedStream<Item, Color> colorPartitionedStream = itemStream
                        .keyBy(new KeySelector<Item, Color>(){...});
```

对于`规则`流，它应该被广播到所有的下游 task 中，下游 task 应当存储这些规则并根据它寻找满足规则的图形对。下面这段代码会完成：
i)  将`规则`广播给所有下游 task；
ii) 使用 `MapStateDescriptor` 来描述并创建 broadcast state 在下游的存储结构

```java

// 一个 map descriptor，它描述了用于存储规则名称与规则本身的 map 存储结构
MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
			"RulesBroadcastState",
			BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(new TypeHint<Rule>() {}));
		
// 广播流，广播规则并且创建 broadcast state
BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                        .broadcast(ruleStateDescriptor);
```

最终，为了使用`规则`来筛选`图形`序列，我们需要：
 1. 将两个流关联起来
 2. 完成我们的模式识别逻辑

为了关联一个非广播流（keyed 或者 non-keyed）与一个广播流（`BroadcastStream`），我们可以调用非广播流的方法 `connect()`，并将 `BroadcastStream` 当做参数传入。
这个方法的返回参数是 `BroadcastConnectedStream`，具有类型方法 `process()`，传入一个特殊的 `CoProcessFunction` 来书写我们的模式识别逻辑。
具体传入 `process()` 的是哪个类型取决于非广播流的类型：
 - 如果流是一个 **keyed** 流，那就是 `KeyedBroadcastProcessFunction` 类型；
 - 如果流是一个 **non-keyed** 流，那就是 `BroadcastProcessFunction` 类型。

在我们的例子中，`图形`流是一个 keyed stream，所以我们书写的代码如下：

{{< hint warning >}}
`connect()` 方法需要由非广播流来进行调用，`BroadcastStream` 作为参数传入。
{{< /hint >}}

```java
DataStream<String> output = colorPartitionedStream
                 .connect(ruleBroadcastStream)
                 .process(
                     
                     // KeyedBroadcastProcessFunction 中的类型参数表示：
                     //   1. key stream 中的 key 类型
                     //   2. 非广播流中的元素类型
                     //   3. 广播流中的元素类型
                     //   4. 结果的类型，在这里是 string
                     
                     new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {
                         // 模式匹配逻辑
                     }
                 );
```

### BroadcastProcessFunction 和 KeyedBroadcastProcessFunction

在传入的 `BroadcastProcessFunction` 或 `KeyedBroadcastProcessFunction` 中，我们需要实现两个方法。`processBroadcastElement()` 方法负责处理广播流中的元素，`processElement()` 负责处理非广播流中的元素。
两个子类型定义如下：

```java
public abstract class BroadcastProcessFunction<IN1, IN2, OUT> extends BaseBroadcastProcessFunction {

    public abstract void processElement(IN1 value, ReadOnlyContext ctx, Collector<OUT> out) throws Exception;

    public abstract void processBroadcastElement(IN2 value, Context ctx, Collector<OUT> out) throws Exception;
}
```

```java
public abstract class KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> {

    public abstract void processElement(IN1 value, ReadOnlyContext ctx, Collector<OUT> out) throws Exception;

    public abstract void processBroadcastElement(IN2 value, Context ctx, Collector<OUT> out) throws Exception;

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception;
}
```

需要注意的是 `processBroadcastElement()` 负责处理广播流的元素，而 `processElement()` 负责处理另一个流的元素。两个方法的第二个参数(Context)不同，均有以下方法：
 1. 得到广播流的存储状态：`ctx.getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)`
 2. 查询元素的时间戳：`ctx.timestamp()`
 3. 查询目前的Watermark：`ctx.currentWatermark()`
 4. 目前的处理时间(processing time)：`ctx.currentProcessingTime()`
 5. 产生旁路输出：`ctx.output(OutputTag<X> outputTag, X value)`

在 `getBroadcastState()` 方法中传入的 `stateDescriptor` 应该与调用 `.broadcast(ruleStateDescriptor)` 的参数相同。

这两个方法的区别在于对 broadcast state 的访问权限不同。在处理广播流元素这端，是**具有读写权限的**，而对于处理非广播流元素这端是**只读**的。
这样做的原因是，Flink 中是不存在跨 task 通讯的。所以为了保证 broadcast state 在所有的并发实例中是一致的，我们在处理广播流元素的时候给予写权限，在所有的 task 中均可以看到这些元素，并且要求对这些元素处理是一致的，
那么最终所有 task 得到的 broadcast state 是一致的。

{{< hint warning >}}
`processBroadcastElement()` 的实现必须在所有的并发实例中具有确定性的结果。
{{< /hint >}}

同时，`KeyedBroadcastProcessFunction` 在 Keyed Stream 上工作，所以它提供了一些 `BroadcastProcessFunction` 没有的功能:
 1. `processElement()` 的参数 `ReadOnlyContext` 提供了方法能够访问 Flink 的定时器服务，可以注册事件定时器(event-time timer)或者处理时间的定时器(processing-time timer)。当定时器触发时，会调用 `onTimer()` 方法，
 提供了 `OnTimerContext`，它具有 `ReadOnlyContext` 的全部功能，并且提供：
  - 查询当前触发的是一个事件还是处理时间的定时器
  - 查询定时器关联的key
 2. `processBroadcastElement()` 方法中的参数 `Context` 会提供方法 `applyToKeyedState(StateDescriptor<S, VS> stateDescriptor, KeyedStateFunction<KS, S> function)`。
 这个方法使用一个 `KeyedStateFunction` 能够对 `stateDescriptor` 对应的 state 中**所有 key 的存储状态**进行某些操作。

{{< hint warning >}}
注册一个定时器只能在 `KeyedBroadcastProcessFunction` 的 `processElement()` 方法中进行。
  在 `processBroadcastElement()` 方法中不能注册定时器，因为广播的元素中并没有关联的 key。
{{< /hint >}}

回到我们当前的例子中，`KeyedBroadcastProcessFunction` 应该实现如下：

```java
new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {

    // 存储部分匹配的结果，即匹配了一个元素，正在等待第二个元素
    // 我们用一个数组来存储，因为同时可能有很多第一个元素正在等待
    private final MapStateDescriptor<String, List<Item>> mapStateDesc =
        new MapStateDescriptor<>(
            "items",
            BasicTypeInfo.STRING_TYPE_INFO,
            new ListTypeInfo<>(Item.class));

    // 与之前的 ruleStateDescriptor 相同
    private final MapStateDescriptor<String, Rule> ruleStateDescriptor = 
        new MapStateDescriptor<>(
            "RulesBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Rule>() {}));

    @Override
    public void processBroadcastElement(Rule value,
                                        Context ctx,
                                        Collector<String> out) throws Exception {
        ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
    }

    @Override
    public void processElement(Item value,
                               ReadOnlyContext ctx,
                               Collector<String> out) throws Exception {

        final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc);
        final Shape shape = value.getShape();
    
        for (Map.Entry<String, Rule> entry :
                ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
            final String ruleName = entry.getKey();
            final Rule rule = entry.getValue();
    
            List<Item> stored = state.get(ruleName);
            if (stored == null) {
                stored = new ArrayList<>();
            }
    
            if (shape == rule.second && !stored.isEmpty()) {
                for (Item i : stored) {
                    out.collect("MATCH: " + i + " - " + value);
                }
                stored.clear();
            }
    
            // 不需要额外的 else{} 段来考虑 rule.first == rule.second 的情况
            if (shape.equals(rule.first)) {
                stored.add(value);
            }
    
            if (stored.isEmpty()) {
                state.remove(ruleName);
            } else {
                state.put(ruleName, stored);
            }
        }
    }
}
```

## 重要注意事项

这里有一些 broadcast state 的重要注意事项，在使用它时需要时刻清楚：

  - **没有跨 task 通讯：**如上所述，这就是为什么**只有**在 `(Keyed)-BroadcastProcessFunction` 中处理广播流元素的方法里可以更改 broadcast state 的内容。
  同时，用户需要保证所有 task 对于 broadcast state 的处理方式是一致的，否则会造成不同 task 读取 broadcast state 时内容不一致的情况，最终导致结果不一致。

  - **broadcast state 在不同的 task 的事件顺序可能是不同的：**虽然广播流中元素的过程能够保证所有的下游 task 全部能够收到，但在不同 task 中元素的到达顺序可能不同。
  所以 broadcast state 的更新*不能依赖于流中元素到达的顺序*。

  - **所有的 task 均会对 broadcast state 进行 checkpoint：**虽然所有 task 中的 broadcast state 是一致的，但当 checkpoint 来临时所有 task 均会对 broadcast state 做 checkpoint。
  这个设计是为了防止在作业恢复后读文件造成的文件热点。当然这种方式会造成 checkpoint 一定程度的写放大，放大倍数为 p（=并行度）。Flink 会保证在恢复状态/改变并发的时候数据**没有重复**且**没有缺失**。
  在作业恢复时，如果与之前具有相同或更小的并发度，所有的 task 读取之前已经 checkpoint 过的 state。在增大并发的情况下，task 会读取本身的 state，多出来的并发（`p_new` - `p_old`）会使用轮询调度算法读取之前 task 的 state。

  - **不使用 RocksDB state backend：** broadcast state 在运行时保存在内存中，需要保证内存充足。这一特性同样适用于所有其他 Operator State。

{{< top >}}
