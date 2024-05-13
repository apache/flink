---
title: State Processor API
weight: 3
type: docs
aliases:
  - /zh/dev/libs/state_processor_api.html
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


# 状态处理器API

Apache Flink的状态处理器API供了强大的功能，可在`批` 处理下使用Flink的DataStream API读取、写入和修改savepoint和checkpoint。
由于 [DataStream和Table API的互操作性]({{< ref "docs/dev/table/data_stream_api" >}})，您甚至可以使用关系型Table API或SQL查询来分析和处理状态数据。

例如，您可以对正在运行的流处理应用程序建立savepoint，并使用DataStream批处理程序对其进行分析，以验证应用程序的正确性。
或者，您可以从任何存储区读取一个批数据，对其进行预处理，并将结果写入一个savepoint，用于引导流处理的状态
还可以修复不一致的状态条目。
最后，状态处理器API为改进之前受到参数和设计选择限制的有状态应用程序创造了很多方法，此前这些限制在启动后不能更改，否则将丢失应用程序的所有状态。
例如，现在可以任意修改状态的数据类型，调整操作符的最大并行度，拆分或合并操作符状态，重新分配操作符UID等等。

要开始使用状态处理器API，请把以下库添加到您的应用程序中包含。

{{< artifact flink-state-processor-api >}}

## 将应用程序状态映射到数据集

状态处理器API将流处理的状态映射到一个或多个可以分别处理的数据集。
为了能够使用该API，您需要了解这种映射是如何工作的。

让我们先看看有状态的Flink作业是什么样子。
一个Flink作业由运算符组成; 通常包括一个或多个源运算符，一些用于实际处理的运算符，以及一个或多个汇聚运算符。
每个运算符以一个或多个任务的方式并行运行，并且可以使用不同类型的状态进行操作。
一个运算符可以具有零个、一个或多个 *“运算符状态”* ，这些状态被组织为列表，其范围限定在运算符的任务中。
如果该运算符应用于分区流，它还可以具有零个、一个或多个 *“分区状态”* ，这些状态的范围限定在从每个处理记录中提取的密钥中。
您可以将分区状态视为分布式键值映射。

下图显示了名为“MyApp”的应用程序，其中包括三个运算符，分别称为“Src”、“Proc”和“Snk”。
Src具有一个运算符状态（os1），Proc具有一个运算符状态（os2）和两个分区状态（ks1, ks2），而Snk没有状态。

{{< img src="/fig/application-my-app-state-processor-api.png" width="600px" alt="Application: MyApp" >}}

MyApp的savepoint或checkpoint包含所有状态的数据，用一种每个任务的状态都可以被恢复的方式搭建起来。
在使用批处理作业处理savepoint（或checkpoint）的数据时，我们需要一种将各个任务状态的数据映射为数据集或表的思维模型。
事实上，我们可以将savepoint视为数据库。每个运算符（通过其UID标识）表示一个命名空间。
每个运算符的运算符状态映射到命名空间中的一个专用表，该表具有一个列，其中包含所有任务的状态数据。
每个运算符的分区状态都映射到一个单独的表，其中包括一个键列和每个分区状态的一个列。
下图显示了MyApp的savepoint如何映射到数据库。

{{< img src="/fig/database-my-app-state-processor-api.png" width="600px" alt="Database: MyApp" >}}

该图显示了如何将Src的运算符状态值映射到具有一列和五行的表中，每行对应Src所有并行任务中每个列表条目。
同样，Proc的运算符状态os2被映射到一个单独的表中。
分区状态ks1和ks2被合并到一个具有三列的单个表中，分别对应键、ks1和ks2。
分区表中包含了每个不同键的分区状态。
由于运算符“Snk”没有任何状态，因此其命名空间为空。

## 识别运算符

状态处理器API 可以让您用 [UIDs]({{< ref "docs/concepts/glossary" >}}#UID) 或 [UID 哈希]({{< ref "docs/concepts/glossary" >}}#UID-hashes) 基于 `OperatorIdentifier#forUid/forUidHash`来识别运算符。
哈希只有在 `UIDs` 不可用用时才能使用, 例如，当创建 [savepoint]({{< ref "docs/ops/state/savepoints" >}}) 的应用程序未不明确或 `UID`未知。

## 读取状态

读取状态的第一步是指定一个有效savepoint或checkpoint的路径以及用来恢复数据的`StateBackend` 。
恢复状态的兼容性保证与恢复 `DataStream` 应用程序保持一致。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
SavepointReader savepoint = SavepointReader.read(env, "hdfs://path/", new HashMapStateBackend());
```


### 运算符状态

[运算符状态]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#operator-state) 是Flink中的所有非分区状态.
这包括但不限于应用程序中对 `CheckpointedFunction` 或 `BroadcastState` 的任何使用。
当读取运算符状态时，用户需要指定运算符UID、状态名称和类型信息。

#### 运算符列表状态

用`getListState` 存储在`CheckpointedFunction` 的运算符状态可以用 `ExistingSavepoint#readListState`来读取。
该状态名称和类型信息应与在DataStream应用程序中宣布此状态的 `ListStateDescriptor` 中使用的名称和类型信息相匹配。

```java
DataStream<Integer> listState  = savepoint.readListState<>(
    OperatorIdentifier.forUid("my-uid"),
    "list-state",
    Types.INT);
```

#### 运算符联合列表状态

用 `getUnionListState` 存储在 `CheckpointedFunction` 的运算符状态可以用 `ExistingSavepoint#readUnionState`来读取。
该状态名称和类型信息应与在DataStream应用程序中宣布此状态的 `ListStateDescriptor` 中使用的名称和类型信息相匹配。
该框架将返回状态的 _单_ 副本 ，相当于还原具有并行度为 1 的 DataStream。

```java
DataStream<Integer> listState  = savepoint.readUnionState<>(
    OperatorIdentifier.forUid("my-uid"),
    "union-state",
    Types.INT);
```

#### 广播状态

[广播状态]({{< ref "docs/dev/datastream/fault-tolerance/broadcast_state" >}}) 可以用 `ExistingSavepoint#readBroadcastState` 来读取.
该状态名称和类型信息应与在DataStream应用程序中宣布此状态的 `MapStateDescriptor` 中使用的名称和类型信息相匹配。
该框架将返回状态的 _单_ 副本 ，相当于还原具有并行度为 1 的 DataStream。

```java
DataStream<Tuple2<Integer, Integer>> broadcastState = savepoint.readBroadcastState<>(
    OperatorIdentifier.forUid("my-uid"),
    "broadcast-state",
    Types.INT,
    Types.INT);
```

#### 使用自定义序列化器

如果一个运算符状态读取器在定义写出状态的`StateDescriptor` 时使用了自定义 `TypeSerializers` 那么这个运算符状态读取器一定支持使用自定义 `TypeSerializers` 。

```java
DataStream<Integer> listState = savepoint.readListState<>(
    OperatorIdentifier.forUid("uid"),
    "list-state", 
    Types.INT,
    new MyCustomIntSerializer());
```

### 分区状态

[分区状态]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-state), 是所有相对于键进行分区的状态。
在读取分区状态时，用户需要指定运算符ID和 `KeyedStateReaderFunction<KeyType, OutputType>`。

`KeyedStateReaderFunction` 让用户可以读取任意列和复杂的状态类型，例如ListState、MapState和AggregatingState。
这意味着，如果运算符包含一个具有状态的处理函数，例如：

```java
public class StatefulFunctionWithTime extends KeyedProcessFunction<Integer, Integer, Void> {
 
   ValueState<Integer> state;
 
   ListState<Long> updateTimes;

   @Override
   public void open(OpenContext openContext) {
      ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
      state = getRuntimeContext().getState(stateDescriptor);

      ListStateDescriptor<Long> updateDescriptor = new ListStateDescriptor<>("times", Types.LONG);
      updateTimes = getRuntimeContext().getListState(updateDescriptor);
   }
 
   @Override
   public void processElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
      state.update(value + 1);
      updateTimes.add(System.currentTimeMillis());
   }
}
```

那么就可以通过定义输出类型和相应的`KeyedStateReaderFunction`来进行读取。
```java
DataStream<KeyedState> keyedState = savepoint.readKeyedState(OperatorIdentifier.forUid("my-uid"), new ReaderFunction());

public class KeyedState {
  public int key;

  public int value;

  public List<Long> times;
}
 
public class ReaderFunction extends KeyedStateReaderFunction<Integer, KeyedState> {

  ValueState<Integer> state;
 
  ListState<Long> updateTimes;

  @Override
  public void open(OpenContext openContext) {
    ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
    state = getRuntimeContext().getState(stateDescriptor);

    ListStateDescriptor<Long> updateDescriptor = new ListStateDescriptor<>("times", Types.LONG);
    updateTimes = getRuntimeContext().getListState(updateDescriptor);
  }
 
  @Override
  public void readKey(
    Integer key,
    Context ctx,
    Collector<KeyedState> out) throws Exception {
        
    KeyedState data = new KeyedState();
    data.key    = key;
    data.value  = state.value();
    data.times  = StreamSupport
      .stream(updateTimes.get().spliterator(), false)
      .collect(Collectors.toList());

    out.collect(data);
  }
}
```

除了读取已注册的状态值外， 每个键还可以访问一个包含元数据的 `内容` ，例如已注册的事件时间和处理时间定时器。

**注意:** 在使用`KeyedStateReaderFunction`时, 所有状态描述符都必须在open方法内部及早注册。 任何调用`RuntimeContext#get*State` 的尝试都会导致`运行时异常`.

### 窗口状态

状态处理器API支持从 [窗口运算符]({{< ref "docs/dev/datastream/operators/windows" >}}) 中读取状态。
在读取窗口状态时，用户需要指定运算符ID、窗口分配器和聚合类型。

此外，可以指定 `WindowReaderFunction` 来为每次读取提供类似于 `WindowFunction` 或 `ProcessWindowFunction` 的附加信息。

就像有一个DataStream应用程序，用于计算每分钟每个用户的点击次数。

```java
class Click {
    public String userId;

    public LocalDateTime time;    
}

class ClickCounter implements AggregateFunction<Click, Integer, Integer> {

	@Override
	public Integer createAccumulator() {
		return 0;
	}

	@Override
	public Integer add(Click value, Integer accumulator) {
		return 1 + accumulator;
	}

	@Override
	public Integer getResult(Integer accumulator) {
		return accumulator;
	}

	@Override
	public Integer merge(Integer a, Integer b) {
		return a + b;
	}
}

DataStream<Click> clicks = ...;

clicks
    .keyBy(click -> click.userId)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new ClickCounter())
    .uid("click-window")
    .addSink(new Sink());

```

这一状态可以用以下代码读取。

```java

class ClickState {
    
    public String userId;

    public int count;

    public TimeWindow window;

    public Set<Long> triggerTimers;
}

class ClickReader extends WindowReaderFunction<Integer, ClickState, String, TimeWindow> { 

	@Override
	public void readWindow(
            String key,
            Context<TimeWindow> context,
            Iterable<Integer> elements,
            Collector<ClickState> out) {
		ClickState state = new ClickState();
		state.userId = key;
		state.count = elements.iterator().next();
		state.window = context.window();
		state.triggerTimers = context.registeredEventTimeTimers();
		
		out.collect(state);
	}
}

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
SavepointReader savepoint = SavepointReader.read(env, "hdfs://checkpoint-dir", new HashMapStateBackend());

savepoint
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate("click-window", new ClickCounter(), new ClickReader(), Types.String, Types.INT, Types.INT)
    .print();

```

此外， 来自`CountTrigger` 的触发器状态或自定义的触发器可以用
`WindowReaderFunction` 内的 `Context#triggerState` 方法来读取。

## 写入新的Savepoints

`Savepoint`' 也可以被写入，这样可以实现根据历史数据引导状态的用例。
每个savepoint由一个或多个 `StateBootstrapTransformation` 组成（下文有解释），其中每个StateBootstrapTransformation都定义了单个运算符的状态。

{{< hint info >}}
使用 `SavepointWriter` 时, 您的应用程序必须在 [批]({{< ref "docs/dev/datastream/execution_mode" >}}) 处理执行模式下运行。
{{< /hint >}}

{{< hint info >}}
**注意** 状态处理器API目前不提供Scala API。因此，
它将始终使用Java类型堆栈自动推导序列化器。要为Scala DataStream API引导
一个savepoint，请手动传入所有类型信息。
{{< /hint >}}

```java
int maxParallelism = 128;

SavepointWriter
    .newSavepoint(env, new HashMapStateBackend(), maxParallelism)
    .withOperator(OperatorIdentifier.forUid("uid1"), transformation1)
    .withOperator(OperatorIdentifier.forUid("uid2"), transformation2)
    .write(savepointPath);
```

与每个运算符相关联的[UID]({{< ref "docs/ops/state/savepoints" >}}#assigning-operator-ids) 必须一对一地匹配您 `DataStream` 应用程序中分配给运算符的UID; 这些UID是Flink判断哪个状态映射到哪个运算符的依据。

### 运算符状态

基于 `CheckpointedFunction` 的简单运算符状态, 可以用 `StateBootstrapFunction` 创建.

```java
public class SimpleBootstrapFunction extends StateBootstrapFunction<Integer> {

    private ListState<Integer> state;

    @Override
    public void processElement(Integer value, Context ctx) throws Exception {
        state.add(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }
	
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state = context.getOperatorState().getListState(new ListStateDescriptor<>("state", Types.INT));
    }
}

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<Integer> data = env.fromElements(1, 2, 3);

StateBootstrapTransformation transformation = OperatorTransformation
    .bootstrapWith(data)
    .transform(new SimpleBootstrapFunction());
```

### 广播状态

[广播状态]({{< ref "docs/dev/datastream/fault-tolerance/broadcast_state" >}}) 可以用 `BroadcastStateBootstrapFunction` 来写. 和 `DataStream` API的广播状态类似, 完整的状态必须和内存适配。

```java
public class CurrencyRate {
    public String currency;

    public Double rate;
}

public class CurrencyBootstrapFunction extends BroadcastStateBootstrapFunction<CurrencyRate> {

    public static final MapStateDescriptor<String, Double> descriptor = 
        new MapStateDescriptor<>("currency-rates", Types.STRING, Types.DOUBLE);

    @Override
    public void processElement(CurrencyRate value, Context ctx) throws Exception {
        ctx.getBroadcastState(descriptor).put(value.currency, value.rate);
    }
}

DataStream<CurrencyRate> currencyDataSet = env.fromCollection(
    new CurrencyRate("USD", 1.0), new CurrencyRate("EUR", 1.3));

StateBootstrapTransformation<CurrencyRate> broadcastTransformation = OperatorTransformation
    .bootstrapWith(currencyDataSet)
    .transform(new CurrencyBootstrapFunction());
```

### 分区状态

使用 `KeyedStateBootstrapFunction` 可以把分区状态写入 `ProcessFunction`'和其他 `RichFunction` 中

```java
public class Account {
    public int id;

    public double amount;	

    public long timestamp;
}
 
public class AccountBootstrapper extends KeyedStateBootstrapFunction<Integer, Account> {
    ValueState<Double> state;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("total",Types.DOUBLE);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Account value, Context ctx) throws Exception {
        state.update(value.amount);
    }
}
 
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Account> accountDataSet = env.fromCollection(accounts);

StateBootstrapTransformation<Account> transformation = OperatorTransformation
    .bootstrapWith(accountDataSet)
    .keyBy(acc -> acc.id)
    .transform(new AccountBootstrapper());
```

`KeyedStateBootstrapFunction` 支持设置事件时间和处理时间定时器。
这些定时器不会在引导函数内触发，只有在 `DataStream` 应用程序内恢复后才会激活。
如果设置了处理时间定时器，但在定时器时间到达之后才恢复状态，定时器会在应用程序启动时立即触发。

<span class="label label-danger">Attention</span> 如果您的引导函数创建了定时器，那么状态只能使用其中一个[process]({{< ref "docs/dev/datastream/operators/process_function" >}}) 类型函数进行恢复。

### 窗口状态

状态处理器API支持为 [窗口运算符]({{< ref "docs/dev/datastream/operators/windows" >}})编写状态。
在编写窗口状态时，用户需要指定运算符ID、窗口分配器、清除器、可选触发器和聚合类型。
在引导转换中的配置与DataStream窗口中的配置匹配非常重要。

```java
public class Account {
    public int id;

    public double amount;	

    public long timestamp;
}
 
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Account> accountDataSet = env.fromCollection(accounts);

StateBootstrapTransformation<Account> transformation = OperatorTransformation
    .bootstrapWith(accountDataSet)
    .keyBy(acc -> acc.id)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .reduce((left, right) -> left + right);
```

## 修改 Savepoints

除了从头开始创建savepoint外，您还可以基于现有的savepoint创建一个，比如在为现有作业引导单个新运算符时。

```java
SavepointWriter
    .fromExistingSavepoint(env, oldPath, new HashMapStateBackend())
    .withOperator(OperatorIdentifier.forUid("uid"), transformation)
    .write(newPath);
```

### 修改 UID (哈希)

`SavepointWriter#changeOperatorIdenfifier` 可以被用于修改一个运算符的  [UIDs]({{< ref "docs/concepts/glossary" >}}#uid) 或 [UID hash]({{< ref "docs/concepts/glossary" >}}#uid-hash)。

如果一个 `UID` 没有被明确定义 (也就是自动生成，实际上未知), 你可以分配一个你知道`UID hash` 的 `UID`  (例如，通过解析日志):
```java
savepointWriter
    .changeOperatorIdentifier(
        OperatorIdentifier.forUidHash("2feb7f8bcc404c3ac8a981959780bd78"),
        OperatorIdentifier.forUid("new-uid"))
    ...
```

你也可以用另外的 `UID` 替代原先的:
```java
savepointWriter
        .changeOperatorIdentifier(
        OperatorIdentifier.forUid("old-uid"),
        OperatorIdentifier.forUid("new-uid"))
        ...
```
