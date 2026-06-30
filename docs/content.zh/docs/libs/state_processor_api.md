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


# State Processor API

Apache Flink 的 State Processor API 提供了批模式 (BATCH) 下使用 DataStream API 读取、写入、修改 savepoint 和 checkpoint 的强大能力。
由于 [DataStream 和 Table API 是等价的]({{< ref "docs/dev/table/data_stream_api" >}})，也可以使用 Table API 或 SQL 来分析和处理 savepoint 或 checkpoint 中的状态数据。

例如，可以获取一个正在运行的流应用程序的 savepoint，使用 State Processor API 在批模式下对该 savepoint 进行分析，以验证应用程序的行为是否正确；
还可以从任意存储中读取并预处理一批数据后将结果写入一个 savepoint，然后基于这个 savepoint 初始化流应用程序的状态； State Processor API 也可以用来修复不一致的状态条目。
State Processor API 为有状态应用程序的演化提供了新的方式。以前有状态应用程序不能够进行更改，否则会丢失所有状态。现在可以通过 State Processor API 修改状态的数据类型、调整操作符的最大并行度、拆分或合并操作符状态、重新分配操作符 UID 等。

请在应用程序中包含以下库以使用 State Processor API。

{{< artifact flink-state-processor-api >}}

## 从状态到数据集

State Processor API 将流应用程序的状态映射到若干个可以单独处理的数据集中，为了能使用 API，您需要先理解这种映射是如何工作的。

让我们先看看有状态的 Flink 作业是什么样子的。Flink 作业由算子 (Operator) 组成: 一个作业通常包括若干个 Source 算子，一些实际用于计算处理的算子以及若干个 Sink 算子。
每个算子由若干个子任务并行运行，一个算子中可以有不同类型的 State。一个算子可以有若干个 operator state，这些状态被组织成列表，每个子任务的 State 对应列表中的一个元素。
如果一个算子是 keyed stream 中的，则它可以有若干个 keyed state，用来存储从 record 中提取出的 key，keyed state 可以看作分布式键值映射。

下图展示了应用程序 MyApp 中的状态，它由三个名为 Src、Proc 和 Snk 的算子组成。Src 算子有一个 operator state (os1)，Proc 算子有一个 operator state (os2) 和两个 keyed state (ks1、ks2)，Snk 算子是无状态的。

{{< img src="/fig/application-my-app-state-processor-api.png" width="600px" alt="Application: MyApp" >}}

MyApp 的 savepoint 或 checkpoint 包含了所有状态数据，可以用来恢复每个子任务的状态。当使用批处理作业处理 savepoint/checkpoint 的数据时，我们需要一个映射模型，将各个任务的状态数据映射到数据集中。 
事实上，可以将 savepoint 视为数据库，每个算子（由其 UID 标识）代表一个命名空间。算子的 operator state 可以映射为命名空间中一个单列的表，表中的一行代表一个子任务。
算子所有的 keyed state 可以看作一个多列的表，每一列表示一个 keyed state。下图展示了 MyApp 的 savepoint 和数据集间的映射关系。

{{< img src="/fig/database-my-app-state-processor-api.png" width="600px" alt="Database: MyApp" >}}

上图显示了 Src 算子的 operator state 与数据集的映射，数据集的每一行表示一个 Src 算子的子任务的状态。
Proc 算子的 os2 也类似地映射到一个单列的表。Proc 算子的 ks1 和 ks2 组合成一个三列的表，第一列表示key，第二列表示 ks1，第三列表示 ks2，每一行表示一个 key 的状态。
Snk 算子没有状态，因此它的命名空间是空的。

## 算子的标识

State Processor API 允许使用 [UIDs]({{< ref "docs/concepts/glossary" >}}#UID) 或 [UID hash]({{< ref "docs/concepts/glossary" >}}#UID-hashes)来识别算子：`OperatorIdentifier#forUid/forUidHash`。
仅当无法使用 UID 时才应使用 UID hash，例如，当创建 [savepoint]({{< ref "docs/ops/state/savepoints" >}}) 的应用程序未指定 UID 或算子 UID 未知时。

## 通过 State Processor API 读取状态

读取状态首先需要指定 savepoint 或 checkpoint 的路径以及用于恢复数据的 `状态存储后端 (StateBackend)`。State processor API 恢复的状态与 DataStream 应用恢复的状态是一致的。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
SavepointReader savepoint = SavepointReader.read(env, "hdfs://path/", new HashMapStateBackend());
```

### Operator State

Flink 中的 non-keyed state 被称为 [operator state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#operator-state)。
在应用程序中使用 `CheckpointedFunction` 或 `BroadcastState` 会生成 operator State。 读取 operator state 时，需要指定算子 UID、状态名称和类型信息。 

#### Operator List State

通过 `getListState` 存储在 `CheckpointedFunction` 中的 operator state 可以用 `SavepointReader#readListState` 读取。 
状态名称和类型信息应该与定义在 DataStream 应用程序中声明此状态的 `ListStateDescriptor` 相匹配。

```java
DataStream<Integer> listState  = savepoint.readListState<>(
    OperatorIdentifier.forUid("my-uid"),
    "list-state",
    Types.INT);
```

#### Operator Union List State

通过 `getUnionListState` 存储在 `CheckpointedFunction` 中的 operator state 可以用 `SavepointReader#readUnionState` 读取。 
状态名称和类型信息应该与定义在 DataStream 应用程序中声明此状态的 `ListStateDescriptor` 相匹配。 
State Processor API 将返回一个状态的 _单_ 副本，可以看作并发度为 1 的 DataStream 应用。

```java
DataStream<Integer> listState  = savepoint.readUnionState<>(
    OperatorIdentifier.forUid("my-uid"),
    "union-state",
    Types.INT);
```

#### 广播状态 Broadcast State

可以用 `SavepointReader#readBroadcastState` 读取 [BroadcastState]({{< ref "docs/dev/datastream/fault-tolerance/broadcast_state" >}})。
状态名称和类型信息应该与定义在 DataStream 应用程序中声明此状态的 `MapStateDescriptor` 相匹配。
State Processor API 将返回一个状态的 _单_ 副本，可以看作并发度为 1 的 DataStream 应用。

```java
DataStream<Tuple2<Integer, Integer>> broadcastState = savepoint.readBroadcastState<>(
    OperatorIdentifier.forUid("my-uid"),
    "broadcast-state",
    Types.INT,
    Types.INT);
```

#### 使用自定义序列化器

如果在写出状态时 `StateDescriptor` 使用了自定义的 `TypeSerializer`，Operator state 也支持使用自定义的 `TypeSerializers` 来读取。

```java
DataStream<Integer> listState = savepoint.readListState<>(
    OperatorIdentifier.forUid("uid"),
    "list-state", 
    Types.INT,
    new MyCustomIntSerializer());
```

### Keyed State

[Keyed state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-state)，又叫分区状态 (partitioned state)，是使用一个 key 进行分区的状态。
当读取 keyed state 时，需要指定算子 id 和一个 `KeyedStateReaderFunction<KeyType, OutputType>`。

`KeyedStateReaderFunction` 允许用户读取任意列和复杂的状态类型，如 ListState, MapState, 和 AggregatingState。
这意味着如果一个算子包含一个带状态的处理函数，如：

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

算子中的状态可以通过定义输出类型和相应的 `KeyedStateReaderFunction` 来读取。

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

除了读取注册的状态之外，每个 key 还可以访问包括 event time 和 processing time [计时器](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/concepts/time/)等元数据的 `Context`。

**注意：** 当使用 `KeyedStateReaderFunction` 时，所有状态描述符必须在 `open` 函数中注册。 否则任何尝试调用 `RuntimeContext#get*State` 将导致 `RuntimeException`。

#### Filtering Keys

When you are only interested in state for a subset of keys, you can push a `SavepointKeyFilter` into the read instead of reading the whole keyed state and filtering afterward.

Depending on its type, a filter skips unnecessary reads at two levels:

* **Split pruning** — when the filter resolves to an exact set of keys (`SavepointKeyFilter.exact(...)`), Flink can determine up front which key groups own those keys and skips every other input split without ever opening it. This is the largest saving, but it only works for exact keys.
* **Key-level filtering** — within each split, every key is tested and non-matching keys are skipped before their state is read. This applies to all filters; a range filter relies on this level alone, since it cannot prune splits.

```java
import org.apache.flink.state.api.filter.SavepointKeyFilter;

// Read the state for a single key
DataStream<KeyedState> singleKey = savepoint.readKeyedState(
    OperatorIdentifier.forUid("my-uid"),
    new ReaderFunction(),
    Types.INT,
    TypeInformation.of(KeyedState.class),
    SavepointKeyFilter.exact(42));

// Read the state for a finite set of keys
DataStream<KeyedState> someKeys = savepoint.readKeyedState(
    OperatorIdentifier.forUid("my-uid"),
    new ReaderFunction(),
    Types.INT,
    TypeInformation.of(KeyedState.class),
    SavepointKeyFilter.exact(Set.of(1, 2, 3)));

// Read the state for a key range: 10 <= key < 100
DataStream<KeyedState> keyRange = savepoint.readKeyedState(
    OperatorIdentifier.forUid("my-uid"),
    new ReaderFunction(),
    Types.INT,
    TypeInformation.of(KeyedState.class),
    SavepointKeyFilter.range(10, true, 100, false));
```

`SavepointKeyFilter` provides the following factory methods:

* `SavepointKeyFilter.exact(K key)` / `SavepointKeyFilter.exact(Set<K> keys)` — match a single key or a finite set of keys.
* `SavepointKeyFilter.range(K lower, boolean lowerInclusive, K upper, boolean upperInclusive)` — match a range (`K` must implement `Comparable<K>`). Either bound may be `null` to leave that side unbounded.
* `SavepointKeyFilter.empty()` — match no keys. Not intended for direct use — it only serves as an internal building block for the Table API filter pushdown.

When the built-in filters are not enough, you can implement the `SavepointKeyFilter<K>` interface yourself.
For use with the DataStream API, only `test(K key)` has to be implemented; it is called for every key in each opened split and decides whether that key will be read.

```java
// Reads the state only for even keys
public class EvenKeyFilter implements SavepointKeyFilter<Integer> {

    @Override
    public boolean test(Integer key) {
        return key % 2 == 0;
    }
}

DataStream<KeyedState> evenState = savepoint.readKeyedState(
    OperatorIdentifier.forUid("my-uid"),
    new ReaderFunction(),
    Types.INT,
    TypeInformation.of(KeyedState.class),
    new EvenKeyFilter());
```

By default, a custom filter only enables key-level filtering — every split is still opened.
If your filter resolves to a finite set of keys, additionally override `getExactKeys()` to return them; Flink then prunes the splits whose key groups cannot contain any of those keys, just like the built-in `exact(...)` filter.

```java
// Reads the state only for keys 0..max
public class UpToKeyFilter implements SavepointKeyFilter<Integer> {

    private final int max;

    public UpToKeyFilter(int max) {
        this.max = max;
    }

    @Override
    public boolean test(Integer key) {
        return key >= 0 && key <= max;
    }

    @Override
    public Set<Integer> getExactKeys() {
        // Enumerate the finite key set so Flink can prune splits up front
        Set<Integer> keys = new HashSet<>();
        for (int k = 0; k <= max; k++) {
            keys.add(k);
        }
        return keys;
    }
}

DataStream<KeyedState> firstKeys = savepoint.readKeyedState(
    OperatorIdentifier.forUid("my-uid"),
    new ReaderFunction(),
    Types.INT,
    TypeInformation.of(KeyedState.class),
    new UpToKeyFilter(100));
```

The remaining interface methods can be left at their defaults for DataStream API usage, as they are only used internally in the Table API during push-down handling.

### 窗口状态 Window State

State Processor API 支持读取[窗口算子]({{< ref "docs/dev/datastream/operators/windows" >}})的状态，当读取窗口状态时，需要指定算子 id，窗口分配器和聚合类型。
此外，可以指定类似于 `WindowFunction` 或 `ProcessWindowFunction` 的 `WindowReaderFunction` 来增强每次读取的附加信息。

假设下面是一个统计用户每分钟点击次数的 DataStream 应用程序。

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
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new ClickCounter())
    .uid("click-window")
    .addSink(new Sink());

```

它的状态可以通过如下方式读取。

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
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate("click-window", new ClickCounter(), new ClickReader(), Types.STRING, Types.INT, Types.INT)
    .print();

```

另外，可以通过 `WindowReaderFunction` 里的 `Context#triggerState` 方法读取 `CountTrigger` 或自定义触发器的状态。

## 通过 State Processor API 生成新 savepoint

State processor API 可以用来生成 savepoint，这使得用户可以基于历史数据进行状态的初始化。
每个 savepoint 可以由若干个 `StateBootstrapTransformation` 生成，每个 `StateBootstrapTransformation` 定义了一个算子的状态。

{{< hint info >}}
当使用 `SavepointWriter` 时，您的应用程序必须在 [批]({{< ref "docs/dev/datastream/execution_mode" >}}) 执行模式下运行。
{{< /hint >}}

```java
int maxParallelism = 128;

SavepointWriter
    .newSavepoint(env, new HashMapStateBackend(), maxParallelism)
    .withOperator(OperatorIdentifier.forUid("uid1"), transformation1)
    .withOperator(OperatorIdentifier.forUid("uid2"), transformation2)
    .write(savepointPath);
```

与每个算子关联的 [UID]({{< ref "docs/ops/state/savepoints" >}}#assigning-operator-ids) 必须与 DataStream 应用程序中分配给算子的 UID 一一对应；这样 Flink 才能知道什么状态映射到哪个算子。

### Operator State

Simple operator state, using `CheckpointedFunction`, can be created using the `StateBootstrapFunction`.
在 DataStream API 中通过 `CheckpointedFunction` 创建出的、简单的 operator state，在 state processor API 中可以用 `StateBootstrapFunction` 创建。

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

### 广播状态 Broadcast State

[BroadcastState]({{< ref "docs/dev/datastream/fault-tolerance/broadcast_state" >}}) 可以用 `BroadcastStateBootstrapFunction` 生成。与 DataStream API 中的 broadcast state 一样，它的全部状态必须能完全放入内存。

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

### 分区状态 Keyed State

`ProcessFunction` 和其他 `RichFunction` 中的 keyed state 可以用 `KeyedStateBootstrapFunction` 生成。

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

`KeyedStateBootstrapFunction` 支持设置 event time 定时器和 processing time 定时器。
计时器不会在 state processor API 的 bootstrap 函数内触发，只有在 DataStream 应用程序中恢复后才会激活。
如果设置了 processing time 计时器，但直到该时间过去后状态才恢复，则计时器将在启动后立即触发。

<span class="label label-danger">注意</span> 如果您的 bootstrap 函数创建了计时器，则只能使用 [process]({{< ref "docs/dev/datastream/operators/process_function" >}}) 类型的函数之一来恢复状态。

### 窗口状态 Window State

State processor API 支持写出 [window operator]({{< ref "docs/dev/datastream/operators/windows" >}}) 的状态。
当写出窗口状态时，需要指定算子 id、窗口分配器、淘汰器(evictor)、触发器(可选)以及聚合类型。
State processor API 中 bootstrap transformation 的配置需要与 DataStream 窗口的配置相匹配。

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
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .reduce((left, right) -> left + right);
```

## 修改 Savepoint

除了从头开始创建 savepoint 之外，您还可以基于现有 savepoint 修改，例如为现有作业初始化一个新的算子。

```java
SavepointWriter
    .fromExistingSavepoint(env, oldPath, new HashMapStateBackend())
    .withOperator(OperatorIdentifier.forUid("uid"), transformation)
    .write(newPath);
```

### 更改 UID (hashes)

`SavepointWriter#changeOperatorIdentifier` 可以用来修改一个算子的 [UIDs]({{< ref "docs/concepts/glossary" >}}#uid) 或 [UID hash]({{< ref "docs/concepts/glossary" >}}#uid-hash)。

如果一个 `UID` 没有被显式地设置(自动生成的并且是未知的)，那么你可以通过提供 `UID hash` (通过解析 logs 获得 `UID hash`) 来为算子赋予一个 `UID`:
```java
savepointWriter
    .changeOperatorIdentifier(
        OperatorIdentifier.forUidHash("2feb7f8bcc404c3ac8a981959780bd78"),
        OperatorIdentifier.forUid("new-uid"))
    ...
```

也支持用新的 `UID` 替换就旧的 `UID`：
```java
savepointWriter
    .changeOperatorIdentifier(
        OperatorIdentifier.forUid("old-uid"),
        OperatorIdentifier.forUid("new-uid"))
    ...
```

## Table API

### Getting started

Before you interrogate state using the Table API, make sure to review our [Flink SQL]({{< ref "docs/sql/reference/overview" >}}) guidelines.

IMPORTANT NOTE: State Table API only supports keyed state.

### Metadata

The following SQL table function allows users to read the metadata of savepoints and checkpoints in the following way:
```SQL
LOAD MODULE state;
SELECT * FROM savepoint_metadata('/root/dir/of/checkpoint-data/chk-1');
```

The new table function creates a table with the following fixed schema:

| Key                                      | Data type       | Description                                                                                                                                                                                     |
|------------------------------------------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| checkpoint-id                            | BIGINT NOT NULL | Checkpoint ID.                                                                                                                                                                                  |
| operator-name                            | STRING          | Operator Name.                                                                                                                                                                                  |
| operator-uid                             | STRING          | Operator UID.                                                                                                                                                                                   |
| operator-uid-hash                        | STRING NOT NULL | Operator UID hash.                                                                                                                                                                              |
| operator-parallelism                     | INT NOT NULL    | Parallelism of the operator.                                                                                                                                                                    |
| operator-max-parallelism                 | INT NOT NULL    | Maximum parallelism of the operator.                                                                                                                                                            |
| operator-subtask-state-count             | INT NOT NULL    | Number of operator subtask states. It represents the state partition count divided by the operator's parallelism and might be 0 if the state is not partitioned (for example broadcast source). |
| operator-coordinator-state-size-in-bytes | BIGINT NOT NULL | The operator’s coordinator state size in bytes, or zero if no coordinator state.                                                                                                                |
| operator-total-size-in-bytes             | BIGINT NOT NULL | Total operator state size in bytes.                                                                                                                                                             |

### Keyed State

[Keyed state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-state), also known as partitioned state, is any state that is partitioned relative to a key.

The SQL connector allows users to read arbitrary columns as ValueState and complex state types such as ListState, MapState.
This means if an operator contains a stateful process function such as:
```java
eventStream
  .keyBy(e -> (Integer)e.key)
  .process(new StatefulFunction())
  .uid("my-uid");

...

public class Account {
    private Integer id;
    public Double amount;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}

@org.apache.avro.specific.AvroGenerated
public class AvroRecord ... {
    // Generated record which contains at least the following field: long longData
}

public class StatefulFunction extends KeyedProcessFunction<Integer, Integer, Void> {
  private ValueState<Integer> myValueState;
  private ValueState<Account> myAccountValueState;
  private ListState<Integer> myListState;
  private MapState<Integer, Integer> myMapState;
  private ValueState<AvroRecord> myAvroState;

  @Override
  public void open(OpenContext openContext) {
    myValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("MyValueState", Integer.class));
    myAccountValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("MyAccountValueState", Account.class));
    myValueState = getRuntimeContext().getListState(new ListStateDescriptor<>("MyListState", Integer.class));
    myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("MyMapState", Integer.class, Integer.class));
    myAvroState = getRuntimeContext().getMapState(new ValueStateDescriptor<>("MyAvroState", new AvroTypeInfo<>(AvroRecord.class)));
  }
  ...
}
```

Then it can read by querying a table created using the following SQL statement:
```SQL
CREATE TABLE state_table (
  k INTEGER,
  MyValueState INTEGER,
  MyAccountValueState ROW<id INTEGER, amount DOUBLE>,
  MyListState ARRAY<INTEGER>,
  MyMapState MAP<INTEGER, INTEGER>,
  MyAvroState ROW<longData bigint>,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'connector' = 'savepoint',
  'state.backend.type' = 'rocksdb',
  'state.path' = '/root/dir/of/checkpoint-data/chk-1',
  'operator.uid' = 'my-uid'
);
```

#### Filter Pushdown on the Key Column

When a query filters on the key column (the column declared as `PRIMARY KEY`), the savepoint connector automatically pushes the predicate down into the savepoint scan.
As with the DataStream `SavepointKeyFilter`, point and `IN` lookups prune whole key groups, while range predicates narrow the per-split key iteration, so only the relevant state is read.

```SQL
-- Point lookup: reads only the key group that contains key 42
SELECT * FROM state_table WHERE k = 42;

-- Point lookups on a finite set of keys, only key groups that contain 1, 2, or 3 are read
SELECT * FROM state_table WHERE k IN (1, 2, 3);

-- Range scan, reads all key groups but filters out irrelevant keys before state is read
SELECT * FROM state_table WHERE k >= 10 AND k < 100;
SELECT * FROM state_table WHERE k BETWEEN 10 AND 100;
```

The following predicates on the key column can be pushed down:

* `=` and `IN` — resolve to an exact set of keys and enable key-group pruning.
* `<`, `<=`, `>`, `>=`, and `BETWEEN` — resolve to a key range and enable key-level filtering within each split.
* `AND` — pushed down when **all** of its operands are range predicates; the ranges are intersected into a single, tighter range. If any operand is an equality/IN predicate, the entire AND expression is not pushed down — neither split pruning nor key-level filtering applies.
* `OR` — pushed down when **every** branch resolves to exact keys (the branches are unioned into one key set, which is equivalent to an `IN`). If any branch is a range predicate, the entire OR expression is not pushed down.

### Connector options

#### General options
| Option             | Required | Default | Type                                   | Description                                                                                                                                                                                                                          |
|--------------------|----------|---------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector          | required | (none)  | String                                 | Specify what connector to use, here should be 'savepoint'.                                                                                                                                                                           |
| state.backend.type | optional | (none)  | Enum Possible values: hashmap, rocksdb | Defines the state backend which must be used for state reading. This must match with the value which was defined in Flink job which created the savepoint or checkpoint. If not provided then it falls back to `state.backend.type` in flink configuration. |
| state.path         | required | (none)  | String                                 | Defines the state path which must be used for state reading. All file system that are supported by Flink can be used here.                                                                                                           |
| operator.uid       | optional | (none)  | String                                 | Defines the operator UID which must be used for state reading (can't be used together with `operator.uid.hash`). Either `operator.uid` or `operator.uid.hash` must be specified.                                                     |
| operator.uid.hash  | optional | (none)  | String                                 | Defines the operator UID hash which must be used for state reading (can't be used together with `operator.uid`). Either `operator.uid` or `operator.uid.hash` must be specified.                                                     |

#### Connector options for column ‘#’
| Option                           | Required | Default | Type                                   | Description                                                                                                                                                                                                                                                                      |
|----------------------------------|----------|---------|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fields.#.state-name              | optional | (none)  | String                                 | Overrides the state name which must be used for state reading. This can be useful when the state name contains characters which are not compliant with SQL column names.                                                                                                         |
| fields.#.state-type              | optional | (none)  | Enum Possible values: list, map, value | Defines the state type which must be used for state reading, including value, list and map. When it's not provided then it tries to infer from the SQL type (ARRAY=list, MAP=map, all others=value).                                                                             |
| fields.#.key-class               | optional | (none)  | String                                 | Defines the format class scheme for decoding map key data (for ex. java.lang.Long). Either key-class or key-type-factory can be specified. When none of them are provided then the format class scheme tries to infer from the SQL type (only primitive types supported).        |
| fields.#.key-type-factory        | optional | (none)  | String                                 | Defines the type information factory for decoding map key data. Either key-class or key-type-factory can be specified. When none of them are provided then the format class scheme tries to infer from the SQL type (only primitive types supported).                            |
| fields.#.value-class             | optional | (none)  | String                                 | Defines the format class scheme for decoding value data (for ex. java.lang.Long). Either value-class or value-info-factory can be specified. When none of them are provided then the format class scheme tries to infer from the SQL type (only primitive types supported).      |
| fields.#.value-type-factory      | optional | (none)  | String                                 | Defines the type information factory for decoding value data. Either value-class or value-type-factory can be specified. When none of them are provided then the format class scheme tries to infer from the SQL type (only primitive types supported).                          |

### Default Data Type Mapping

The state SQL connector infers the data type for primitive types when `fields.#.value-class` and `fields.#.key-class`
are not defined. The following table shows the `Flink SQL type` -> `Java type` default mapping. If the mapping is not calculated properly
then it can be overridden with the two mentioned config parameters on a per-column basis.

| Flink SQL type          | Java type                                                               |
|-------------------------|-------------------------------------------------------------------------|
| CHAR / VARCHAR / STRING | java.lang.String                                                        |
| BOOLEAN                 | boolean                                                                 |
| BINARY / VARBINARY      | byte[]                                                                  |
| DECIMAL                 | org.apache.flink.table.data.DecimalData                                 |
| TINYINT                 | byte                                                                    |
| SMALLINT                | short                                                                   |
| INTEGER                 | int                                                                     |
| BIGINT                  | long                                                                    |
| FLOAT                   | float                                                                   |
| DOUBLE                  | double                                                                  |
| DATE                    | int                                                                     |
| INTERVAL_YEAR_MONTH     | long                                                                    |
| INTERVAL_DAY_TIME       | long                                                                    |
| ARRAY                   | java.util.List                                                          |
| MAP                     | java.util.Map                                                           |
| ROW                     | java.util.List\<org.apache.flink.table.types.logical.RowType.RowField\> |

SHORTCUT: When a complex java class is defined in a column with STRING SQL type then the class instance
`toString` method result will be the column value. This can be useful when a quick explanatory query is required.
