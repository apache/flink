---
title: "State Processor API"
nav-title: State Processor API
nav-parent_id: libs
nav-pos: 2
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

Apache Flink's State Processor API provides powerful functionality to reading, writing, and modifing savepoints and checkpoints using Flink’s batch DataSet API.
Due to the [interoperability of DataSet and Table API](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/common.html#integration-with-datastream-and-dataset-api), you can even use relational Table API or SQL queries to analyze and process state data.

For example, you can take a savepoint of a running stream processing application and analyze it with a DataSet batch program to verify that the application behaves correctly.
Or you can read a batch of data from any store, preprocess it, and write the result to a savepoint that you use to bootstrap the state of a streaming application.
It is also possible to fix inconsistent state entries.
Finally, the State Processor API opens up many ways to evolve a stateful application that was previously blocked by parameter and design choices that could not be changed without losing all the state of the application after it was started.
For example, you can now arbitrarily modify the data types of states, adjust the maximum parallelism of operators, split or merge operator state, re-assign operator UIDs, and so on.

To get started with the state processor api, include the following library in your application.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-state-processor-api{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}

* This will be replaced by the TOC
{:toc}

## Mapping Application State to DataSets 

The State Processor API maps the state of a streaming application to one or more data sets that can be processed separately.
In order to be able to use the API, you need to understand how this mapping works.

But let us first have a look at what a stateful Flink job looks like.
A Flink job is composed of operators; typically one or more source operators, a few operators for the actual processing, and one or more sink operators.
Each operator runs in parallel in one or more tasks and can work with different types of state.
An operator can have zero, one, or more *“operator states”* which are organized as lists that are scoped to the operator's tasks.
If the operator is applied on a keyed stream, it can also have zero, one, or more *“keyed states”* which are scoped to a key that is extracted from each processed record.
You can think of keyed state as a distributed key-value map. 

The following figure shows the application “MyApp” which consists of three operators called “Src”, “Proc”, and “Snk”.
Src has one operator state (os1), Proc has one operator state (os2) and two keyed states (ks1, ks2) and Snk is stateless.

<p style="display: block; text-align: center; margin-top: 20px; margin-bottom: 20px">
	<img src="{{ site.baseurl }}/fig/application-my-app-state-processor-api.png" width="600px" alt="Application: MyApp"/>
</p>

A savepoint or checkpoint of MyApp consists of the data of all states, organized in a way that the states of each task can be restored.
When processing the data of a savepoint (or checkpoint) with a batch job, we need a mental model that maps the data of the individual tasks' states into data sets or tables.
In fact, we can think of a savepoint as a database. Every operator (identified by its UID) represents a namespace.
Each operator state of an operator is mapped to a dedicated table in the namespace with a single column that holds the state's data of all tasks.
All keyed states of an operator are mapped to a single table consisting of a column for the key, and one column for each keyed state.
The following figure shows how a savepoint of MyApp is mapped to a database.

<p style="display: block; text-align: center; margin-top: 20px; margin-bottom: 20px">
	<img src="{{ site.baseurl }}/fig/database-my-app-state-processor-api.png" width="600px" alt="Database: MyApp"/>
</p>

The figure shows how the values of Src's operator state are mapped to a table with one column and five rows, one row for each of the list entries across all parallel tasks of Src.
Operator state os2 of the operator “Proc” is similarly mapped to an individual table.
The keyed states ks1 and ks2 are combined to a single table with three columns, one for the key, one for ks1 and one for ks2.
The keyed table holds one row for each distinct key of both keyed states.
Since the operator “Snk” does not have any state, its namespace is empty.

## Reading State

Reading state begins by specifying the path to a valid savepoint or checkpoint along with the `StateBackend` that should be used to restore the data.
The compatibility guarantees for restoring state are identical to those when restoring a `DataStream` application.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
ExistingSavepoint savepoint = Savepoint.load(bEnv, "hdfs://path/", new MemoryStateBackend());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val bEnv      = ExecutionEnvironment.getExecutionEnvironment
val savepoint = Savepoint.load(bEnv, "hdfs://path/", new MemoryStateBackend)
{% endhighlight %}
</div>
</div>

### Operator State

[Operator state]({{ site.baseurl }}/dev/stream/state/state.html#operator-state) is any non-keyed state in Flink.
This includes, but is not limited to, any use of `CheckpointedFunction` or `BroadcastState` within an application.
When reading operator state, users specify the operator uid, the state name, and the type information.

#### Operator List State

Operator state stored in a `CheckpointedFunction` using `getListState` can be read using `ExistingSavepoint#readListState`.
The state name and type information should match those used to define the `ListStateDescriptor` that declared this state in the DataStream application.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Integer> listState  = savepoint.readListState<>(
    "my-uid",
    "list-state",
    Types.INT);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val listState  = savepoint.readListState(
    "my-uid",
    "list-state",
    Types.INT)
{% endhighlight %}
</div>
</div>

#### Operator Union List State

Operator state stored in a `CheckpointedFunction` using `getUnionListState` can be read using `ExistingSavepoint#readUnionState`.
The state name and type information should match those used to define the `ListStateDescriptor` that declared this state in the DataStream application.
The framework will return a _single_ copy of the state, equivalent to restoring a DataStream with parallelism 1.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Integer> listState  = savepoint.readUnionState<>(
    "my-uid",
    "union-state",
    Types.INT);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val listState  = savepoint.readUnionState(
    "my-uid",
    "union-state",
    Types.INT)
{% endhighlight %}
</div>
</div>


#### Broadcast State

[BroadcastState]({{ site.baseurl }} /dev/stream/state/broadcast_state.html) can be read using `ExistingSavepoint#readBroadcastState`.
The state name and type information should match those used to define the `MapStateDescriptor` that declared this state in the DataStream application.
The framework will return a _single_ copy of the state, equivalent to restoring a DataStream with parallelism 1.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Tuple2<Integer, Integer>> broadcastState = savepoint.readBroadcastState<>(
    "my-uid",
    "broadcast-state",
    Types.INT,
    Types.INT);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val broadcastState = savepoint.readBroadcastState(
    "my-uid",
    "broadcast-state",
    Types.INT,
    Types.INT)
{% endhighlight %}
</div>
</div>

#### Using Custom Serializers

Each of the operator state readers support using custom `TypeSerializers` if one was used to define the `StateDescriptor` that wrote out the state. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Integer> listState = savepoint.readListState<>(
    "uid",
    "list-state", 
    Types.INT,
    new MyCustomIntSerializer());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val listState = savepoint.readListState(
    "uid",
    "list-state", 
    Types.INT,
    new MyCustomIntSerializer)
{% endhighlight %}
</div>
</div>

### Keyed State

[Keyed state]({{ site.baseurl }}/dev/stream/state/state.html#keyed-state), or partitioned state, is any state that is partitioned relative to a key.
When reading a keyed state, users specify the operator id and a `KeyedStateReaderFunction<KeyType, OutputType>`.

The `KeyedStateReaderFunction` allows users to read arbitrary columns and complex state types such as ListState, MapState, and AggregatingState.
This means if an operator contains a stateful process function such as:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class StatefulFunctionWithTime extends KeyedProcessFunction<Integer, Integer, Void> {
 
   ValueState<Integer> state;
 
   ListState<Long> updateTimes;

   @Override
   public void open(Configuration parameters) {
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
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class StatefulFunctionWithTime extends KeyedProcessFunction[Integer, Integer, Void] {
 
   var state: ValueState[Integer] = _
 
   var updateTimes: ListState[Long] = _ 

   @throws[Exception]
   override def open(parameters: Configuration): Unit = {
      val stateDescriptor = new ValueStateDescriptor("state", Types.INT)
      state = getRuntimeContext().getState(stateDescriptor)

      val updateDescriptor = new ListStateDescriptor("times", Types.LONG)
      updateTimes = getRuntimeContext().getListState(updateDescriptor)
   }
 
   @throws[Exception]
   override def processElement(value: Integer, ctx: KeyedProcessFunction[ Integer, Integer, Void ]#Context, out: Collector[Void]): Unit = {
      state.update(value + 1)
      updateTimes.add(System.currentTimeMillis)
   }
}
{% endhighlight %}
</div>
</div>

Then it can read by defining an output type and corresponding `KeyedStateReaderFunction`. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<KeyedState> keyedState = savepoint.readKeyedState("my-uid", new ReaderFunction());

public class KeyedState {
  public int key;

  public int value;

  public List<Long> times;
}
 
public class ReaderFunction extends KeyedStateReaderFunction<Integer, KeyedState> {

  ValueState<Integer> state;
 
  ListState<Long> updateTimes;

  @Override
  public void open(Configuration parameters) {
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
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val keyedState = savepoint.readKeyedState("my-uid", new ReaderFunction)

case class KeyedState(key: Int, value: Int, times: List[Long])
 
class ReaderFunction extends KeyedStateReaderFunction[Integer, KeyedState] {
 
  var state: ValueState[Integer] = _

  var updateTimes: ListState[Long] = _
 
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
     val stateDescriptor = new ValueStateDescriptor("state", Types.INT)
     state = getRuntimeContext().getState(stateDescriptor)

      val updateDescriptor = new ListStateDescriptor("times", Types.LONG)
      updateTimes = getRuntimeContext().getListState(updateDescriptor)
    }
 

  @throws[Exception]
  override def processKey(
    key: Int,
    ctx: Context,
    out: Collector[Keyedstate]): Unit = {
 
     val data = KeyedState(key, state.value(), updateTimes.get.asScala.toList)
     out.collect(data)
  }
}
{% endhighlight %}
</div>
</div>

Along with reading registered state values, each key has access to a `Context` with metadata such as registered event time and processing time timers.

{% panel **Note:** When using a `KeyedStateReaderFunction`, all state descriptors must be registered eagerly inside of open. Any attempt to call a `RuntimeContext#get*State` will result in a `RuntimeException`. %}

## Writing New Savepoints

`Savepoint`'s may also be written, which allows such use cases as bootstrapping state based on historical data.
Each savepoint is made up of one or more `BootstrapTransformation`'s (explained below), each of which defines the state for an individual operator.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
int maxParallelism = 128;

Savepoint
    .create(new MemoryStateBackend(), maxParallelism)
    .withOperator("uid1", transformation1)
    .withOperator("uid2", transformation2)
    .write(savepointPath);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val maxParallelism = 128

Savepoint
    .create(new MemoryStateBackend(), maxParallelism)
    .withOperator("uid1", transformation1)
    .withOperator("uid2", transformation2)
    .write(savepointPath)
{% endhighlight %}
</div>
</div>

The [UIDs]({{ site.baseurl}}/ops/state/savepoints.html#assigning-operator-ids) associated with each operator must match one to one with the UIDs assigned to the operators in your `DataStream` application; these are how Flink knows what state maps to which operator.

### Operator State

Simple operator state, using `CheckpointedFunction`, can be created using the `StateBootstrapFunction`. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
DataSet<Integer> data = env.fromElements(1, 2, 3);

BootstrapTransformation transformation = OperatorTransformation
    .bootstrapWith(data)
    .transform(new SimpleBootstrapFunction());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class SimpleBootstrapFunction extends StateBootstrapFunction[Integer] {

    var ListState[Integer] state = _

    @throws[Exception]
    override def processElement(value: Integer, ctx: Context): Unit = {
        state.add(value)
    }

    @throws[Exception]
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
    }
	
    @throws[Exception]
    override def initializeState(context: FunctionInitializationContext): Unit = {
        state = context.getOperatorState().getListState(new ListStateDescriptor("state", Types.INT))
    }
}

val env = ExecutionEnvironment.getExecutionEnvironment
val data = env.fromElements(1, 2, 3)

BootstrapTransformation transformation = OperatorTransformation
    .bootstrapWith(data)
    .transform(new SimpleBootstrapFunction)
{% endhighlight %}
</div>
</div>

### Broadcast State

[BroadcastState]({{ site.baseurl }} /dev/stream/state/broadcast_state.html) can be written using a `BroadcastStateBootstrapFunction`. Similar to broadcast state in the `DataStream` API, the full state must fit in memory. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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

DataSet<CurrencyRate> currencyDataSet = bEnv.fromCollection(
    new CurrencyRate("USD", 1.0), new CurrencyRate("EUR", 1.3));

BootstrapTransformation<CurrencyRate> broadcastTransformation = OperatorTransformation
    .bootstrapWith(currencyDataSet)
    .transform(new CurrencyBootstrapFunction());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
case class CurrencyRate(currency: String, rate: Double)

object CurrencyBootstrapFunction {
    val descriptor = new MapStateDescriptor("currency-rates", Types.STRING, Types.DOUBLE)
}

class CurrencyBootstrapFunction extends BroadcastStateBootstrapFunction[CurrencyRate] {

    @throws[Exception]
    override processElement(value: CurrencyRate, ctx: Context): Unit = {
        ctx.getBroadcastState(descriptor).put(value.currency, value.rate)
    }
}

val currencyDataSet = bEnv.fromCollection(CurrencyRate("USD", 1.0), CurrencyRate("EUR", 1.3))

val broadcastTransformation = OperatorTransformation
    .bootstrapWith(currencyDataSet)
    .transform(new CurrencyBootstrapFunction)
{% endhighlight %}
</div>
</div>

### Keyed State

Keyed state for `ProcessFunction`'s and other `RichFunction` types can be written using a `KeyedStateBootstrapFunction`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class Account {
    public int id;

    public double amount;	

    public long timestamp;
}
 
public class AccountBootstrapper extends KeyedStateBootstrapFunction<Integer, Account> {
    ValueState<Double> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("total",Types.DOUBLE);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Account value, Context ctx) throws Exception {
        state.update(value.amount);
    }
}
 
ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Account> accountDataSet = bEnv.fromCollection(accounts);

BootstrapTransformation<Account> transformation = OperatorTransformation
    .bootstrapWith(accountDataSet)
    .keyBy(acc -> acc.id)
    .transform(new AccountBootstrapper());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
case class Account(id: Int, amount: Double, timestamp: Long)

class AccountBootstrapper extends KeyedStateBootstrapFunction[Integer, Account] {
    var state: ValueState[Double]

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
        val descriptor = new ValueStateDescriptor("total",Types.DOUBLE)
        state = getRuntimeContext().getState(descriptor)
    }

    @throws[Exception]
    override def processElement(value: Account, ctx: Context): Unit = {
        state.update(value.amount)
    }
}
 
val bEnv = ExecutionEnvironment.getExecutionEnvironment()

val accountDataSet = bEnv.fromCollection(accounts)

val transformation = OperatorTransformation
    .bootstrapWith(accountDataSet)
    .keyBy(acc => acc.id)
    .transform(new AccountBootstrapper)
{% endhighlight %}
</div>
</div>

The `KeyedStateBootstrapFunction` supports setting event time and processing time timers.
The timers will not fire inside the bootstrap function and only become active once restored within a `DataStream` application.
If a processing time timer is set but the state is not restored until after that time has passed, the timer will fire immediately upon start.

<span class="label label-danger">Attention</span> If your bootstrap function creates timers, the state can only be restored using one of the [process]({{ site.baseurl }}/dev/stream/operators/process_function.html) type functions.

## Modifying Savepoints

Besides creating a savepoint from scratch, you can base one off an existing savepoint such as when bootstrapping a single new operator for an existing job.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Savepoint
    .load(bEnv, new MemoryStateBackend(), oldPath)
    .withOperator("uid", transformation)
    .write(newPath);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
Savepoint
    .load(bEnv, new MemoryStateBackend, oldPath)
    .withOperator("uid", transformation)
    .write(newPath)
{% endhighlight %}
</div>
</div>

{% panel **Note:** When basing a new savepoint on existing state, the state processor api makes a shallow copy of the pointers to the existing operators. This means that both savepoints share state and one cannot be deleted without corrupting the other! %}
