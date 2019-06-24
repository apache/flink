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

Apache Flink's State Processor API provides powerful functionality to reading, writing, and modifing savepoints and checkpoints using Flink’s batch DataSet api.
This is useful for tasks such as analyzing state for interesting patterns, troubleshooting or auditing jobs by checking for discrepancies, and bootstrapping state for new applications.

* This will be replaced by the TOC
{:toc}

## Abstraction

To understand how to best interact with savepoints in a batch context it is important to have a clear mental model of how the data in Flink state relates to a traditional relational database.

A database can be thought of as one or more namespaces, each containing a collection of tables.
Those tables in turn contain columns whose values have some intrinsic relationship between them, such as being scoped under the same key.

A savepoint represents the state of a Flink job at a particular point in time which is made up of many operators.
Those operators contain various kinds of state, both partitioned or keyed state, and non-partitioned or operator state. 

<div data-lang="java" markdown="1">
{% highlight java %}
MapStateDescriptor<Integer, Double> CURRENCY_RATES = new MapStateDescriptor<>("rates", Types.INT, Types.DOUBLE);
 
class CurrencyConverter extends BroadcastProcessFunction<Transaction, CurrencyRate, Transaction> {
 
  public void processElement(
        Transaction value,
        ReadOnlyContext ctx,
        Collector<Transaction> out) throws Exception {
 
     Double rate = ctx.getBroadcastState(CURRENCY_RATES).get(value.currencyId);
     if (rate != null) {
        value.amount *= rate;
     }
     out.collect(value);
  }
 
  public void processBroadcastElement(
        CurrencyRate value,
        Context ctx,
        Collector<Transaction> out) throws Exception {
        ctx.getBroadcastState(CURRENCY_RATES).put(value.currencyId, value.rate);
  }
}
  
class Summarize extends RichFlatMapFunction<Transaction, Summary> {
  transient ValueState<Double> totalState;
  transient ValueState<Integer> countState;
 
  public void open(Configuration configuration) throws Exception {
     totalState = getRuntimeContext().getState(new ValueStateDescriptor<>("total", Types.DOUBLE));
     countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.INT));
  }
 
  public void flatMap(Transaction value, Collector<Summary> out) throws Exception {
     Summary summary = new Summary();
     summary.total = value.amount;
     summary.count = 1;
 
     Double currentTotal = totalState.value();
     if (currentTotal != null) {
        summary.total += currentTotal;
     }
 
     Integer currentCount = countState.value();
     if (currentCount != null) {
        summary.count += currentCount;
     }
     countState.update(summary.count);
 
     out.collect(summary);
  }
}
 
DataStream<Transaction> transactions = . . .
BroadcastStream<CurrencyRate> rates = . . .
transactions
  .connect(rates)
  .process(new CurrencyConverter())
  .uid("currency_converter")
  .keyBy(transaction -> transaction.accountId)
  .flatMap(new Summarize())
  .uid("summarize")
{% endhighlight %}
</div>

This job contains multiple operators along with various kinds of state.
When analyzing that state we can first scope data by its operator, named by setting its uid.
Within each operator we can look at the registered states.
`CurrencyConverter` has a broadcast state, which is a type of non-partitioned operator state.
In general, there is no relationship between any two elements in an operator state and so we can look at each value as being its own row.
Contrast this with Summarize, which contains two keyed states.
Because both states are scoped under the same key we can safely assume there exists some relationship between the two values.
Therefore, keyed state is best understood as a single table per operator containing one _key_ column along with _n_ value columns, one for each registered state.
All of this means that the state for this job could be described using the following pseudo-sql commands. 

{% highlight sql %}
CREATE NAMESPACE currency_converter;
 
CREATE TABLE currency_converter.rates (
   value Tuple2<Integer, Double>
);
 
CREATE NAMESPACE summarize;
 
CREATE TABLE summarize.keyed_state (
   key   INTEGER PRIMARY KEY,
   total DOUBLE,
   count INTEGER
);
{% endhighlight %}

In general, the savepoint ↔ database relationship can be summarized as:

    * A savepoint is a database
    * An operator is a namespace named by its uid
    * Each operator state represents a single table
        * Each element in an operator state represents a single row in that table
    * Each operator containing keyed state has a single “keyed_state” table
        * Each keyed_state table has one key column mapping the key value of the operator
        * Each registered state represents a single column in the table
        * Each row in the table maps to a single key

## Reading State

Reading state begins by specifiying the path to a valid savepoint or checkpoint along with the `StateBackend` that should be used to restore the data.
The compatability guarantees for restoring state are identical to those when restoring a `DataStream` application.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
ExistingSavepoint savepoint = Savepoint.load(bEnv, "hdfs://path/", new RocksDBStateBackend());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val bEnv      = ExecutionEnvironment.getExecutionEnvironment()
val savepoint = Savepoint.load(bEnv, "hdfs://path/", new RocksDBStateBackend())
{% endhighlight %}
</div>
</div>

When reading operator state, simply specify the operator uid, state name, and type information.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Integer> listState  = savepoint.readListState(
    "my-uid",
    "list-state",
    Types.INT);

DataSet<Integer> unionState = savepoint.readUnionState(
    "my-uid",
    "union-state",
    Types.INT);
 
DataSet<Tuple2<Integer, Integer>> broadcastState = savepoint.readBroadcastState(
    "my-uid",
    "broadcast-state",
    Types.INT,
    Types.INT);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val listState  = savepoint.readListState(
    "my-uid",
    "list-state",
    Types.INT)

val unionState = savepoint.readUnionState(
    "my-uid",
    "union-state",
    Types.INT)
 
val broadcastState = savepoint.readBroadcastState(
    "my-uid",
    "broadcast-state",
    Types.INT,
    Types.INT)
{% endhighlight %}
</div>
</div>

A custom `TypeSerializer` may also be specified if one was used in the `StateDescriptor` for the state.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Integer> listState = savepoint.readListState(
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
    new MyCustomIntSerializer())
{% endhighlight %}
</div>
</div>

When reading keyed state, users specify a KeyedStateReaderFunction to allow reading arbitrary columns and complex state types such as ListState, MapState, and AggregatingState.
This means if an operator contains a stateful process function such as:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class StatefulFunctionWithTime extends KeyedProcessFunction<Integer, Integer, Void> {
 
   ValueState<Integer> state;
 
   @Override
   public void open(Configuration parameters) {
      state = getRuntimeContext().getState(stateDescriptor);
   }
 
   @Override
   public void processElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
      state.update(value + 1);
   }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
public class StatefulFunctionWithTime extends KeyedProcessFunction[Integer, Integer, Void] {
 
  var state: ValueState[Integer];
 
   @throws[Exception]
   override def open(Configuration parameters) {
      state = getRuntimeContext().getState(stateDescriptor);
   }
 
   @throws[Exception]
   override def processElement(value: Integer, ctx: Context, out: Collector[Void]) {
      state.update(value + 1);
   }
}
{% endhighlight %}
</div>
</div>

Then it can read by defining an output type and corresponding KeyedStateReaderFunction. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
class KeyedState {
  Integer key;
  Integer value;
}
 
class ReaderFunction extends KeyedStateReaderFunction<Integer, KeyedState> {
  ValueState<Integer> state;
 
  @Override
  public void open(Configuration parameters) {
     state = getRuntimeContext().getState(stateDescriptor);
  }
 
  @Override
  public void processKey(
    Integer key,
    Context ctx,
    Collector<KeyedState> out) throws Exception {
 
     KeyedState data = new KeyedState();
     data.key    = key;
     data.value  = state.value();
     out.collect(data);
  }
}
 
DataSet<KeyedState> keyedState = savepoint.readKeyedState("my-uid", new ReaderFunction());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
case class KeyedState(key: Int, value: Int)
 
class ReaderFunction extends KeyedStateReaderFunction[Integer, KeyedState] {
  var state ValueState[Integer];
 
  override def open(Configuration parameters) {
     state = getRuntimeContext().getState(stateDescriptor);
  }
 
  override def processKey(
    key: Int,
    ctx: Context,
    out: Collector[Keyedstate]) throws Exception {
 
     val data = KeyedState(key, state.value())
     out.collect(data);
  }
}

val keyedState = savepoint.readKeyedState("my-uid", new ReaderFunction());
{% endhighlight %}
</div>
</div>

{% panel **Note:** When using a `KeyedStateReaderFunction` all state descriptors must be registered eagerly inside of open. Any attempt to call `RuntimeContext#getState`, `RuntimeContext#getListState`, or `RuntimeContext#getMapState` will result in a `RuntimeException`. %}

## Writing New Savepoints

State writers are based around the abstraction of `Savepoint`, where one `Savepoint` may have many operators and the state for any particular operator is created using a `BootstrapTransformation`.

A `BootstrapTransformation` starts with a `DataSet` containing the values that are to be written into state.
The transformation may be optionally `keyed` depending on whether or not you are writing keyed or operator state.
Finally a bootstrap function is applied depending to the transformation; Flink supplies `KeyedStateBootstrapFunction` for writing keyed state, `StateBootstrapFunction` for writing non keyed state, and `BroadcastStateBootstrapFunction` for writing broadcast state.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public  class Account {
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

    override def open(parameters: Configuration): Unit = {
        val descriptor = new ValueStateDescriptor[Double]("total",Types.DOUBLE)
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
    .keyBy(acc -> acc.id)
    .transform(new AccountBootstrapper())
{% endhighlight %}
</div>
</div>

The 'KeyedStateBootstrapFunction` supports setting event time and processing time timers.
The timers will not fire insde the bootstrap function and only become active once restored within a `DataStream` application.
If a processing time timer is set but the state is not restored until after that time has passed, the timer will fire immediatly upon start.

Once one or more transformations have been created they may be combined into a single `Savepoint`. 
`Savepoint`'s are created using a state backend and max parallelism, they may contain any number of operators. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Savepoint
    .create(backend, 128)
    .withOperator("uid1", transformation1)
    .withOperator("uid2", transformation2)
    .write(savepointPath);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
Savepoint
    .create(backend, 128)
    .withOperator("uid1", transformation1)
    .withOperator("uid2", transformation2)
    .write(savepointPath)
{% endhighlight %}
</div>
</div>
		
Besides creating a savepoint from scratch, you can base on off an existing savepoint such as when bootstrapping a single new operator for an existing job.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Savepoint
    .load(backend, oldPath)
    .withOperator("uid", transformation)
    .write(newPath);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
Savepoint
    .load(backend, oldPath)
    .withOperator("uid", transformation)
    .write(newPath)
{% endhighlight %}
</div>
</div>

{% panel **Note:** When basing a new savepoint on existing state, the state processor api makes a shallow copy of the pointers to the existing operators. This means that both savepoints share state and one cannot be deleted without corrupting the other! %}
