---
title: State Processor API
weight: 2
type: docs
aliases:
  - /dev/libs/state_processor_api.html
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

Apache Flink's State Processor API provides powerful functionality for reading, writing, and modifying savepoints and checkpoints using Flink’s [DataStream API]({{< ref "#datastream-api" >}}) and [Table API]({{< ref "#table-api" >}}) under `BATCH` execution.
Due to the [interoperability of DataStream and Table API]({{< ref "docs/dev/table/data_stream_api" >}}), you can even use relational Table API or SQL queries to analyze and process state data.

For example, you can take a savepoint of a running stream processing application and analyze it with a DataStream batch program to verify that the application behaves correctly.
Or you can read a batch of data from any store, preprocess it, and write the result to a savepoint that you use to bootstrap the state of a streaming application.
It is also possible to fix inconsistent state entries.
Finally, the State Processor API opens up many ways to evolve a stateful application that was previously blocked by parameter and design choices that could not be changed without losing all the state of the application after it was started.
For example, you can now arbitrarily modify the data types of states, adjust the maximum parallelism of operators, split or merge operator state, re-assign operator UIDs, and so on.

To get started with the state processor api, include the following library in your application.

{{< artifact flink-state-processor-api >}}

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

{{< img src="/fig/application-my-app-state-processor-api.png" width="600px" alt="Application: MyApp" >}}

A savepoint or checkpoint of MyApp consists of the data of all states, organized in a way that the states of each task can be restored.
When processing the data of a savepoint (or checkpoint) with a batch job, we need a mental model that maps the data of the individual tasks' states into data sets or tables.
In fact, we can think of a savepoint as a database. Every operator (identified by its UID) represents a namespace.
Each operator state of an operator is mapped to a dedicated table in the namespace with a single column that holds the state's data of all tasks.
All keyed states of an operator are mapped to a single table consisting of a column for the key, and one column for each keyed state.
The following figure shows how a savepoint of MyApp is mapped to a database.

{{< img src="/fig/database-my-app-state-processor-api.png" width="600px" alt="Database: MyApp" >}}

The figure shows how the values of Src's operator state are mapped to a table with one column and five rows, one row for each of the list entries across all parallel tasks of Src.
Operator state os2 of the operator “Proc” is similarly mapped to an individual table.
The keyed states ks1 and ks2 are combined to a single table with three columns, one for the key, one for ks1 and one for ks2.
The keyed table holds one row for each distinct key of both keyed states.
Since the operator “Snk” does not have any state, its namespace is empty.

## Identifying operators

The State Processor API allows you to identify operators using [UIDs]({{< ref "docs/concepts/glossary.md" >}}#UID) or [UID hashes]({{< ref "docs/concepts/glossary" >}}#UID-hashes) via `OperatorIdentifier#forUid/forUidHash`.
Hashes should only be used when the use of `UIDs` is not possible, for example when the application that created the [savepoint]({{< ref "docs/ops/state/savepoints" >}}) did not specify them or when the `UID` is unknown.

## DataStream API
### Reading State

Reading state begins by specifying the path to a valid savepoint or checkpoint along with the `StateBackend` that should be used to restore the data.
The compatibility guarantees for restoring state are identical to those when restoring a `DataStream` application.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
SavepointReader savepoint = SavepointReader.read(env, "hdfs://path/", new HashMapStateBackend());
```

#### Operator State

[Operator state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#operator-state) is any non-keyed state in Flink.
This includes, but is not limited to, any use of `CheckpointedFunction` or `BroadcastState` within an application.
When reading operator state, users specify the operator uid, the state name, and the type information.

##### Operator List State

Operator state stored in a `CheckpointedFunction` using `getListState` can be read using `SavepointReader#readListState`.
The state name and type information should match those used to define the `ListStateDescriptor` that declared this state in the DataStream application.

```java
DataStream<Integer> listState  = savepoint.readListState<>(
    OperatorIdentifier.forUid("my-uid"),
    "list-state",
    Types.INT);
```

##### Operator Union List State

Operator state stored in a `CheckpointedFunction` using `getUnionListState` can be read using `SavepointReader#readUnionState`.
The state name and type information should match those used to define the `ListStateDescriptor` that declared this state in the DataStream application.
The framework will return a _single_ copy of the state, equivalent to restoring a DataStream with parallelism 1.

```java
DataStream<Integer> listState  = savepoint.readUnionState<>(
    OperatorIdentifier.forUid("my-uid"),
    "union-state",
    Types.INT);
```

##### Broadcast State

[BroadcastState]({{< ref "docs/dev/datastream/fault-tolerance/broadcast_state" >}}) can be read using `SavepointReader#readBroadcastState`.
The state name and type information should match those used to define the `MapStateDescriptor` that declared this state in the DataStream application.
The framework will return a _single_ copy of the state, equivalent to restoring a DataStream with parallelism 1.

```java
DataStream<Tuple2<Integer, Integer>> broadcastState = savepoint.readBroadcastState<>(
    OperatorIdentifier.forUid("my-uid"),
    "broadcast-state",
    Types.INT,
    Types.INT);
```

##### Using Custom Serializers

Each of the operator state readers support using custom `TypeSerializers` if one was used to define the `StateDescriptor` that wrote out the state. 

```java
DataStream<Integer> listState = savepoint.readListState<>(
    OperatorIdentifier.forUid("uid"),
    "list-state", 
    Types.INT,
    new MyCustomIntSerializer());
```

#### Keyed State

[Keyed state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-state), also known as partitioned state, is any state that is partitioned relative to a key.
When reading a keyed state, users specify the operator id and a `KeyedStateReaderFunction<KeyType, OutputType>`.

The `KeyedStateReaderFunction` allows users to read arbitrary columns and complex state types such as ListState, MapState, and AggregatingState.
This means if an operator contains a stateful process function such as:

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

Then it can read by defining an output type and corresponding `KeyedStateReaderFunction`. 

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

Along with reading registered state values, each key has access to a `Context` with metadata such as registered event time and processing time timers.

**Note:** When using a `KeyedStateReaderFunction`, all state descriptors must be registered eagerly inside of open. Any attempt to call a `RuntimeContext#get*State` will result in a `RuntimeException`.

#### Window State

The state processor api supports reading state from a [window operator]({{< ref "docs/dev/datastream/operators/windows" >}}).
When reading a window state, users specify the operator id, window assigner, and aggregation type.

Additionally, a `WindowReaderFunction` can be specified to enrich each read with additional information similar
to a `WindowFunction` or `ProcessWindowFunction`.

Suppose a DataStream application that counts the number of clicks per user per minute.

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

This state can be read using the code below.

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
    .aggregate("click-window", new ClickCounter(), new ClickReader(), Types.String, Types.INT, Types.INT)
    .print();
```

Additionally, trigger state - from `CountTrigger`s or custom triggers - can be read using the method
`Context#triggerState` inside the `WindowReaderFunction`.

### Writing New Savepoints

`Savepoint`'s may also be written, which allows such use cases as bootstrapping state based on historical data.
Each savepoint is made up of one or more `StateBootstrapTransformation`'s (explained below), each of which defines the state for an individual operator.

{{< hint info >}}
When using the `SavepointWriter`, your application must be executed under [BATCH]({{< ref "docs/dev/datastream/execution_mode" >}}) execution.
{{< /hint >}}

```java
int maxParallelism = 128;

SavepointWriter
    .newSavepoint(env, new HashMapStateBackend(), maxParallelism)
    .withOperator(OperatorIdentifier.forUid("uid1"), transformation1)
    .withOperator(OperatorIdentifier.forUid("uid2"), transformation2)
    .write(savepointPath);
```

The [UIDs]({{< ref "docs/ops/state/savepoints" >}}#assigning-operator-ids) associated with each operator must match one to one with the UIDs assigned to the operators in your `DataStream` application; these are how Flink knows what state maps to which operator.

#### Operator State

Simple operator state, using `CheckpointedFunction`, can be created using the `StateBootstrapFunction`. 

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

#### Broadcast State

[BroadcastState]({{< ref "docs/dev/datastream/fault-tolerance/broadcast_state" >}}) can be written using a `BroadcastStateBootstrapFunction`. Similar to broadcast state in the `DataStream` API, the full state must fit in memory. 

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

#### Keyed State

Keyed state for `ProcessFunction`'s and other `RichFunction` types can be written using a `KeyedStateBootstrapFunction`.

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

The `KeyedStateBootstrapFunction` supports setting event time and processing time timers.
The timers will not fire inside the bootstrap function and only become active once restored within a `DataStream` application.
If a processing time timer is set but the state is not restored until after that time has passed, the timer will fire immediately upon start.

<span class="label label-danger">Attention</span> If your bootstrap function creates timers, the state can only be restored using one of the [process]({{< ref "docs/dev/datastream/operators/process_function" >}}) type functions.

#### Window State

The state processor api supports writing state for the [window operator]({{< ref "docs/dev/datastream/operators/windows" >}}).
When writing window state, users specify the operator id, window assigner, evictor, optional trigger, and aggregation type.
It is important the configurations on the bootstrap transformation match the configurations on the DataStream window.

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

### Modifying Savepoints

Besides creating a savepoint from scratch, you can base one off an existing savepoint such as when bootstrapping a single new operator for an existing job.

```java
SavepointWriter
    .fromExistingSavepoint(env, oldPath, new HashMapStateBackend())
    .withOperator(OperatorIdentifier.forUid("uid"), transformation)
    .write(newPath);
```

#### Changing UID (hashes)

`SavepointWriter#changeOperatorIdenfifier` can be used to modify the [UIDs]({{< ref "docs/concepts/glossary" >}}#uid) or [UID hash]({{< ref "docs/concepts/glossary" >}}#uid-hash) of an operator.

If a `UID` was not explicitly set (and was thus auto-generated and is effectively unknown), you can assign a `UID` provided that you know the `UID hash` (e.g., by parsing the logs):
```java
savepointWriter
    .changeOperatorIdentifier(
        OperatorIdentifier.forUidHash("2feb7f8bcc404c3ac8a981959780bd78"),
        OperatorIdentifier.forUid("new-uid"))
    ...
```

You can also replace one `UID` with another:
```java
savepointWriter
    .changeOperatorIdentifier(
        OperatorIdentifier.forUid("old-uid"),
        OperatorIdentifier.forUid("new-uid"))
    ...
```

## Table API

### Getting started

Before you interrogate state using the table API, make sure to review our [Flink SQL]({{< ref "docs/dev/table/sql/overview" >}}) guidelines.

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

    public Integer geId() {
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

public class AvroSavepointTypeInformationFactory implements SavepointTypeInformationFactory {
    @Override
    public TypeInformation<?> getTypeInformation() {
        return new AvroTypeInfo<>(AvroRecord.class);
    }
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
  'operator.uid' = 'my-uid',
  'fields.MyAvroState.value-type-factory' = 'org.apache.flink.state.table.AvroSavepointTypeInformationFactory'
);
```

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
