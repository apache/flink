---
title: "Legacy Planner"
nav-parent_id: tableapi
nav-pos: 1001
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

Table planners are responsible for translating relational operators into an executable, optimized Flink job.
Flink supports two different planner implementations; the modern planner (sometimes referred to as `Blink`) and the legacy planner.
For production use cases, we recommend the modern planner which is the default.

The legacy planner is in maintenance mode and no longer under active development.
The primary reason to continue using the legacy planner is [DataSet]({% link dev/batch/index.md %}) interop.

This page describes how to use the Legacy planner and where its semantics differ from the 
modern planner. 

* This will be replaced by the TOC
{:toc}

## Setup

### Dependencies

When deploying to a cluster, the legacy planner is bundled in Flinks distribution by default.
If you want to run the Table API & SQL programs locally within your IDE, you must add the
following set of modules to your application.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}

### Configuring the TableEnvironment 

When creating a `TableEnvironment` the Legacy planner is configured via the `EnvironmentSettings`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .useOldPlanner()
    .inStreamingMode()
    //.inBatchMode()
    .build();

TableEnvironment tEnv = TableEnvironment.create(settings);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings
    .newInstance()
    .useOldPlanner()
    .inStreamingMode()
    //.inBatchMode()
    .build()

val tEnv = TableEnvironment.create(settings)
{% endhighlight %}
</div>
</div>

`BatchTableEnvironment` may used for [DataSet]({% link dev/batch/index.md %}) and [DataStream]({% link dev/datastream_api.md %}) interop respectively.

{% capture custom_operator_note %}
If you are not using the Legacy planner for DataSet interop, the community strongly
encourages you to consider the modern table planner.
{% endcapture %}
{% include note.html content=custom_operator_note %}

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

EnvironmentSettings fsSettings = EnvironmentSettings
    .newInstance()
    .useOldPlanner()
    .inStreamingMode()
    .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
val tEnv = BatchTableEnvironment.create(env)

val fsSettings = EnvironmentSettings
    .newInstance()
    .useOldPlanner()
    .inStreamingMode()
    .build()

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
StreamTableEnvironment tEnv = StreamTableEnvironment.create(fsEnv, fsSettings)
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment

f_b_env = ExecutionEnvironment.get_execution_environment()
f_b_t_env = BatchTableEnvironment.create(f_b_env, table_config)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

f_s_env = StreamExecutionEnvironment.get_execution_environment()
f_s_settings = EnvironmentSettings.new_instance().use_old_planner().in_streaming_mode().build()
f_s_t_env = StreamTableEnvironment.create(f_s_env, environment_settings=f_s_settings)
{% endhighlight %}
</div>
</div>


## Integration with DataSet

The primary use case for the Legacy planner is interoperation with the DataSet API. 
To translate `DataSet`s to and from tables, applications must use the `BatchTableEnvironment`.

### Create a View from a DataSet

A `DataSet` can be registered in a `BatchTableEnvironment` as a `View`.
The schema of the resulting view depends on the data type of the registered collection.

**Note:** Views created from a `DataSet` can be registered as temporary views only.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableEnvironment tEnv = ...; 
DataSet<Tuple2<Long, String>> dataset = ...;

tEnv.createTemporaryView("my-table", dataset, $("myLong"), $("myString"))
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight java %}
val tEnv: BatchTableEnvironment = ??? 
val dataset: DataSet[(Long, String)] = ???

tEnv.createTemporaryView("my-table", dataset, $"myLong", $"myString")
{% endhighlight %}
</div>
</div>

### Create a Table from a DataSet

A `DataSet` can be directly converted to a `Table` in a `BatchTableEnvironment`.
The schema of the resulting view depends on the data type of the registered collection.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableEnvironment tEnv = ...; 
DataSet<Tuple2<Long, String>> dataset = ...;

Table myTable = tEnv.fromDataSet("my-table", dataset, $("myLong"), $("myString"))
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight java %}
val tEnv: BatchTableEnvironment = ??? 
val dataset: DataSet[(Long, String)] = ???

val table = tEnv.fromDataSet("my-table", dataset, $"myLong", $"myString")
{% endhighlight %}
</div>
</div>

### Convert a Table to a DataSet

A `Table` can be converted to a `DataSet`.
In this way, custom DataSet programs can be run on the result of a Table API or SQL query.

When converting from a `Table`, users must specify the data type of the results.
Often the most convenient conversion type is `Row`.
The following list gives an overview of the features of the different options.

- **Row**: fields are mapped by position, arbitrary number of fields, support for `null` values, no type-safe access.
- **POJO**: fields are mapped by name (POJO fields must be named as `Table` fields), arbitrary number of fields, support for `null` values, type-safe access.
- **Case Class**: fields are mapped by position, no support for `null` values, type-safe access.
- **Tuple**: fields are mapped by position, limitation to 22 (Scala) or 25 (Java) fields, no support for `null` values, type-safe access.
- **Atomic Type**: `Table` must have a single field, no support for `null` values, type-safe access.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

Table table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into a DataSet of Row by specifying a class
DataSet<Row> dsRow = tableEnv.toDataSet(table, Row.class);

// Convert the Table into a DataSet of Tuple2<String, Integer> via a TypeInformation
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
DataSet<Tuple2<String, Integer>> dsTuple = tableEnv.toDataSet(table, tupleType);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv = BatchTableEnvironment.create(env)

val table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into a DataSet of Row
val dsRow: DataSet[Row] = tableEnv.toDataSet[Row](table)

// Convert the Table into a DataSet of Tuple2[String, Int]
val dsTuple: DataSet[(String, Int)] = tableEnv.toDataSet[(String, Int)](table)
{% endhighlight %}
</div>
</div>

<span class="label label-danger">Attention</span> **Once the Table is converted to a DataSet, we must use the ExecutionEnvironment.execute method to execute the DataSet program.**

{% top %}

Main Differences Between the Two Planners
-----------------------------------------

1. Blink treats batch jobs as a special case of streaming. As such, the conversion between Table and DataSet is also not supported, and batch jobs will not be translated into `DateSet` programs but translated into `DataStream` programs, the same as the streaming jobs.
2. The Blink planner does not support `BatchTableSource`, use bounded `StreamTableSource` instead of it.
3. The implementations of `FilterableTableSource` for the old planner and the Blink planner are incompatible. The old planner will push down `PlannerExpression`s into `FilterableTableSource`, while the Blink planner will push down `Expression`s.
4. String based key-value config options (Please see the documentation about [Configuration]({% link dev/table/config.md %}) for details) are only used for the Blink planner.
5. The implementation(`CalciteConfig`) of `PlannerConfig` in two planners is different.
6. The Blink planner will optimize multiple-sinks into one DAG on both `TableEnvironment` and `StreamTableEnvironment`. The old planner will always optimize each sink into a new DAG, where all DAGs are independent of each other.
7. The old planner does not support catalog statistics now, while the Blink planner does.

