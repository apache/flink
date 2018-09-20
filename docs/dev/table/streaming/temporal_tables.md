---
title: "Temporal Tables"
nav-parent_id: streaming_tableapi
nav-pos: 4
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

Temporal Tables represent a concept of a table that changes over time.
Flink can keep track of those changes and allows for accessing the table's content at a certain point in time within a query

* This will be replaced by the TOC
{:toc}

Motivation
----------

Let's assume that we have the following tables.

{% highlight sql %}
SELECT * FROM Orders;

rowtime amount currency
======= ====== =========
10:15        2 Euro
10:30        1 US Dollar
10:32       50 Yen
10:52        3 Euro
11:04        5 US Dollar
{% endhighlight %}

`Orders` is an append-only table that represents payments for given `amount` and given `currency`.
For example at `10:15` there was an order for an amount of `2 Euro`.

{% highlight sql %}
SELECT * FROM RatesHistory;

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Euro        114
09:00   Yen           1
10:45   Euro        116
11:15   Euro        119
{% endhighlight %}

`RatesHistory` represents an ever changing append-only stream of currency exchange rates, with respect to `Yen` (which has a rate of `1`).
For example exchange rate for a period from `09:00` to `10:45` of `Euro` to `Yen` was `114`.
From `10:45` to `11:15` it was `116`.

Task is now to calculate a value of all of the `Orders` converted to common currency (`Yen`).
For example we would like to convert the order
{% highlight sql %}
rowtime amount currency
======= ====== =========
10:15        2 Euro
{% endhighlight %}
using the appropriate conversion rate for the given `rowtime` (`114`).
Without using Temporal Tables in order to do so, one would need to write such query:
{% highlight sql %}
SELECT
  SUM(o.amount * r.rate) AS amount
FROM Orders AS o,
  RatesHistory AS r
WHERE r.currency = o.currency
AND r.rowtime = (
  SELECT MAX(rowtime)
  FROM Rates AS r2
  WHERE r2.currency = o.currency
  AND r2.rowtime <= o.rowtime);
{% endhighlight %}
Temporal Tables are a concept that aims to simplify this query,
speed up it's execution and reduce state usage.

In order to define a Temporal Table, we must define it's primary key,
Primary key allows us to overwrite older values in the Temporal Table.
In the above example `currency` would be a primary key for `RatesHistory` table.
Secondly a [time attribute](time_attributes.html) is also required,
that determines which row is newer and which one is older.

Temporal Table Functions
------------------------

In order to access the data in the Temporal Table,
one must pass a time attribute that determines the version of the table that will be returned.
Flink uses the SQL syntax of Table Functions to provide a way to express it.
Once defined, Temporal Table Function takes a single argument `timeAttribute` and returns a set of rows.
This set contains the latest versions of the rows for all of existing primary keys with respect to the given `timeAttribute`.

Assuming that we defined a `Rates(timeAttribute)` Temporal Table Function based on `RatesHistory` table.
We could query such function in the following way:

{% highlight sql %}
SELECT * FROM Rates('10:15');

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Euro        114
09:00   Yen           1

SELECT * FROM Rates('11:00');

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
10:45   Euro        116
09:00   Yen           1
{% endhighlight %}

Each query to `Rates(timeAttribute)` would return the state of the `Rates` for the given `timeAttribute`:

**Note**: Currently Flink doesn't support directly querying the Temporal Table Functions with a constant `timeAttribute`.
At the moment Temporal Table Functions can only be used in joins.
Above example was used to provide an intuition about what function `Rates(timeAttribute)` returns.

Processing time
---------------

### Defining Temporal Table Function

In order to define processing time Temporal Table:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.table.functions.TemporalTableFunction;
(...)

// Get the stream and table environments.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

// Provide static data set of orders table.
List<Tuple2<Long, String>> ordersData = new ArrayList<>();
ordersData.add(Tuple2.of(2L, "Euro"));
ordersData.add(Tuple2.of(1L, "US Dollar"));
ordersData.add(Tuple2.of(50L, "Yen"));
ordersData.add(Tuple2.of(3L, "Euro"));
ordersData.add(Tuple2.of(5L, "US Dollar"));

// Provide static data set of rates history table.
List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
ratesHistoryData.add(Tuple2.of("Euro", 114L));
ratesHistoryData.add(Tuple2.of("Yen", 1L));
ratesHistoryData.add(Tuple2.of("Euro", 116L));
ratesHistoryData.add(Tuple2.of("Euro", 119L));

// Create and register example tables using above data sets.
// In the real setup, you should replace this with your own tables.
DataStream<Tuple2<Long, String>> ordersStream = env.fromCollection(ordersData);
Table orders = tEnv.fromDataStream(ordersStream, "o_amount, o_currency, o_proctime.proctime");

DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, "r_currency, r_rate, r_proctime.proctime");

tEnv.registerTable("Orders", orders);
tEnv.registerTable("RatesHistory", ratesHistory);

// Create and register TemporalTableFunction. It will used "r_proctime" as the time attribute
// and "r_currency" as the primary key.
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency"); // <==== (1)
tEnv.registerFunction("Rates", rates);                                                              // <==== (2)

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// Get the stream and table environments.
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

// Provide static data set of orders table.
val ordersData = new mutable.MutableList[(Long, String)]
ordersData.+=((2L, "Euro"))
ordersData.+=((1L, "US Dollar"))
ordersData.+=((50L, "Yen"))
ordersData.+=((3L, "Euro"))
ordersData.+=((5L, "US Dollar"))

// Provide static data set of rates history table.
val ratesHistoryData = new mutable.MutableList[(String, Long)]
ratesHistoryData.+=(("US Dollar", 102L))
ratesHistoryData.+=(("Euro", 114L))
ratesHistoryData.+=(("Yen", 1L))
ratesHistoryData.+=(("Euro", 116L))
ratesHistoryData.+=(("Euro", 119L))

// Create and register example tables using above data sets.
// In the real setup, you should replace this with your own tables.
val orders = env
  .fromCollection(ordersData)
  .toTable(tEnv, 'o_amount, 'o_currency, 'o_proctime.proctime)
val ratesHistory = env
  .fromCollection(ratesHistoryData)
  .toTable(tEnv, 'r_currency, 'r_rate, 'r_proctime.proctime)

tEnv.registerTable("Orders", orders)
tEnv.registerTable("RatesHistory", ratesHistory)

// Create and register TemporalTableFunction. It will used "r_proctime" as the time attribute
// and "r_currency" as the primary key.
val rates = ratesHistory.createTemporalTableFunction('r_proctime, 'r_currency) // <==== (1)
tEnv.registerFunction("Rates", rates)                                          // <==== (2)
{% endhighlight %}
</div>
</div>

In the line `(1)` we created a `rates` [Temporal Table Function](#temporal-table-functions).
This allows us to use `rates` function in Table API.
Line `(2)` registers this function under `Rates` name in our table environment,
which allows us to use `Rates` function in SQL.

### Joining with Temporal Table Function

After [defining Temporal Table Function](#defining-temporal-table-function) we can start using it.
Temporal Table Functions can be used in the same way how normal Table Functions would be used.
For example to solve our motivating problem of converting currencies from `Orders` table,
we could:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}
Table result = orders
    .join(new Table(tEnv, "rates(o_proctime)"), "o_currency = r_currency")
    .select("o_amount * r_rate").as("amount")
    .sum("amount")
    .toAppendStream<Row>()
    .print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val result = orders
    .join(rates('o_proctime), 'r_currency === 'o_currency)
    .select('o_amount * 'r_rate).as('amount)
    .sum('amount)
    .toAppendStream[Row]
    .print
{% endhighlight %}
</div>
</div>

With processing time it is impossible to pass "past" time attributes as an argument to the Temporal Table Function.
By definition it is always a current timestamp.
Thus processing time Temporal Table Function invocations will always return the latest known versions of the underlying table
and any updates in the underlying history table will also immediately overwrite the current values.
Those new updates will have no effect on the previously results emitted/processed records from the probe side.

One can think about processing time Temporal Join as a simple `HashMap<K, V>`
that stores all of the records from the build side.
When a new record from the build side have the same key as some previous record,
the old value is just simply overwritten.
Every record from the probe side is always evaluated against the most recent/current state of the `HashMap`.

#### Resource usage

Only the latest version (with respect to the defined primary key) of the build side records are being kept on the state.
