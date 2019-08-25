---
title: "Table API"
nav-parent_id: tableapi
nav-pos: 20
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

The Table API is a unified, relational API for stream and batch processing. Table API queries can be run on batch or streaming input without modifications. The Table API is a super set of the SQL language and is specially designed for working with Apache Flink. The Table API is a language-integrated API for Scala and Java. Instead of specifying queries as String values as common with SQL, Table API queries are defined in a language-embedded style in Java or Scala with IDE support like autocompletion and syntax validation. 

The Table API shares many concepts and parts of its API with Flink's SQL integration. Have a look at the [Common Concepts & API]({{ site.baseurl }}/dev/table/common.html) to learn how to register tables or to create a `Table` object. The [Streaming Concepts](./streaming) pages discuss streaming specific concepts such as dynamic tables and time attributes.

The following examples assume a registered table called `Orders` with attributes `(a, b, c, rowtime)`. The `rowtime` field is either a logical [time attribute](./streaming/time_attributes.html) in streaming or a regular timestamp field in batch.

* This will be replaced by the TOC
{:toc}

Overview & Examples
-----------------------------

The Table API is available for Scala and Java. The Scala Table API leverages on Scala expressions, the Java Table API is based on strings which are parsed and converted into equivalent expressions.

The following example shows the differences between the Scala and Java Table API. The table program is executed in a batch environment. It scans the `Orders` table, groups by field `a`, and counts the resulting rows per group. The result of the table program is converted into a `DataSet` of type `Row` and printed.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

The Java Table API is enabled by importing `org.apache.flink.table.api.java.*`. The following example shows how a Java Table API program is constructed and how expressions are specified as strings.

{% highlight java %}
import org.apache.flink.table.api._
import org.apache.flink.table.api.java._

// environment configuration
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

// register Orders table in table environment
// ...

// specify table program
Table orders = tEnv.scan("Orders"); // schema (a, b, c, rowtime)

Table counts = orders
        .groupBy("a")
        .select("a, b.count as cnt");

// conversion to DataSet
DataSet<Row> result = tEnv.toDataSet(counts, Row.class);
result.print();
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

The Scala Table API is enabled by importing `org.apache.flink.api.scala._` and `org.apache.flink.table.api.scala._`.

The following example shows how a Scala Table API program is constructed. Table attributes are referenced using [Scala Symbols](http://scala-lang.org/files/archive/spec/2.12/01-lexical-syntax.html#symbol-literals), which start with an apostrophe character (`'`).

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

// environment configuration
val env = ExecutionEnvironment.getExecutionEnvironment
val tEnv = BatchTableEnvironment.create(env)

// register Orders table in table environment
// ...

// specify table program
val orders = tEnv.scan("Orders") // schema (a, b, c, rowtime)

val result = orders
               .groupBy('a)
               .select('a, 'b.count as 'cnt)
               .toDataSet[Row] // conversion to DataSet
               .print()
{% endhighlight %}

</div>
<div data-lang="python" markdown="1">

使用`from pyflink.table import *`来导入Python Table API。

下面这个例子演示了如何组织一个Python Table API程序，以及字符串形式的表达式用法。

{% highlight python %}
from pyflink.table import *
from pyflink.dataset import *

# environment configuration
env = ExecutionEnvironment.get_execution_environment()
t_env = TableEnvironment.create(env, TableConfig())

# register Orders table and Result table sink in table environment
# ...

# specify table program
orders = t_env.scan("Orders")  # schema (a, b, c, rowtime)

orders.group_by("a").select("a, b.count as cnt").insert_into("result")

t_env.execute("python_job")

{% endhighlight %}

</div>
</div>

The next example shows a more complex Table API program. The program scans again the `Orders` table. It filters null values, normalizes the field `a` of type String, and calculates for each hour and product `a` the average billing amount `b`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
// environment configuration
// ...

// specify table program
Table orders = tEnv.scan("Orders"); // schema (a, b, c, rowtime)

Table result = orders
        .filter("a.isNotNull && b.isNotNull && c.isNotNull")
        .select("a.lowerCase() as a, b, rowtime")
        .window(Tumble.over("1.hour").on("rowtime").as("hourlyWindow"))
        .groupBy("hourlyWindow, a")
        .select("a, hourlyWindow.end as hour, b.avg as avgBillingAmount");
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

{% highlight scala %}
// environment configuration
// ...

// specify table program
val orders: Table = tEnv.scan("Orders") // schema (a, b, c, rowtime)

val result: Table = orders
        .filter('a.isNotNull && 'b.isNotNull && 'c.isNotNull)
        .select('a.lowerCase() as 'a, 'b, 'rowtime)
        .window(Tumble over 1.hour on 'rowtime as 'hourlyWindow)
        .groupBy('hourlyWindow, 'a)
        .select('a, 'hourlyWindow.end as 'hour, 'b.avg as 'avgBillingAmount)
{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
# environment configuration
# ...

# specify table program
orders = t_env.scan("Orders")  # schema (a, b, c, rowtime)

result = orders.filter("a.isNotNull && b.isNotNull && c.isNotNull") \
               .select("a.lowerCase() as a, b, rowtime") \
               .window(Tumble.over("1.hour").on("rowtime").alias("hourlyWindow")) \
               .group_by("hourlyWindow, a") \
               .select("a, hourlyWindow.end as hour, b.avg as avgBillingAmount")
{% endhighlight %}

</div>
</div>

Since the Table API is a unified API for batch and streaming data, both example programs can be executed on batch and streaming inputs without any modification of the table program itself. In both cases, the program produces the same results given that streaming records are not late (see [Streaming Concepts](streaming) for details).

{% top %}

Operations
----------

The Table API supports the following operations. Please note that not all operations are available in both batch and streaming yet; they are tagged accordingly.

### Scan, Projection, and Filter

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
  		<td>
        <strong>Scan</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
  		<td>
        <p>Similar to the FROM clause in a SQL query. Performs a scan of a registered table.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
{% endhighlight %}
      </td>
  	</tr>
    <tr>
      <td>
        <strong>Select</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL SELECT statement. Performs a select operation.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.select("a, c as d");
{% endhighlight %}
        <p>You can use star (<code>*</code>) to act as a wild card, selecting all of the columns in the table.</p>
{% highlight java %}
Table result = orders.select("*");
{% endhighlight %}
</td>
        </tr>
    <tr>
      <td>
        <strong>As</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Renames fields.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.as("x, y, z, t");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Where / Filter</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL WHERE clause. Filters out rows that do not pass the filter predicate.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.where("b === 'red'");
{% endhighlight %}
or
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.filter("a % 2 === 0");
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
  		<td>
        <strong>Scan</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
  		<td>
        <p>Similar to the FROM clause in a SQL query. Performs a scan of a registered table.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
{% endhighlight %}
      </td>
  	</tr>
  	<tr>
      <td>
        <strong>Select</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL SELECT statement. Performs a select operation.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.select('a, 'c as 'd)
{% endhighlight %}
        <p>You can use star (<code>*</code>) to act as a wild card, selecting all of the columns in the table.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.select('*)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>As</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Renames fields.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders").as('x, 'y, 'z, 't)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Where / Filter</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL WHERE clause. Filters out rows that do not pass the filter predicate.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.filter('a % 2 === 0)
{% endhighlight %}
or
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.where('b === "red")
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>
<div data-lang="python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
  		<td>
        <strong>Scan</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
  		<td>
        <p>类似于SQL请求中的FROM子句，将一个环境中已注册的表转换成Table对象。</p>
{% highlight python %}
orders = table_env.scan("Orders")
{% endhighlight %}
      </td>
  	</tr>
    <tr>
      <td>
        <strong>Select</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>类似于SQL请求中的SELECT子句，执行一个select操作。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.select("a, c as d")
{% endhighlight %}
        <p>您可以使用星号 (<code>*</code>) 表示选择表中的所有列。</p>
{% highlight python %}
result = orders.select("*")
{% endhighlight %}
</td>
        </tr>
    <tr>
      <td>
        <strong>Alias</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>重命名字段。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.alias("x, y, z, t")
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Where / Filter</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>类似于SQL请求中的WHERE子句，过滤掉表中不满足条件的行。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.where("b === 'red'")
{% endhighlight %}
or
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.filter("a % 2 === 0")
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

### Column Operations
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
          <td>
            <strong>AddColumns</strong><br>
            <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
          </td>
          <td>
          <p>Performs a field add operation. It will throw an exception if the added fields already exist.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.addColumns("concat(c, 'sunny')");
{% endhighlight %}
</td>
        </tr>

 <tr>
     <td>
                    <strong>AddOrReplaceColumns</strong><br>
                    <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
                  </td>
                  <td>
                  <p>Performs a field add operation. Existing fields will be replaced if add columns name is the same as the existing column name.  Moreover, if the added fields have duplicate field name, then the last one is used. </p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.addOrReplaceColumns("concat(c, 'sunny') as desc");
{% endhighlight %}
                  </td>
                </tr>
         <tr>
                  <td>
                    <strong>DropColumns</strong><br>
                    <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
                  </td>
                  <td>
                  <p>Performs a field drop operation. The field expressions should be field reference expressions, and only existing fields can be dropped.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.dropColumns("b, c");
{% endhighlight %}
                  </td>
                </tr>
         <tr>
                  <td>
                    <strong>RenameColumns</strong><br>
                    <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
                  </td>
                  <td>
                  <p>Performs a field rename operation. The field expressions should be alias expressions, and only the existing fields can be renamed.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.renameColumns("b as b2, c as c2");
{% endhighlight %}
                  </td>
                </tr>
  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
     <tr>
          <td>
            <strong>AddColumns</strong><br>
            <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
          </td>
          <td>
            <p>Performs a field add operation. It will throw an exception if the added fields already exist.</p>
{% highlight scala %}
val orders = tableEnv.scan("Orders");
val result = orders.addColumns(concat('c, "Sunny"))
{% endhighlight %}
          </td>
        </tr>
         <tr>
                  <td>
                    <strong>AddOrReplaceColumns</strong><br>
                    <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
                  </td>
                  <td>
                     <p>Performs a field add operation. Existing fields will be replaced if add columns name is the same as the existing column name.  Moreover, if the added fields have duplicate field name, then the last one is used. </p>
{% highlight scala %}
val orders = tableEnv.scan("Orders");
val result = orders.addOrReplaceColumns(concat('c, "Sunny") as 'desc)
{% endhighlight %}
                  </td>
                </tr>
         <tr>
                  <td>
                    <strong>DropColumns</strong><br>
                    <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
                  </td>
                  <td>
                    <p>Performs a field drop operation. The field expressions should be field reference expressions, and only existing fields can be dropped.</p>
{% highlight scala %}
val orders = tableEnv.scan("Orders");
val result = orders.dropColumns('b, 'c)
{% endhighlight %}
                  </td>
                </tr>
 <tr>
                  <td>
                    <strong>RenameColumns</strong><br>
                    <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
                  </td>
                  <td>
                    <p>Performs a field rename operation. The field expressions should be alias expressions, and only the existing fields can be renamed.</p>
{% highlight scala %}
val orders = tableEnv.scan("Orders");
val result = orders.renameColumns('b as 'b2, 'c as 'c2)
{% endhighlight %}
                  </td>
                </tr>
  </tbody>
</table>
</div>
<div data-lang="python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
  <tr>
          <td>
            <strong>AddColumns</strong><br>
            <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
          </td>
          <td>
          <p>执行新增字段操作。如果欲添加字段已经存在，将会抛出异常。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.add_columns("concat(c, 'sunny')")
{% endhighlight %}
</td>
        </tr>
        
 <tr>
     <td>
                    <strong>AddOrReplaceColumns</strong><br>
                    <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
                  </td>
                  <td>
                  <p>执行新增字段操作。如果欲添加字段已经存在，将会替换该字段。如果新增字段列表中有同名字段，取最靠后的为有效字段。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.add_or_replace_columns("concat(c, 'sunny') as desc")
{% endhighlight %}
                  </td>
                </tr>
         <tr>
                  <td>
                    <strong>DropColumns</strong><br>
                    <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
                  </td>
                  <td>
                  <p>执行删除字段操作。参数必须是字段列表，并且必须是已经存在的字段才能被删除。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.drop_columns("b, c")
{% endhighlight %}
                  </td>
                </tr>
         <tr>
                  <td>
                    <strong>RenameColumns</strong><br>
                    <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
                  </td>
                  <td>
                  <p>执行重命名字段操作。参数必须是字段别名(例：b as b2)列表，并且必须是已经存在的字段才能被重命名。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.rename_columns("b as b2, c as c2")
{% endhighlight %}
                  </td>
                </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

### Aggregations

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>GroupBy Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span><br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to a SQL GROUP BY clause. Groups the rows on the grouping keys with a following running aggregation operator to aggregate rows group-wise.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.groupBy("a").select("a, b.sum as d");
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the type of aggregation and the number of distinct grouping keys. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
    	<td>
        <strong>GroupBy Window Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>Groups and aggregates a table on a <a href="#group-windows">group window</a> and possibly one or more grouping keys.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders
    .window(Tumble.over("5.minutes").on("rowtime").as("w")) // define window
    .groupBy("a, w") // group by key and window
    .select("a, w.start, w.end, w.rowtime, b.sum as d"); // access window properties and aggregate
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Over Window Aggregation</strong><br>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
       <p>Similar to a SQL OVER clause. Over window aggregates are computed for each row, based on a window (range) of preceding and succeeding rows. See the <a href="#over-windows">over windows section</a> for more details.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders
    // define window
    .window(Over
      .partitionBy("a")
      .orderBy("rowtime")
      .preceding("UNBOUNDED_RANGE")
      .following("CURRENT_RANGE")
      .as("w"))
    .select("a, b.avg over w, b.max over w, b.min over w"); // sliding aggregate
{% endhighlight %}
       <p><b>Note:</b> All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single <a href="streaming/time_attributes.html">time attribute</a>.</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span> <br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to a SQL DISTINCT aggregation clause such as COUNT(DISTINCT a). Distinct aggregation declares that an aggregation function (built-in or user-defined) is only applied on distinct input values. Distinct can be applied to <b>GroupBy Aggregation</b>, <b>GroupBy Window Aggregation</b> and <b>Over Window Aggregation</b>.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
// Distinct aggregation on group by
Table groupByDistinctResult = orders
    .groupBy("a")
    .select("a, b.sum.distinct as d");
// Distinct aggregation on time window group by
Table groupByWindowDistinctResult = orders
    .window(Tumble.over("5.minutes").on("rowtime").as("w")).groupBy("a, w")
    .select("a, b.sum.distinct as d");
// Distinct aggregation on over window
Table result = orders
    .window(Over
        .partitionBy("a")
        .orderBy("rowtime")
        .preceding("UNBOUNDED_RANGE")
        .as("w"))
    .select("a, b.avg.distinct over w, b.max over w, b.min over w");
{% endhighlight %}
        <p>User-defined aggregation function can also be used with DISTINCT modifiers. To calculate the aggregate results only for distinct values, simply add the distinct modifier towards the aggregation function. </p>
{% highlight java %}
Table orders = tEnv.scan("Orders");

// Use distinct aggregation for user-defined aggregate functions
tEnv.registerFunction("myUdagg", new MyUdagg());
orders.groupBy("users").select("users, myUdagg.distinct(points) as myDistinctResult");
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct fields. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span> <br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns records with distinct value combinations.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.distinct();
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct fields. Please provide a query configuration with valid retention interval to prevent excessive state size. If state cleaning is enabled, distinct have to emit messages to prevent too early state eviction of downstream operators which makes distinct contains result updating. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>

    <tr>
      <td>
        <strong>GroupBy Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span><br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to a SQL GROUP BY clause. Groups the rows on the grouping keys with a following running aggregation operator to aggregate rows group-wise.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.groupBy('a).select('a, 'b.sum as 'd)
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the type of aggregation and the number of distinct grouping keys. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
    	<td>
        <strong>GroupBy Window Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>Groups and aggregates a table on a <a href="#group-windows">group window</a> and possibly one or more grouping keys.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result: Table = orders
    .window(Tumble over 5.minutes on 'rowtime as 'w) // define window
    .groupBy('a, 'w) // group by key and window
    .select('a, w.start, 'w.end, 'w.rowtime, 'b.sum as 'd) // access window properties and aggregate
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Over Window Aggregation</strong><br>
        <span class="label label-primary">Streaming</span>
      </td>
    	<td>
       <p>Similar to a SQL OVER clause. Over window aggregates are computed for each row, based on a window (range) of preceding and succeeding rows. See the <a href="#over-windows">over windows section</a> for more details.</p>
       {% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result: Table = orders
    // define window
    .window(Over
      partitionBy 'a
      orderBy 'rowtime
      preceding UNBOUNDED_RANGE
      following CURRENT_RANGE
      as 'w)
    .select('a, 'b.avg over 'w, 'b.max over 'w, 'b.min over 'w) // sliding aggregate
{% endhighlight %}
       <p><b>Note:</b> All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single <a href="streaming/time_attributes.html">time attribute</a>.</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span> <br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to a SQL DISTINCT AGGREGATION clause such as COUNT(DISTINCT a). Distinct aggregation declares that an aggregation function (built-in or user-defined) is only applied on distinct input values. Distinct can be applied to <b>GroupBy Aggregation</b>, <b>GroupBy Window Aggregation</b> and <b>Over Window Aggregation</b>.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders");
// Distinct aggregation on group by
val groupByDistinctResult = orders
    .groupBy('a)
    .select('a, 'b.sum.distinct as 'd)
// Distinct aggregation on time window group by
val groupByWindowDistinctResult = orders
    .window(Tumble over 5.minutes on 'rowtime as 'w).groupBy('a, 'w)
    .select('a, 'b.sum.distinct as 'd)
// Distinct aggregation on over window
val result = orders
    .window(Over
        partitionBy 'a
        orderBy 'rowtime
        preceding UNBOUNDED_RANGE
        as 'w)
    .select('a, 'b.avg.distinct over 'w, 'b.max over 'w, 'b.min over 'w)
{% endhighlight %}
        <p>User-defined aggregation function can also be used with DISTINCT modifiers. To calculate the aggregate results only for distinct values, simply add the distinct modifier towards the aggregation function. </p>
{% highlight scala %}
val orders: Table = tEnv.scan("Orders");

// Use distinct aggregation for user-defined aggregate functions
val myUdagg = new MyUdagg();
orders.groupBy('users).select('users, myUdagg.distinct('points) as 'myDistinctResult);
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct fields. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns records with distinct value combinations.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.distinct()
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct fields. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>
<div data-lang="python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>GroupBy Aggregation</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span><br>
        <span class="label label-info">结果持续更新</span>
      </td>
      <td>
        <p>类似于SQL的GROUP BY子句。将数据按照指定字段进行分组，之后对各组内数据执行聚合操作。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.group_by("a").select("a, b.sum as d")
{% endhighlight %}
        <p><b>注意：</b> 对于流式查询，计算查询结果所需的状态（state）可能会无限增长，具体情况取决于聚合操作的类型和分组的数量。您可能需要在查询配置中设置状态保留时间，以防止状态过大。详情请看<a href="streaming/query_configuration.html">查询配置</a>。</p>
      </td>
    </tr>
    <tr>
    	<td>
        <strong>GroupBy Window Aggregation</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
    	<td>
        <p>在一个窗口上分组和聚合数据，可包含其它分组字段。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.window(Tumble.over("5.minutes").on("rowtime").alias("w")) \ 
               .group_by("a, w") \
               .select("a, w.start, w.end, w.rowtime, b.sum as d")
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Over Window Aggregation</strong><br>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
       <p>类似于SQL中的OVER开窗函数。Over窗口聚合对每一行都进行一次聚合计算，聚合的对象是以当前行的位置为基准，向前向后取一个区间范围内的所有数据。详情请见<a href="#over-windows">Over窗口</a>一节。</p>
{% highlight python %}
orders = table_env.scan("Orders")
result = orders.over_window(Over.partition_by("a").order_by("rowtime")
                            .preceding("UNBOUNDED_RANGE").following("CURRENT_RANGE")
                            .alias("w")) \
    .select("a, b.avg over w, b.max over w, b.min over w")
{% endhighlight %}
       <p><b>注意：</b> 所有的聚合操作必须在同一个窗口上定义，即分组，排序，范围等属性必须一致。目前，窗口区间范围的向前（PRECEDING）取值没有限制，可以为无界（UNBOUNDED），但是向后（FOLLOWING）只支持当前行（CURRENT ROW），其它向后范围取值暂不支持。排序（ORDER BY）属性必须指定单个<a href="streaming/time_attributes.html">时间属性</a>。</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct Aggregation</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span> <br>
        <span class="label label-info">结果持续更新</span>
      </td>
      <td>
        <p>类似于SQL聚合函数中的的DISTINCT关键字比如COUNT(DISTINCT a)。带有distinct标记的聚合函数只会接受不重复的输入，重复输入将被丢弃。这个去重特性可以在<b>分组聚合（GroupBy Aggregation）</b>，<b>分组窗口聚合（GroupBy Window Aggregation）</b>以及<b>Over窗口聚合（Over Window Aggregation）</b>上使用。</p>
{% highlight python %}
orders = table_env.scan("Orders")
# Distinct aggregation on group by
group_by_distinct_result = orders.group_by("a") \
                                 .select("a, b.sum.distinct as d")
# Distinct aggregation on time window group by
group_by_window_distinct_result = orders.window(
    Tumble.over("5.minutes").on("rowtime").alias("w")).groupBy("a, w") \
    .select("a, b.sum.distinct as d")
# Distinct aggregation on over window
result = orders.over_window(Over
                       .partition_by("a")
                       .order_by("rowtime")
                       .preceding("UNBOUNDED_RANGE")
                       .alias("w")) \
                       .select(
                       "a, b.avg.distinct over w, b.max over w, b.min over w")
{% endhighlight %}
        <p><b>注意：</b> 对于流式查询，计算查询结果所需的状态（state）可能会无限增长，具体情况取决于执行去重判断时参与判断的字段的数量。您可能需要在查询配置中设置状态保留时间，以防止状态过大。详情请看<a href="streaming/query_configuration.html">查询配置</a>。</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span> <br>
        <span class="label label-info">结果持续更新</span>
      </td>
      <td>
        <p>类似于SQL中的DISTINCT子句。返回去重后的数据。</p>
{% highlight java %}
orders = table_env.scan("Orders")
result = orders.distinct()
{% endhighlight %}
        <p><b>注意：</b> 对于流式查询，计算查询结果所需的状态（state）可能会无限增长，具体情况取决于执行去重判断时参与判断的字段的数量。您可能需要在查询配置中设置状态保留时间，以防止状态过大。详情请看<a href="streaming/query_configuration.html">查询配置</a>。</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

### Joins

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
      <td>
        <strong>Inner Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined through join operator or using a where or filter operator.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.join(right).where("a = d").select("a, b, e");
{% endhighlight %}
<p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
        <strong>Outer Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to SQL LEFT/RIGHT/FULL OUTER JOIN clauses. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");

Table leftOuterResult = left.leftOuterJoin(right, "a = d").select("a, b, e");
Table rightOuterResult = left.rightOuterJoin(right, "a = d").select("a, b, e");
Table fullOuterResult = left.fullOuterJoin(right, "a = d").select("a, b, e");
{% endhighlight %}
<p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Time-windowed Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><b>Note:</b> Time-windowed joins are a subset of regular joins that can be processed in a streaming fashion.</p>

        <p>A time-windowed join requires at least one equi-join predicate and a join condition that bounds the time on both sides. Such a condition can be defined by two appropriate range predicates (<code>&lt;, &lt;=, &gt;=, &gt;</code>) or a single equality predicate that compares <a href="streaming/time_attributes.html">time attributes</a> of the same type (i.e., processing time or event time) of both input tables.</p>
        <p>For example, the following predicates are valid window join conditions:</p>

        <ul>
          <li><code>ltime === rtime</code></li>
          <li><code>ltime &gt;= rtime &amp;&amp; ltime &lt; rtime + 10.minutes</code></li>
        </ul>

{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c, ltime.rowtime");
Table right = tableEnv.fromDataSet(ds2, "d, e, f, rtime.rowtime");

Table result = left.join(right)
  .where("a = d && ltime >= rtime - 5.minutes && ltime < rtime + 10.minutes")
  .select("a, b, e, ltime");
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Inner Join with Table Function</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function. A row of the left (outer) table is dropped, if its table function call returns an empty result.
        </p>
{% highlight java %}
// register User-Defined Table Function
TableFunction<String> split = new MySplitUDTF();
tableEnv.registerFunction("split", split);

// join
Table orders = tableEnv.scan("Orders");
Table result = orders
    .joinLateral("split(c).as(s, t, v)")
    .select("a, b, s, t, v");
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Left Outer Join with Table Function</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function. If a table function call returns an empty result, the corresponding outer row is preserved and the result padded with null values.</p>
        <p><b>Note:</b> Currently, the predicate of a table function left outer join can only be empty or literal <code>true</code>.</p>
{% highlight java %}
// register User-Defined Table Function
TableFunction<String> split = new MySplitUDTF();
tableEnv.registerFunction("split", split);

// join
Table orders = tableEnv.scan("Orders");
Table result = orders
    .leftOuterJoinLateral("split(c).as(s, t, v)")
    .select("a, b, s, t, v");
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Join with Temporal Table</strong><br>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><a href="streaming/temporal_tables.html">Temporal tables</a> are tables that track changes over time.</p>
        <p>A <a href="streaming/temporal_tables.html#temporal-table-functions">temporal table function</a> provides access to the state of a temporal table at a specific point in time.
        The syntax to join a table with a temporal table function is the same as in <i>Inner Join with Table Function</i>.</p>

        <p>Currently only inner joins with temporal tables are supported.</p>
{% highlight java %}
Table ratesHistory = tableEnv.scan("RatesHistory");

// register temporal table function with a time attribute and primary key
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction(
    "r_proctime",
    "r_currency");
tableEnv.registerFunction("rates", rates);

// join with "Orders" based on the time attribute and key
Table orders = tableEnv.scan("Orders");
Table result = orders
    .joinLateral("rates(o_proctime)", "o_currency = r_currency")
{% endhighlight %}
        <p>For more information please check the more detailed <a href="streaming/temporal_tables.html">temporal tables concept description</a>.</p>
      </td>
    </tr>

  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>

  	<tr>
      <td>
        <strong>Inner Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined through join operator or using a where or filter operator.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'd, 'e, 'f)
val result = left.join(right).where('a === 'd).select('a, 'b, 'e)
{% endhighlight %}
<p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Outer Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to SQL LEFT/RIGHT/FULL OUTER JOIN clauses. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight scala %}
val left = tableEnv.fromDataSet(ds1, 'a, 'b, 'c)
val right = tableEnv.fromDataSet(ds2, 'd, 'e, 'f)

val leftOuterResult = left.leftOuterJoin(right, 'a === 'd).select('a, 'b, 'e)
val rightOuterResult = left.rightOuterJoin(right, 'a === 'd).select('a, 'b, 'e)
val fullOuterResult = left.fullOuterJoin(right, 'a === 'd).select('a, 'b, 'e)
{% endhighlight %}
<p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Time-windowed Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><b>Note:</b> Time-windowed joins are a subset of regular joins that can be processed in a streaming fashion.</p>

        <p>A time-windowed join requires at least one equi-join predicate and a join condition that bounds the time on both sides. Such a condition can be defined by two appropriate range predicates (<code>&lt;, &lt;=, &gt;=, &gt;</code>) or a single equality predicate that compares <a href="streaming/time_attributes.html">time attributes</a> of the same type (i.e., processing time or event time) of both input tables.</p>
        <p>For example, the following predicates are valid window join conditions:</p>

        <ul>
          <li><code>'ltime === 'rtime</code></li>
          <li><code>'ltime &gt;= 'rtime &amp;&amp; 'ltime &lt; 'rtime + 10.minutes</code></li>
        </ul>

{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c, 'ltime.rowtime)
val right = ds2.toTable(tableEnv, 'd, 'e, 'f, 'rtime.rowtime)

val result = left.join(right)
  .where('a === 'd && 'ltime >= 'rtime - 5.minutes && 'ltime < 'rtime + 10.minutes)
  .select('a, 'b, 'e, 'ltime)
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Inner Join with Table Function</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function. A row of the left (outer) table is dropped, if its table function call returns an empty result.
        </p>
        {% highlight scala %}
// instantiate User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .joinLateral(split('c) as ('s, 't, 'v))
    .select('a, 'b, 's, 't, 'v)
{% endhighlight %}
        </td>
    </tr>
    <tr>
    	<td>
        <strong>Left Outer Join with Table Function</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span></td>
    	<td>
        <p>Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function. If a table function call returns an empty result, the corresponding outer row is preserved and the result padded with null values.</p>
        <p><b>Note:</b> Currently, the predicate of a table function left outer join can only be empty or literal <code>true</code>.</p>
{% highlight scala %}
// instantiate User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .leftOuterJoinLateral(split('c) as ('s, 't, 'v))
    .select('a, 'b, 's, 't, 'v)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Join with Temporal Table</strong><br>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><a href="streaming/temporal_tables.html">Temporal tables</a> are tables that track their changes over time.</p>
        <p>A <a href="streaming/temporal_tables.html#temporal-table-functions">temporal table function</a> provides access to the state of a temporal table at a specific point in time.
        The syntax to join a table with a temporal table function is the same as in <i>Inner Join with Table Function</i>.</p>

        <p>Currently only inner joins with temporal tables are supported.</p>
{% highlight scala %}
val ratesHistory = tableEnv.scan("RatesHistory")

// register temporal table function with a time attribute and primary key
val rates = ratesHistory.createTemporalTableFunction('r_proctime, 'r_currency)

// join with "Orders" based on the time attribute and key
val orders = tableEnv.scan("Orders")
val result = orders
    .joinLateral(rates('o_rowtime), 'r_currency === 'o_currency)
{% endhighlight %}
        <p>For more information please check the more detailed <a href="streaming/temporal_tables.html">temporal tables concept description</a>.</p>
      </td>
    </tr>

  </tbody>
</table>
</div>
<div data-lang="python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
      <td>
        <strong>Inner Join</strong><br>
        <span class="label label-primary">批处理</span>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>类似于SQL的JOIN子句。对两张表执行内连接操作。两张表必须具有不同的字段名称，并且必须在join方法或者随后的where或filter方法中定义至少一个等值连接条件。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("d, e, f")
result = left.join(right).where("a = d").select("a, b, e")
{% endhighlight %}
<p><b>注意：</b> 对于流式查询，计算查询结果所需的状态（state）可能会无限增长，具体取决于不重复的输入行的数量。您可能需要在查询配置中设置状态保留时间，以防止状态过大。详情请看<a href="streaming/query_configuration.html">查询配置</a>。</p>
      </td>
    </tr>

    <tr>
      <td>
        <strong>Outer Join</strong><br>
        <span class="label label-primary">批处理</span>
        <span class="label label-primary">流处理</span>
        <span class="label label-info">结果持续更新</span>
      </td>
      <td>
        <p>类似于SQL的LEFT/RIGHT/FULL OUTER JOIN子句。对两张表执行外连接操作。两张表必须具有不同的字段名称，并且必须定义至少一个等值连接条件。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("d, e, f")

left_outer_result = left.left_outer_join(right, "a = d").select("a, b, e")
right_outer_result = left.right_outer_join(right, "a = d").select("a, b, e")
full_outer_result = left.full_outer_join(right, "a = d").select("a, b, e")
{% endhighlight %}
<p><b>注意：</b> 对于流式查询，计算查询结果所需的状态（state）可能会无限增长，具体取决于不重复的输入行的数量。您可能需要在查询配置中设置状态保留时间，以防止状态过大。详情请看<a href="streaming/query_configuration.html">查询配置</a>。</p>
      </td>
    </tr>
    <tr>
      <td><strong>Time-windowed Join</strong><br>
        <span class="label label-primary">批处理</span>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>Python API暂不支持。</p>
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Inner Join with Table Function</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
    	<td>
        <p>将一张表与一个表函数的执行结果执行内连接操作。左表的每一行都会进行一次表函数调用，调用将会返回0个，1个或多个结果，再与这些结果执行连接操作。如果一行数据对应的表函数调用返回了一个空的结果集，则这行数据会被丢弃。
        </p>
{% highlight python %}
# register Java User-Defined Table Function
table_env.register_java_function("split", "com.my.udf.MySplitUDTF")

# join
orders = table_env.scan("Orders")
result = orders.join_lateral("split(c).as(s, t, v)").select("a, b, s, t, v")
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Left Outer Join with Table Function</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>将一张表与一个表函数的执行结果执行左连接操作。左表的每一行都会进行一次表函数调用，调用将会返回0个，1个或多个结果，再与这些结果执行连接操作。如果一行数据对应的表函数调用返回了一个空的结果集，这行数据依然会被保留，对应的右表数值用null(python为None)填充。</p>
        <p><b>注意：</b>目前，表函数的左连接操作的连接条件(join predicate)只能为空或者为"true"常量。</p>
{% highlight python %}
# register Java User-Defined Table Function
table_env.register_java_function("split", "com.my.udf.MySplitUDTF")

# join
orders = table_env.scan("Orders")
result = orders.left_outer_join_lateral("split(c).as(s, t, v)").select("a, b, s, t, v")
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Join with Temporal Table</strong><br>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>Python API暂不支持。</p>
      </td>
    </tr>

  </tbody>
</table>

</div>
</div>

{% top %}

### Set Operations

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
      <td>
        <strong>Union</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL UNION clause. Unions two tables with duplicate records removed. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.union(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>UnionAll</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL UNION ALL clause. Unions two tables. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.unionAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Intersect</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL INTERSECT clause. Intersect returns records that exist in both tables. If a record is present one or both tables more than once, it is returned just once, i.e., the resulting table has no duplicate records. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.intersect(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>IntersectAll</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL INTERSECT ALL clause. IntersectAll returns records that exist in both tables. If a record is present in both tables more than once, it is returned as many times as it is present in both tables, i.e., the resulting table might have duplicate records. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.intersectAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Minus</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not exist in the right table. Duplicate records in the left table are returned exactly once, i.e., duplicates are removed. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.minus(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>MinusAll</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in the right table. A record that is present n times in the left table and m times in the right table is returned (n - m) times, i.e., as many duplicates as are present in the right table are removed. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.minusAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>In</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL IN clause. In returns true if an expression exists in a given table sub-query. The sub-query table must consist of one column. This column must have the same data type as the expression.</p>
{% highlight java %}
Table left = ds1.toTable(tableEnv, "a, b, c");
Table right = ds2.toTable(tableEnv, "a");

// using implicit registration
Table result = left.select("a, b, c").where("a.in(" + right + ")");

// using explicit registration
tableEnv.registerTable("RightTable", right);
Table result = left.select("a, b, c").where("a.in(RightTable)");
{% endhighlight %}

        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
      <td>
        <strong>Union</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL UNION clause. Unions two tables with duplicate records removed, both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'a, 'b, 'c)
val result = left.union(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>UnionAll</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>

      </td>
      <td>
        <p>Similar to a SQL UNION ALL clause. Unions two tables, both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'a, 'b, 'c)
val result = left.unionAll(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Intersect</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL INTERSECT clause. Intersect returns records that exist in both tables. If a record is present in one or both tables more than once, it is returned just once, i.e., the resulting table has no duplicate records. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'e, 'f, 'g)
val result = left.intersect(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>IntersectAll</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL INTERSECT ALL clause. IntersectAll returns records that exist in both tables. If a record is present in both tables more than once, it is returned as many times as it is present in both tables, i.e., the resulting table might have duplicate records. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'e, 'f, 'g)
val result = left.intersectAll(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Minus</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not exist in the right table. Duplicate records in the left table are returned exactly once, i.e., duplicates are removed. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'a, 'b, 'c)
val result = left.minus(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>MinusAll</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in the right table. A record that is present n times in the left table and m times in the right table is returned (n - m) times, i.e., as many duplicates as are present in the right table are removed. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'a, 'b, 'c)
val result = left.minusAll(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>In</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to a SQL IN clause. In returns true if an expression exists in a given table sub-query. The sub-query table must consist of one column. This column must have the same data type as the expression.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c)
val right = ds2.toTable(tableEnv, 'a)
val result = left.select('a, 'b, 'c).where('a.in(right))
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>

  </tbody>
</table>
</div>
<div data-lang="python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
      <td>
        <strong>Union</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
        <p>类似于SQL的UNION子句。将两张表组合成一张表，这张表拥有二者去除重复后的全部数据。两张表的字段和类型必须完全一致。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("a, b, c")
result = left.union(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>UnionAll</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>类似于SQL的UNION ALL子句。将两张表组合成一张表，这张表拥有二者的全部数据。两张表的字段和类型必须完全一致。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("a, b, c")
result = left.union_all(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Intersect</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
        <p>类似于SQL的INTERSECT子句。Intersect返回在两张表中都存在的数据。如果一个记录在两张表中不止出现一次，则只返回一次，即结果表没有重复记录。两张表的字段和类型必须完全一致。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("a, b, c")
result = left.intersect(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>IntersectAll</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
        <p>类似于SQL的INTERSECT ALL子句。IntersectAll返回在两张表中都存在的数据。如果一个记录在两张表中不止出现一次，则按照它在两张表中都出现的次数返回，即结果表可能包含重复数据。两张表的字段和类型必须完全一致。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("a, b, c")
result = left.intersect_all(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Minus</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
        <p>类似于SQL的EXCEPT子句。Minus返回仅存在于左表，不存在于右表中的数据。左表中的相同数据只会返回一次，即数据会被去重。两张表的字段和类型必须完全一致。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("a, b, c")
result = left.minus(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>MinusAll</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
        <p>类似于SQL的EXCEPT ALL子句。MinusAll返回仅存在于左表，不存在于右表中的数据。如果一条数据在左表中出现了n次，在右表中出现了m次，最终这条数据将会被返回(n - m)次，即按右表中出现的次数来移除数据。两张表的字段和类型必须完全一致。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("a, b, c")
result = left.minus_all(right)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>In</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>类似于SQL的IN子句。如果In左边表达式的值在给定的子查询结果中则返回true。子查询的结果必须为单列。此列数据类型必须和表达式一致。</p>
{% highlight python %}
left = table_env.scan("Source1").select("a, b, c")
right = table_env.scan("Source2").select("a")

# using implicit registration
result = left.select("a, b, c").where("a.in(%s)" % right)

# using explicit registration
table_env.register_table("RightTable", right)
result = left.select("a, b, c").where("a.in(RightTable)")
{% endhighlight %}

        <p><b>注意：</b> 对于流式查询，这个操作会被替换成一个连接操作和一个分组操作。计算查询结果所需的状态（state）可能会无限增长，具体取决于不重复的输入行的数量。您可能需要在查询配置中设置状态保留时间，以防止状态过大。详情请看<a href="streaming/query_configuration.html">查询配置</a>。</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

### OrderBy, Offset & Fetch

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Order By</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns records globally sorted across all parallel partitions.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.orderBy("a.asc");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Offset &amp; Fetch</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to the SQL OFFSET and FETCH clauses. Offset and Fetch limit the number of records returned from a sorted result. Offset and Fetch are technically part of the Order By operator and thus must be preceded by it.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");

// returns the first 5 records from the sorted result
Table result1 = in.orderBy("a.asc").fetch(5);

// skips the first 3 records and returns all following records from the sorted result
Table result2 = in.orderBy("a.asc").offset(3);

// skips the first 10 records and returns the next 5 records from the sorted result
Table result3 = in.orderBy("a.asc").offset(10).fetch(5);
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Order By</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns records globally sorted across all parallel partitions.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c)
val result = in.orderBy('a.asc)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Offset &amp; Fetch</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
        <p>Similar to the SQL OFFSET and FETCH clauses. Offset and Fetch limit the number of records returned from a sorted result. Offset and Fetch are technically part of the Order By operator and thus must be preceded by it.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c)

// returns the first 5 records from the sorted result
val result1: Table = in.orderBy('a.asc).fetch(5)

// skips the first 3 records and returns all following records from the sorted result
val result2: Table = in.orderBy('a.asc).offset(3)

// skips the first 10 records and returns the next 5 records from the sorted result
val result3: Table = in.orderBy('a.asc).offset(10).fetch(5)
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>
<div data-lang="python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Order By</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
        <p>类似于SQL的ORDER BY子句。返回包括所有子并发分区内所有数据的全局排序结果。</p>
{% highlight python %}
in = table_env.scan("Source1").select("a, b, c")
result = in.order_by("a.asc")
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Offset &amp; Fetch</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
        <p>类似于SQL的OFFSET和FETCH子句。Offset和Fetch从已排序的结果中返回指定数量的数据。Offset和Fetch在技术上是Order By操作的一部分，因此必须紧跟其后出现。</p>
{% highlight python %}
in = table_env.scan("Source1").select("a, b, c")

# returns the first 5 records from the sorted result
result1 = in.order_by("a.asc").fetch(5)

# skips the first 3 records and returns all following records from the sorted result
result2 = in.order_by("a.asc").offset(3)

# skips the first 10 records and returns the next 5 records from the sorted result
result3 = in.order_by("a.asc").offset(10).fetch(5)
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

### Insert

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Insert Into</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to the INSERT INTO clause in a SQL query. Performs a insertion into a registered output table.</p>

        <p>Output tables must be registered in the TableEnvironment (see <a href="common.html#register-a-tablesink">Register a TableSink</a>). Moreover, the schema of the registered table must match the schema of the query.</p>

{% highlight java %}
Table orders = tableEnv.scan("Orders");
orders.insertInto("OutOrders");
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Insert Into</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Similar to the INSERT INTO clause in a SQL query. Performs a insertion into a registered output table.</p>

        <p>Output tables must be registered in the TableEnvironment (see <a href="common.html#register-a-tablesink">Register a TableSink</a>). Moreover, the schema of the registered table must match the schema of the query.</p>

{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
orders.insertInto("OutOrders")
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>
<div data-lang="python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Insert Into</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>类似于SQL请求中的INSERT INTO子句。将数据输出到一个已注册的输出表中。</p>

        <p>输出表必须先在TableEnvironment中注册（详见<a href="common.html#register-a-tablesink">注册一个TableSink</a>）。此外，注册的表的模式（schema）必须和请求的结果的模式（schema）相匹配。</p>

{% highlight python %}
orders = table_env.scan("Orders");
orders.insert_into("OutOrders");
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

{% top %}

### Group Windows

Group window aggregates group rows into finite groups based on time or row-count intervals and evaluate aggregation functions once per group. For batch tables, windows are a convenient shortcut to group records by time intervals.

Windows are defined using the `window(w: GroupWindow)` clause and require an alias, which is specified using the `as` clause. In order to group a table by a window, the window alias must be referenced in the `groupBy(...)` clause like a regular grouping attribute.
The following example shows how to define a window aggregation on a table.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([GroupWindow w].as("w"))  // define window with alias w
  .groupBy("w")  // group the table by window w
  .select("b.sum");  // aggregate
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val table = input
  .window([w: GroupWindow] as 'w)  // define window with alias w
  .groupBy('w)   // group the table by window w
  .select('b.sum)  // aggregate
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# define window with alias w, group the table by window w, then aggregate
table = input.window([GroupWindow w].alias("w")) \
             .group_by("w").select("b.sum")
{% endhighlight %}
</div>
</div>

In streaming environments, window aggregates can only be computed in parallel if they group on one or more attributes in addition to the window, i.e., the `groupBy(...)` clause references a window alias and at least one additional attribute. A `groupBy(...)` clause that only references a window alias (such as in the example above) can only be evaluated by a single, non-parallel task.
The following example shows how to define a window aggregation with additional grouping attributes.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([GroupWindow w].as("w"))  // define window with alias w
  .groupBy("w, a")  // group the table by attribute a and window w
  .select("a, b.sum");  // aggregate
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val table = input
  .window([w: GroupWindow] as 'w) // define window with alias w
  .groupBy('w, 'a)  // group the table by attribute a and window w
  .select('a, 'b.sum)  // aggregate
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# define window with alias w, group the table by attribute a and window w,
# then aggregate
table = input.window([GroupWindow w].alias("w")) \
             .group_by("w, a").select("b.sum")
{% endhighlight %}
</div>
</div>

Window properties such as the start, end, or rowtime timestamp of a time window can be added in the select statement as a property of the window alias as `w.start`, `w.end`, and `w.rowtime`, respectively. The window start and rowtime timestamps are the inclusive lower and upper window boundaries. In contrast, the window end timestamp is the exclusive upper window boundary. For example a tumbling window of 30 minutes that starts at 2pm would have `14:00:00.000` as start timestamp, `14:29:59.999` as rowtime timestamp, and `14:30:00.000` as end timestamp.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([GroupWindow w].as("w"))  // define window with alias w
  .groupBy("w, a")  // group the table by attribute a and window w
  .select("a, w.start, w.end, w.rowtime, b.count"); // aggregate and add window start, end, and rowtime timestamps
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val table = input
  .window([w: GroupWindow] as 'w)  // define window with alias w
  .groupBy('w, 'a)  // group the table by attribute a and window w
  .select('a, 'w.start, 'w.end, 'w.rowtime, 'b.count) // aggregate and add window start, end, and rowtime timestamps
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# define window with alias w, group the table by attribute a and window w,
# then aggregate and add window start, end, and rowtime timestamps
table = input.window([GroupWindow w].alias("w")) \
             .group_by("w, a") \
             .select("a, w.start, w.end, w.rowtime, b.count")
{% endhighlight %}
</div>
</div>

The `Window` parameter defines how rows are mapped to windows. `Window` is not an interface that users can implement. Instead, the Table API provides a set of predefined `Window` classes with specific semantics, which are translated into underlying `DataStream` or `DataSet` operations. The supported window definitions are listed below.

#### Tumble (Tumbling Windows)

A tumbling window assigns rows to non-overlapping, continuous windows of fixed length. For example, a tumbling window of 5 minutes groups rows in 5 minutes intervals. Tumbling windows can be defined on event-time, processing-time, or on a row-count.

Tumbling windows are defined by using the `Tumble` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="streaming/time_attributes.html">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Tumbling Event-time Window
.window(Tumble.over("10.minutes").on("rowtime").as("w"));

// Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble.over("10.minutes").on("proctime").as("w"));

// Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble.over("10.rows").on("proctime").as("w"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Tumbling Event-time Window
.window(Tumble over 10.minutes on 'rowtime as 'w)

// Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.minutes on 'proctime as 'w)

// Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.rows on 'proctime as 'w)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# Tumbling Event-time Window
.window(Tumble.over("10.minutes").on("rowtime").alias("w"))

# Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble.over("10.minutes").on("proctime").alias("w"))

# Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble.over("10.rows").on("proctime").alias("w"));
{% endhighlight %}
</div>
</div>

#### Slide (Sliding Windows)

A sliding window has a fixed size and slides by a specified slide interval. If the slide interval is smaller than the window size, sliding windows are overlapping. Thus, rows can be assigned to multiple windows. For example, a sliding window of 15 minutes size and 5 minute slide interval assigns each row to 3 different windows of 15 minute size, which are evaluated in an interval of 5 minutes. Sliding windows can be defined on event-time, processing-time, or on a row-count.

Sliding windows are defined by using the `Slide` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length of the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>Defines the slide interval, either as time or row-count interval. The slide interval must be of the same type as the size interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="streaming/time_attributes.html">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Sliding Event-time Window
.window(Slide.over("10.minutes").every("5.minutes").on("rowtime").as("w"));

// Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide.over("10.minutes").every("5.minutes").on("proctime").as("w"));

// Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide.over("10.rows").every("5.rows").on("proctime").as("w"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Sliding Event-time Window
.window(Slide over 10.minutes every 5.minutes on 'rowtime as 'w)

// Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide over 10.minutes every 5.minutes on 'proctime as 'w)

// Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide over 10.rows every 5.rows on 'proctime as 'w)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# Sliding Event-time Window
.window(Slide.over("10.minutes").every("5.minutes").on("rowtime").alias("w"))

# Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide.over("10.minutes").every("5.minutes").on("proctime").alias("w"))

# Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide.over("10.rows").every("5.rows").on("proctime").alias("w"))
{% endhighlight %}
</div>
</div>

#### Session (Session Windows)

Session windows do not have a fixed size but their bounds are defined by an interval of inactivity, i.e., a session window is closes if no event appears for a defined gap period. For example a session window with a 30 minute gap starts when a row is observed after 30 minutes inactivity (otherwise the row would be added to an existing window) and is closed if no row is added within 30 minutes. Session windows can work on event-time or processing-time.

A session window is defined by using the `Session` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>withGap</code></td>
      <td>Defines the gap between two windows as time interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="streaming/time_attributes.html">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Session Event-time Window
.window(Session.withGap("10.minutes").on("rowtime").as("w"));

// Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session.withGap("10.minutes").on("proctime").as("w"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Session Event-time Window
.window(Session withGap 10.minutes on 'rowtime as 'w)

// Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session withGap 10.minutes on 'proctime as 'w)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# Session Event-time Window
.window(Session.with_gap("10.minutes").on("rowtime").alias("w"))

# Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session.with_gap("10.minutes").on("proctime").alias("w"))
{% endhighlight %}
</div>
</div>

{% top %}

### Over Windows

Over window aggregates are known from standard SQL (`OVER` clause) and defined in the `SELECT` clause of a query. Unlike group windows, which are specified in the `GROUP BY` clause, over windows do not collapse rows. Instead over window aggregates compute an aggregate for each input row over a range of its neighboring rows.

Over windows are defined using the `window(w: OverWindow*)` clause (在Python API中为`over_window(*OverWindow)`) and referenced via an alias in the `select()` method. The following example shows how to define an over window aggregation on a table.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([OverWindow w].as("w"))           // define over window with alias w
  .select("a, b.sum over w, c.min over w"); // aggregate over the over window w
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val table = input
  .window([w: OverWindow] as 'w)              // define over window with alias w
  .select('a, 'b.sum over 'w, 'c.min over 'w) // aggregate over the over window w
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# define over window with alias w and aggregate over the over window w
table = input.over_window([OverWindow w].alias("w")) \
    .select("a, b.sum over w, c.min over w")
{% endhighlight %}
</div>
</div>

The `OverWindow` defines a range of rows over which aggregates are computed. `OverWindow` is not an interface that users can implement. Instead, the Table API provides the `Over` class to configure the properties of the over window. Over windows can be defined on event-time or processing-time and on ranges specified as time interval or row-count. The supported over window definitions are exposed as methods on `Over` (and other classes) and are listed below:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Required</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>partitionBy</code></td>
      <td>Optional</td>
      <td>
        <p>Defines a partitioning of the input on one or more attributes. Each partition is individually sorted and aggregate functions are applied to each partition separately.</p>

        <p><b>Note:</b> In streaming environments, over window aggregates can only be computed in parallel if the window includes a partition by clause. Without <code>partitionBy(...)</code> the stream is processed by a single, non-parallel task.</p>
      </td>
    </tr>
    <tr>
      <td><code>orderBy</code></td>
      <td>Required</td>
      <td>
        <p>Defines the order of rows within each partition and thereby the order in which the aggregate functions are applied to rows.</p>

        <p><b>Note:</b> For streaming queries this must be a <a href="streaming/time_attributes.html">declared event-time or processing-time time attribute</a>. Currently, only a single sort attribute is supported.</p>
      </td>
    </tr>
    <tr>
      <td><code>preceding</code></td>
      <td>Optional</td>
      <td>
        <p>Defines the interval of rows that are included in the window and precede the current row. The interval can either be specified as time or row-count interval.</p>

        <p><a href="tableApi.html#bounded-over-windows">Bounded over windows</a> are specified with the size of the interval, e.g., <code>10.minutes</code> for a time interval or <code>10.rows</code> for a row-count interval.</p>

        <p><a href="tableApi.html#unbounded-over-windows">Unbounded over windows</a> are specified using a constant, i.e., <code>UNBOUNDED_RANGE</code> for a time interval or <code>UNBOUNDED_ROW</code> for a row-count interval. Unbounded over windows start with the first row of a partition.</p>

        <p>If the <code>preceding</code> clause is omitted, <code>UNBOUNDED_RANGE</code> and <code>CURRENT_RANGE</code> are used as the default <code>preceding</code> and <code>following</code> for the window.</p>
      </td>
    </tr>
    <tr>
      <td><code>following</code></td>
      <td>Optional</td>
      <td>
        <p>Defines the window interval of rows that are included in the window and follow the current row. The interval must be specified in the same unit as the preceding interval (time or row-count).</p>

        <p>At the moment, over windows with rows following the current row are not supported. Instead you can specify one of two constants:</p>

        <ul>
          <li><code>CURRENT_ROW</code> sets the upper bound of the window to the current row.</li>
          <li><code>CURRENT_RANGE</code> sets the upper bound of the window to sort key of the current row, i.e., all rows with the same sort key as the current row are included in the window.</li>
        </ul>

        <p>If the <code>following</code> clause is omitted, the upper bound of a time interval window is defined as <code>CURRENT_RANGE</code> and the upper bound of a row-count interval window is defined as <code>CURRENT_ROW</code>.</p>
      </td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Required</td>
      <td>
        <p>Assigns an alias to the over window. The alias is used to reference the over window in the following <code>select()</code> clause.</p>
      </td>
    </tr>
  </tbody>
</table>

**Note:** Currently, all aggregation functions in the same `select()` call must be computed of the same over window.

#### Unbounded Over Windows
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Unbounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy("a").orderBy("rowtime").preceding("unbounded_range").as("w"));

// Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy("a").orderBy("proctime").preceding("unbounded_range").as("w"));

// Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy("a").orderBy("rowtime").preceding("unbounded_row").as("w"));

// Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy("a").orderBy("proctime").preceding("unbounded_row").as("w"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Unbounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)

// Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)

// Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)

// Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# Unbounded Event-time over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by("a").order_by("rowtime").preceding("unbounded_range").alias("w"))

# Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by("a").order_by("proctime").preceding("unbounded_range").alias("w"))

# Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by("a").order_by("rowtime").preceding("unbounded_row").alias("w"))
 
# Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by("a").order_by("proctime").preceding("unbounded_row").alias("w"))
{% endhighlight %}
</div>
</div>

#### Bounded Over Windows
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Bounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy("a").orderBy("rowtime").preceding("1.minutes").as("w"))

// Bounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy("a").orderBy("proctime").preceding("1.minutes").as("w"))

// Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy("a").orderBy("rowtime").preceding("10.rows").as("w"))

// Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy("a").orderBy("proctime").preceding("10.rows").as("w"))
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Bounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy 'a orderBy 'rowtime preceding 1.minutes as 'w)

// Bounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy 'a orderBy 'proctime preceding 1.minutes as 'w)

// Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy 'a orderBy 'rowtime preceding 10.rows as 'w)

// Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy 'a orderBy 'proctime preceding 10.rows as 'w)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# Bounded Event-time over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by("a").order_by("rowtime").preceding("1.minutes").alias("w"))

# Bounded Processing-time over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by("a").order_by("proctime").preceding("1.minutes").alias("w"))

# Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by("a").order_by("rowtime").preceding("10.rows").alias("w"))
 
# Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by("a").order_by("proctime").preceding("10.rows").alias("w"))
{% endhighlight %}
</div>
</div>

{% top %}

### Row-based Operations

The row-based operations generate outputs with multiple columns.
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Map</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Performs a map operation with a user-defined scalar function or built-in scalar function. The output will be flattened if the output type is a composite type.</p>
{% highlight java %}
public class MyMapFunction extends ScalarFunction {
    public Row eval(String a) {
        return Row.of(a, "pre-" + a);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.ROW(Types.STRING(), Types.STRING());
    }
}

ScalarFunction func = new MyMapFunction();
tableEnv.registerFunction("func", func);

Table table = input
  .map("func(c)").as("a, b")
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>FlatMap</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Performs a flatMap operation with a table function.</p>
{% highlight java %}
public class MyFlatMapFunction extends TableFunction<Row> {

    public void eval(String str) {
        if (str.contains("#")) {
            String[] array = str.split("#");
            for (int i = 0; i < array.length; ++i) {
                collect(Row.of(array[i], array[i].length()));
            }
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING(), Types.INT());
    }
}

TableFunction func = new MyFlatMapFunction();
tableEnv.registerFunction("func", func);

Table table = input
  .flatMap("func(c)").as("a, b")
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Aggregate</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Performs an aggregate operation with an aggregate function. You have to close the "aggregate" with a select statement and the select statement does not support aggregate functions. The output of aggregate will be flattened if the output type is a composite type.</p>
{% highlight java %}
public class MyMinMaxAcc {
    public int min = 0;
    public int max = 0;
}

public class MyMinMax extends AggregateFunction<Row, MyMinMaxAcc> {

    public void accumulate(MyMinMaxAcc acc, int value) {
        if (value < acc.min) {
            acc.min = value;
        }
        if (value > acc.max) {
            acc.max = value;
        }
    }

    @Override
    public MyMinMaxAcc createAccumulator() {
        return new MyMinMaxAcc();
    }

    public void resetAccumulator(MyMinMaxAcc acc) {
        acc.min = 0;
        acc.max = 0;
    }

    @Override
    public Row getValue(MyMinMaxAcc acc) {
        return Row.of(acc.min, acc.max);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.INT, Types.INT);
    }
}

AggregateFunction myAggFunc = new MyMinMax();
tableEnv.registerFunction("myAggFunc", myAggFunc);
Table table = input
  .groupBy("key")
  .aggregate("myAggFunc(a) as (x, y)")
  .select("key, x, y")
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Group Window Aggregate</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Groups and aggregates a table on a <a href="#group-windows">group window</a> and possibly one or more grouping keys. You have to close the "aggregate" with a select statement. And the select statement does not support "*" or aggregate functions.</p>
{% highlight java %}
AggregateFunction myAggFunc = new MyMinMax();
tableEnv.registerFunction("myAggFunc", myAggFunc);

Table table = input
    .window(Tumble.over("5.minutes").on("rowtime").as("w")) // define window
    .groupBy("key, w") // group by key and window
    .aggregate("myAggFunc(a) as (x, y)")
    .select("key, x, y, w.start, w.end"); // access window properties and aggregate results
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>FlatAggregate</strong><br>
        <span class="label label-primary">Streaming</span><br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to a <b>GroupBy Aggregation</b>. Groups the rows on the grouping keys with the following running table aggregation operator to aggregate rows group-wise. The difference from an AggregateFunction is that TableAggregateFunction may return 0 or more records for a group. You have to close the "flatAggregate" with a select statement. And the select statement does not support aggregate functions.</p>
        <p>Instead of using <code>emitValue</code> to output results, you can also use the <code>emitUpdateWithRetract</code> method. Different from <code>emitValue</code>, <code>emitUpdateWithRetract</code> is used to emit values that have been updated. This method outputs data incrementally in retract mode, i.e., once there is an update, we have to retract old records before sending new updated ones. The <code>emitUpdateWithRetract</code> method will be used in preference to the <code>emitValue</code> method if both methods are defined in the table aggregate function, because the method is treated to be more efficient than <code>emitValue</code> as it can output values incrementally. See <a href="udfs.html#table-aggregation-functions">Table Aggregation Functions</a> for details.</p>
{% highlight java %}
/**
 * Accumulator for Top2.
 */
public class Top2Accum {
    public Integer first;
    public Integer second;
}

/**
 * The top2 user-defined table aggregate function.
 */
public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }


    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
        for (Top2Accum otherAcc : iterable) {
            accumulate(acc, otherAcc.first);
            accumulate(acc, otherAcc.second);
        }
    }

    public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
        // emit the value and rank
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}

tEnv.registerFunction("top2", new Top2());
Table orders = tableEnv.scan("Orders");
Table result = orders
    .groupBy("key")
    .flatAggregate("top2(a) as (v, rank)")
    .select("key, v, rank");
{% endhighlight %}
        <p><b>Note:</b> For streaming queries, the required state to compute the query result might grow infinitely depending on the type of aggregation and the number of distinct grouping keys. Please provide a query configuration with a valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>


    <tr>
      <td>
        <strong>Group Window FlatAggregate</strong><br>
        <span class="label label-primary">Streaming</span><br>
      </td>
      <td>
        <p>Groups and aggregates a table on a <a href="#group-windows">group window</a> and possibly one or more grouping keys. You have to close the "flatAggregate" with a select statement. And the select statement does not support aggregate functions.</p>
{% highlight java %}
tableEnv.registerFunction("top2", new Top2());
Table orders = tableEnv.scan("Orders");
Table result = orders
    .window(Tumble.over("5.minutes").on("rowtime").as("w")) // define window
    .groupBy("a, w") // group by key and window
    .flatAggregate("top2(b) as (v, rank)")
    .select("a, w.start, w.end, w.rowtime, v, rank"); // access window properties and aggregate results
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Map</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Performs a map operation with a user-defined scalar function or built-in scalar function. The output will be flattened if the output type is a composite type.</p>
{% highlight scala %}
class MyMapFunction extends ScalarFunction {
  def eval(a: String): Row = {
    Row.of(a, "pre-" + a)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.STRING, Types.STRING)
}

val func = new MyMapFunction()
val table = input
  .map(func('c)).as('a, 'b)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>FlatMap</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Performs a flatMap operation with a table function.</p>
{% highlight scala %}
class MyFlatMapFunction extends TableFunction[Row] {
  def eval(str: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(2)
        row.setField(0, s)
        row.setField(1, s.length)
        collect(row)
      })
    }
  }

  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.INT)
  }
}

val func = new MyFlatMapFunction
val table = input
  .flatMap(func('c)).as('a, 'b)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Aggregate</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Performs an aggregate operation with an aggregate function. You have to close the "aggregate" with a select statement and the select statement does not support aggregate functions. The output of aggregate will be flattened if the output type is a composite type.</p>
{% highlight scala %}
case class MyMinMaxAcc(var min: Int, var max: Int)

class MyMinMax extends AggregateFunction[Row, MyMinMaxAcc] {

  def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
    if (value < acc.min) {
      acc.min = value
    }
    if (value > acc.max) {
      acc.max = value
    }
  }

  override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(0, 0)

  def resetAccumulator(acc: MyMinMaxAcc): Unit = {
    acc.min = 0
    acc.max = 0
  }

  override def getValue(acc: MyMinMaxAcc): Row = {
    Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Types.INT, Types.INT)
  }
}

val myAggFunc = new MyMinMax
val table = input
  .groupBy('key)
  .aggregate(myAggFunc('a) as ('x, 'y))
  .select('key, 'x, 'y)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Group Window Aggregate</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Groups and aggregates a table on a <a href="#group-windows">group window</a> and possibly one or more grouping keys. You have to close the "aggregate" with a select statement. And the select statement does not support "*" or aggregate functions.</p>
{% highlight scala %}
val myAggFunc = new MyMinMax
val table = input
    .window(Tumble over 5.minutes on 'rowtime as 'w) // define window
    .groupBy('key, 'w) // group by key and window
    .aggregate(myAggFunc('a) as ('x, 'y))
    .select('key, 'x, 'y, 'w.start, 'w.end) // access window properties and aggregate results

{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>FlatAggregate</strong><br>
        <span class="label label-primary">Streaming</span><br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Similar to a <b>GroupBy Aggregation</b>. Groups the rows on the grouping keys with the following running table aggregation operator to aggregate rows group-wise. The difference from an AggregateFunction is that TableAggregateFunction may return 0 or more records for a group. You have to close the "flatAggregate" with a select statement. And the select statement does not support aggregate functions.</p>
        <p>Instead of using <code>emitValue</code> to output results, you can also use the <code>emitUpdateWithRetract</code> method. Different from <code>emitValue</code>, <code>emitUpdateWithRetract</code> is used to emit values that have been updated. This method outputs data incrementally in retract mode, i.e., once there is an update, we have to retract old records before sending new updated ones. The <code>emitUpdateWithRetract</code> method will be used in preference to the <code>emitValue</code> method if both methods are defined in the table aggregate function, because the method is treated to be more efficient than <code>emitValue</code> as it can output values incrementally. See <a href="udfs.html#table-aggregation-functions">Table Aggregation Functions</a> for details.</p>
{% highlight scala %}
import java.lang.{Integer => JInteger}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction

/**
 * Accumulator for top2.
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
}

/**
 * The top2 user-defined table aggregate function.
 */
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top2Accum, its: JIterable[Top2Accum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val top2 = iter.next()
      accumulate(acc, top2.first)
      accumulate(acc, top2.second)
    }
  }

  def emitValue(acc: Top2Accum, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
    // emit the value and rank
    if (acc.first != Int.MinValue) {
      out.collect(JTuple2.of(acc.first, 1))
    }
    if (acc.second != Int.MinValue) {
      out.collect(JTuple2.of(acc.second, 2))
    }
  }
}

val top2 = new Top2
val orders: Table = tableEnv.scan("Orders")
val result = orders
    .groupBy('key)
    .flatAggregate(top2('a) as ('v, 'rank))
    .select('key, 'v, 'rank)
{% endhighlight %}
        <p><b>Note:</b> For streaming queries, the required state to compute the query result might grow infinitely depending on the type of aggregation and the number of distinct grouping keys. Please provide a query configuration with a valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
        <strong>Group Window FlatAggregate</strong><br>
        <span class="label label-primary">Streaming</span><br>
      </td>
      <td>
        <p>Groups and aggregates a table on a <a href="#group-windows">group window</a> and possibly one or more grouping keys. You have to close the "flatAggregate" with a select statement. And the select statement does not support aggregate functions.</p>
{% highlight scala %}
val top2 = new Top2
val orders: Table = tableEnv.scan("Orders")
val result = orders
    .window(Tumble over 5.minutes on 'rowtime as 'w) // define window
    .groupBy('a, 'w) // group by key and window
    .flatAggregate(top2('b) as ('v, 'rank))
    .select('a, w.start, 'w.end, 'w.rowtime, 'v, 'rank) // access window properties and aggregate results

{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

Data Types
----------

Please see the dedicated page about [data types](types.html).

Generic types and (nested) composite types (e.g., POJOs, tuples, rows, Scala case classes) can be fields of a row as well.

Fields of composite types with arbitrary nesting can be accessed with [value access functions](functions.html#value-access-functions).

Generic types are treated as a black box and can be passed on or processed by [user-defined functions](udfs.html).

{% top %}

Expression Syntax
-----------------

Some of the operators in previous sections expect one or more expressions. Expressions can be specified using an embedded Scala DSL or as Strings. Please refer to the examples above to learn how expressions can be specified.

This is the EBNF grammar for expressions:

{% highlight ebnf %}

expressionList = expression , { "," , expression } ;

expression = overConstant | alias ;

alias = logic | ( logic , "as" , fieldReference ) | ( logic , "as" , "(" , fieldReference , { "," , fieldReference } , ")" ) ;

logic = comparison , [ ( "&&" | "||" ) , comparison ] ;

comparison = term , [ ( "=" | "==" | "===" | "!=" | "!==" | ">" | ">=" | "<" | "<=" ) , term ] ;

term = product , [ ( "+" | "-" ) , product ] ;

product = unary , [ ( "*" | "/" | "%") , unary ] ;

unary = [ "!" | "-" | "+" ] , composite ;

composite = over | suffixed | nullLiteral | prefixed | atom ;

suffixed = interval | suffixAs | suffixCast | suffixIf | suffixDistinct | suffixFunctionCall | timeIndicator ;

prefixed = prefixAs | prefixCast | prefixIf | prefixDistinct | prefixFunctionCall ;

interval = timeInterval | rowInterval ;

timeInterval = composite , "." , ("year" | "years" | "quarter" | "quarters" | "month" | "months" | "week" | "weeks" | "day" | "days" | "hour" | "hours" | "minute" | "minutes" | "second" | "seconds" | "milli" | "millis") ;

rowInterval = composite , "." , "rows" ;

suffixCast = composite , ".cast(" , dataType , ")" ;

prefixCast = "cast(" , expression , dataType , ")" ;

dataType = "BYTE" | "SHORT" | "INT" | "LONG" | "FLOAT" | "DOUBLE" | "BOOLEAN" | "STRING" | "DECIMAL" | "SQL_DATE" | "SQL_TIME" | "SQL_TIMESTAMP" | "INTERVAL_MONTHS" | "INTERVAL_MILLIS" | ( "MAP" , "(" , dataType , "," , dataType , ")" ) | ( "PRIMITIVE_ARRAY" , "(" , dataType , ")" ) | ( "OBJECT_ARRAY" , "(" , dataType , ")" ) ;

suffixAs = composite , ".as(" , fieldReference , ")" ;

prefixAs = "as(" , expression, fieldReference , ")" ;

suffixIf = composite , ".?(" , expression , "," , expression , ")" ;

prefixIf = "?(" , expression , "," , expression , "," , expression , ")" ;

suffixDistinct = composite , "distinct.()" ;

prefixDistinct = functionIdentifier , ".distinct" , [ "(" , [ expression , { "," , expression } ] , ")" ] ;

suffixFunctionCall = composite , "." , functionIdentifier , [ "(" , [ expression , { "," , expression } ] , ")" ] ;

prefixFunctionCall = functionIdentifier , [ "(" , [ expression , { "," , expression } ] , ")" ] ;

atom = ( "(" , expression , ")" ) | literal | fieldReference ;

fieldReference = "*" | identifier ;

nullLiteral = "nullOf(" , dataType , ")" ;

timeIntervalUnit = "YEAR" | "YEAR_TO_MONTH" | "MONTH" | "QUARTER" | "WEEK" | "DAY" | "DAY_TO_HOUR" | "DAY_TO_MINUTE" | "DAY_TO_SECOND" | "HOUR" | "HOUR_TO_MINUTE" | "HOUR_TO_SECOND" | "MINUTE" | "MINUTE_TO_SECOND" | "SECOND" ;

timePointUnit = "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "QUARTER" | "WEEK" | "MILLISECOND" | "MICROSECOND" ;

over = composite , "over" , fieldReference ;

overConstant = "current_row" | "current_range" | "unbounded_row" | "unbounded_row" ;

timeIndicator = fieldReference , "." , ( "proctime" | "rowtime" ) ;

{% endhighlight %}

**Literals:** Here, `literal` is a valid Java literal. String literals can be specified using single or double quotes. Duplicate the quote for escaping (e.g. `'It''s me.'` or `"I ""like"" dogs."`).

**Null literals:** Null literals must have a type attached. Use `nullOf(type)` (e.g. `nullOf(INT)`) for creating a null value.

**Field references:** The `fieldReference` specifies a column in the data (or all columns if `*` is used), and `functionIdentifier` specifies a supported scalar function. The column names and function names follow Java identifier syntax.

**Function calls:** Expressions specified as strings can also use prefix notation instead of suffix notation to call operators and functions.

**Decimals:** If working with exact numeric values or large decimals is required, the Table API also supports Java's BigDecimal type. In the Scala Table API decimals can be defined by `BigDecimal("123456")` and in Java by appending a "p" for precise e.g. `123456p`.

**Time representation:** In order to work with temporal values the Table API supports Java SQL's Date, Time, and Timestamp types. In the Scala Table API literals can be defined by using `java.sql.Date.valueOf("2016-06-27")`, `java.sql.Time.valueOf("10:10:42")`, or `java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")`. The Java and Scala Table API also support calling `"2016-06-27".toDate()`, `"10:10:42".toTime()`, and `"2016-06-27 10:10:42.123".toTimestamp()` for converting Strings into temporal types. *Note:* Since Java's temporal SQL types are time zone dependent, please make sure that the Flink Client and all TaskManagers use the same time zone.

**Temporal intervals:** Temporal intervals can be represented as number of months (`Types.INTERVAL_MONTHS`) or number of milliseconds (`Types.INTERVAL_MILLIS`). Intervals of same type can be added or subtracted (e.g. `1.hour + 10.minutes`). Intervals of milliseconds can be added to time points (e.g. `"2016-08-10".toDate + 5.days`).

**Scala expressions:** Scala expressions use implicit conversions. Therefore, make sure to add the wildcard import `org.apache.flink.table.api.scala._` to your programs. In case a literal is not treated as an expression, use `.toExpr` such as `3.toExpr` to force a literal to be converted.

{% top %}
