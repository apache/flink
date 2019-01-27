---
title: "Multiple TableSink Optimization"
nav-parent_id: tableapi
nav-pos: 110
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

Multiple TableSinks are needed if we want to emit multiple results to different external storage in a Flink job. It's better if we can define multiple TableSinks without having common operators executed repeatedly.

### How to avoid executing common operators repeatedly if there are multiple TableSinks in a job.
The following example shows a Flink job which has multiple TableSinks.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// enable subsection optimization
TableConfig conf = new TableConfig();
conf.setSubsectionOptimization(true);
// get a BatchTableEnvironment, works for StreamTableEnvironment equivalently
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, conf);

// register Orders table

// compute revenue for all customers from France
Table revenue = tableEnv.sqlQuery(
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  );
// register a Table
tEnv.registerTable("T", revenue);

// define first TableSink
TableSink csvSink1 = new CsvTableSink("/path/to/file1", ...);

// compute customers with high purchasing ability from France
Table result1 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum >= 100000"
    );
// emit result1 to sink1
result1.writeToSink(csvSink1);

// define second TableSink
TableSink csvSink2 = new CsvTableSink("/path/to/file2", ...);

// compute customers with good purchasing ability from France
Table result2 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum < 100000 AND revSum > 20000"
    );
// emit result2 to sink2
result2.writeToSink(csvSink2);

// execute query
tEnv.execute();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// enable subsection optimization
val conf = new TableConfig
conf.setSubsectionOptimization(true)
val tableEnv = TableEnvironment.getTableEnvironment(env, conf)

// register Orders table

// compute revenue for all customers from France
val revenue = tableEnv.sqlQuery(
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  )
// register a Table
tEnv.registerTable("T", revenue)

// define first TableSink
val csvSink1 = new CsvTableSink("/path/to/file1", ...)

// compute customers with high purchasing ability from France
val result1 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum >= 100000"
    )
// emit result1 to sink1
result1.writeToSink(csvSink1)

// define second TableSink
val csvSink2 = new CsvTableSink("/path/to/file2", ...)

// compute customers with good purchasing ability from France
val result2 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum < 100000 AND revSum > 20000"
    )
// emit result2 to sink2
result2.writeToSink(csvSink2)

// execute query
tEnv.execute()
{% endhighlight %}
</div>
</div>

**Note: It's important to enable subsection optimization if there are multiple TableSinks in a job.**
In the above example, operators to compute revenue for all French customers will be reused if enable subsection optimization. The following picture shows difference between the job enable subsection optimization and the one not.

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/multiple_sink.png" width="50%" height="50%" />
</div>

{% top %}


