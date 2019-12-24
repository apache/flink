---
title: "Data Manipulation Language (DML)"
nav-parent_id: sql
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

* This will be replaced by the TOC
{:toc}

DMLs are specified with the `sqlUpdate()` method of the `TableEnvironment` (the same to DDLs). The method `sqlUpdate()` for DMLs is a lazy execution, the DMLs will be executed only when `TableEnvironment.execute(jobName)` is invoked.

## Specifying a DML

The following examples show how to specify a SQL DDL.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tEnv = TableEnvironment.create(settings);

// register a source table named "Orders" and a sink table named "RubberOrders"
tEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
tEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)");

// run a SQL update query on the registered source table and emit the result to registered sink table
tEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tEnv = TableEnvironment.create(settings)

// register a source table named "Orders" and a sink table named "RubberOrders"
tEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)")
tEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)")

// run a SQL update query on the registered source table and emit the result to registered sink table
tEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.newInstance()...
table_env = TableEnvironment.create(settings)

# register a source table named "Orders" and a sink table named "RubberOrders"
table_env.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)")
table_env.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)")

# run a SQL update query on the registered source table and emit the result to registered sink table
table_env \
    .sqlUpdate("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

{% top %}

## Supported Syntax

{% highlight sql %}

insert:
  INSERT INTO tableReference
  query

{% endhighlight %}


## Operations

### Insert

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
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
        <p>Output tables must be registered in the TableEnvironment (see <a href="common.html#register-a-tablesink">Register a TableSink</a>). Moreover, the schema of the registered table must match the schema of the query.</p>

{% highlight sql %}
INSERT INTO OutputTable
SELECT users, tag
FROM Orders
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}