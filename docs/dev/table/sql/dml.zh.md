---
title: "Data Manipulation Language (DML)"
nav-title: "Data Manipulation Language"
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

## Run a DML

The following examples show how to specify a SQL DML.

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

## Insert from select queries

Query Results can be inserted into tables by using the insert clause.

### Syntax

{% highlight sql %}

INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec] select_statement

part_spec:
  (part_col_name1=val1 [, part_col_name2=val2, ...])

{% endhighlight %}

**OVERWRITE**

`INSERT OVERWRITE` will overwrite any existing data in the table or partition. Otherwise, new data is appended.

**PARTITION**

`PARTITION` clause should contain static partition columns of this inserting.

### Examples

{% highlight sql %}

-- Creates a partitioned table
CREATE TABLE country_page_view (user STRING, cnt INT, date STRING, country STRING)
PARTITIONED BY (date, country)
WITH (...)

-- Appends rows into the partition (date='2019-8-30', country='China')
INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- Overwrites the partition (date='2019-8-30', country='China') using rows in page_view_source
INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

{% endhighlight %}


## Insert values into tables

### Syntax

{% highlight sql %}
INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name VALUES values_row [, values_row ...]

values_row:
    : (val1 [, val2, ...])
{% endhighlight %}

**OVERWRITE**

`INSERT OVERWRITE` will overwrite any existing data in the table. Otherwise, new data is appended.

### Examples

{% highlight sql %}

CREATE TABLE students (name STRING, age INT, gpa DECIMAL(3, 2)) WITH (...);

INSERT INTO students
  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);

{% endhighlight %}

{% top %}