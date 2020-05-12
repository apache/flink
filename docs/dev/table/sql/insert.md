---
title: "INSERT Statement"
nav-parent_id: sql
nav-pos: 5
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

INSERT statements are used to add rows to a table.

## Run an INSERT statement

INSERT statements are specified with the `sqlUpdate()` method of the `TableEnvironment` or executed in [SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html). The method `sqlUpdate()` for INSERT statements is a lazy execution, they will be executed only when `TableEnvironment.execute(jobName)` is invoked.

The following examples show how to run an INSERT statement in `TableEnvironment` and in SQL CLI.

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
tEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
tEnv.sqlUpdate("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

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
table_env.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
table_env.sqlUpdate("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

# run a SQL update query on the registered source table and emit the result to registered sink table
table_env \
    .sqlUpdate("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...);

Flink SQL> SHOW TABLES;
Orders
RubberOrders

Flink SQL> INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
[INFO] Submitting SQL update statement to the cluster...
[INFO] Table update statement has been successfully submitted to the cluster:
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

-- Appends rows into the static partition (date='2019-8-30', country='China')
INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- Appends rows into partition (date, country), where date is static partition with value '2019-8-30',
-- country is dynamic partition whose value is dynamic determined by each row.
INSERT INTO country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;

-- Overwrites rows into static partition (date='2019-8-30', country='China')
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- Overwrites rows into partition (date, country), where date is static partition with value '2019-8-30',
-- country is dynamic partition whose value is dynamic determined by each row.
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;
{% endhighlight %}


## Insert values into tables

The INSERT...VALUES statement can be used to insert data into tables directly from SQL.

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