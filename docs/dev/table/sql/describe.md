---
title: "DESCRIBE Statements"
nav-parent_id: sql
nav-pos: 7
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

DESCRIBE statements are used to describe the schema of a table or a view.


## Run a DESCRIBE statement

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

DESCRIBE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns the schema of given table for a successful DESCRIBE operation, otherwise will throw an exception.

The following examples show how to run a DESCRIBE statement in `TableEnvironment`.

</div>

<div data-lang="python" markdown="1">

DESCRIBE statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns the schema of given table for a successful DESCRIBE operation, otherwise will throw an exception.

The following examples show how to run a DESCRIBE statement in `TableEnvironment`.

</div>

<div data-lang="SQL CLI" markdown="1">

DESCRIBE statements can be executed in [SQL CLI]({% link dev/table/sqlClient.md %}).

The following examples show how to run a DESCRIBE statement in SQL CLI.

</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// register a table named "Orders"
tableEnv.executeSql(
        "CREATE TABLE Orders (" +
        " `user` BIGINT NOT NULl," +
        " product VARCHAR(32)," +
        " amount INT," +
        " ts TIMESTAMP(3)," +
        " ptime AS PROCTIME()," +
        " PRIMARY KEY(`user`) NOT ENFORCED," +
        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS" +
        ") with (...)");

// print the schema
tableEnv.executeSql("DESCRIBE Orders").print();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// register a table named "Orders"
 tableEnv.executeSql(
        "CREATE TABLE Orders (" +
        " `user` BIGINT NOT NULl," +
        " product VARCHAR(32)," +
        " amount INT," +
        " ts TIMESTAMP(3)," +
        " ptime AS PROCTIME()," +
        " PRIMARY KEY(`user`) NOT ENFORCED," +
        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS" +
        ") with (...)")

// print the schema
tableEnv.executeSql("DESCRIBE Orders").print()

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

# register a table named "Orders"
table_env.execute_sql( \
        "CREATE TABLE Orders (" 
        " `user` BIGINT NOT NULl," 
        " product VARCHAR(32),"
        " amount INT,"
        " ts TIMESTAMP(3),"
        " ptime AS PROCTIME(),"
        " PRIMARY KEY(`user`) NOT ENFORCED,"
        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS"
        ") with (...)");

# print the schema
table_env.execute_sql("DESCRIBE Orders").print()

{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE Orders (
>  `user` BIGINT NOT NULl,
>  product VARCHAR(32),
>  amount INT,
>  ts TIMESTAMP(3),
>  ptime AS PROCTIME(),
>  PRIMARY KEY(`user`) NOT ENFORCED,
>  WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS
> ) with (
>  ...
> );
[INFO] Table has been created.

Flink SQL> DESCRIBE Orders;

{% endhighlight %}
</div>
</div>

The result of the above example is:
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight text %}

+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
|    name |                             type |  null |       key | computed column |                  watermark |
+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
|    user |                           BIGINT | false | PRI(user) |                 |                            |
| product |                      VARCHAR(32) |  true |           |                 |                            |
|  amount |                              INT |  true |           |                 |                            |
|      ts |           TIMESTAMP(3) *ROWTIME* |  true |           |                 | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP(3) NOT NULL *PROCTIME* | false |           |      PROCTIME() |                            |
+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
5 rows in set

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight text %}

+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
|    name |                             type |  null |       key | computed column |                  watermark |
+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
|    user |                           BIGINT | false | PRI(user) |                 |                            |
| product |                      VARCHAR(32) |  true |           |                 |                            |
|  amount |                              INT |  true |           |                 |                            |
|      ts |           TIMESTAMP(3) *ROWTIME* |  true |           |                 | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP(3) NOT NULL *PROCTIME* | false |           |      PROCTIME() |                            |
+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
5 rows in set

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight text %}

+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
|    name |                             type |  null |       key | computed column |                  watermark |
+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
|    user |                           BIGINT | false | PRI(user) |                 |                            |
| product |                      VARCHAR(32) |  true |           |                 |                            |
|  amount |                              INT |  true |           |                 |                            |
|      ts |           TIMESTAMP(3) *ROWTIME* |  true |           |                 | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP(3) NOT NULL *PROCTIME* | false |           |      PROCTIME() |                            |
+---------+----------------------------------+-------+-----------+-----------------+----------------------------+
5 rows in set

{% endhighlight %}
</div>
<div data-lang="SQL CLI" markdown="1">
{% highlight text %}

root
 |-- user: BIGINT NOT NULL
 |-- product: VARCHAR(32)
 |-- amount: INT
 |-- ts: TIMESTAMP(3) *ROWTIME*
 |-- ptime: TIMESTAMP(3) NOT NULL *PROCTIME* AS PROCTIME()
 |-- WATERMARK FOR ts AS `ts` - INTERVAL '1' SECOND
 |-- CONSTRAINT PK_3599338 PRIMARY KEY (user)

{% endhighlight %}
</div>

</div>


{% top %}

## Syntax

{% highlight sql %}
DESCRIBE [catalog_name.][db_name.]table_name
{% endhighlight %}
