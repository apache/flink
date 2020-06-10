---
title: "DESCRIBE 语句"
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

DESCRIBE 语句用来描述一张表或者视图的 Schema。


## 执行 DESCRIBE 语句

DESCRIBE 语句可以通过 `TableEnvironment` 的 `executeSql()` 执行，也可以在 [SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html) 中执行 DROP 语句。 若 DESCRIBE 操作执行成功，executeSql() 方法返回该表的 Schema，否则会抛出异常。

以下的例子展示了如何在 TableEnvironment 和 SQL CLI 中执行一个 DESCRIBE 语句。

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

上述例子执行的结果为:
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

## 语法

{% highlight sql %}
DESCRIBE [catalog_name.][db_name.]table_name
{% endhighlight %}
