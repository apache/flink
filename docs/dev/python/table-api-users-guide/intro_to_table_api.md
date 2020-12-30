---
title: "Intro to the Python Table API"
nav-parent_id: python_tableapi
nav-pos: 25
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

This document is a short introduction to the PyFlink Table API, which is used to help novice users quickly understand the basic usage of PyFlink Table API.
For advanced usage, please refer to other documents in this User Guide.

* This will be replaced by the TOC
{:toc}

Common Structure of Python Table API Program 
--------------------------------------------

All Table API and SQL programs, both batch and streaming, follow the same pattern. The following code example shows the common structure of Table API and SQL programs.

{% highlight python %}

from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# 1. create a TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# 2. create source Table
table_env.execute_sql("""
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '10'
    )
""")

# 3. create sink Table
table_env.execute_sql("""
    CREATE TABLE print (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# 4. query from source table and perform caculations
# create a Table from a Table API query:
source_table = table_env.from_path("datagen")
# or create a Table from a SQL query:
source_table = table_env.sql_query("SELECT * FROM datagen")

result_table = source_table.select(source_table.id + 1, source_table.data)

# 5. emit query result to sink table
# emit a Table API result Table to a sink table:
result_table.execute_insert("print").wait()
# or emit results via SQL query:
table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").wait()

{% endhighlight %}

{% top %}

Create a TableEnvironment
---------------------------

The `TableEnvironment` is a central concept of the Table API and SQL integration. The following code example shows how to create a TableEnvironment:

{% highlight python %}

from pyflink.table import EnvironmentSettings, StreamTableEnvironment, BatchTableEnvironment

# create a blink streaming TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# create a blink batch TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

# create a flink streaming TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_old_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# create a flink batch TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_old_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

{% endhighlight %}

For more details about the different ways to create a `TableEnvironment`, please refer to the [TableEnvironment Documentation]({% link dev/python/table-api-users-guide/table_environment.md %}#create-a-tableenvironment).

The `TableEnvironment` is responsible for:

* Creating `Table`s
* Registering `Table`s as a temporary view
* Executing SQL queries, see [SQL]({% link dev/table/sql/index.md %}) for more details
* Registering user-defined (scalar, table, or aggregation) functions, see [General User-defined Functions]({% link dev/python/table-api-users-guide/udfs/python_udfs.md %}) and [Vectorized User-defined Functions]({% link dev/python/table-api-users-guide/udfs/vectorized_python_udfs.md %}) for more details
* Configuring the job, see [Python Configuration]({% link dev/python/python_config.md %}) for more details
* Managing Python dependencies, see [Dependency Management]({% link dev/python/table-api-users-guide/dependency_management.md %}) for more details
* Submitting the jobs for execution

Currently there are 2 planners available: flink planner and blink planner.

You should explicitly set which planner to use in the current program.
We recommend using the blink planner as much as possible. 

{% top %}

Create Tables
---------------

`Table` is a core component of the Python Table API. A `Table` is a logical representation of the intermediate result of a Table API Job.

A `Table` is always bound to a specific `TableEnvironment`. It is not possible to combine tables from different TableEnvironments in same query, e.g., to join or union them.

### Create using a List Object

You can create a Table from a list object:

{% highlight python %}

# create a blink batch TableEnvironment
from pyflink.table import EnvironmentSettings, BatchTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')])
table.to_pandas()

{% endhighlight %}

The result is:

{% highlight text %}
   _1     _2
0   1     Hi
1   2  Hello
{% endhighlight %}

You can also create the Table with specified column names:

{% highlight python %}

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table.to_pandas()

{% endhighlight %}

The result is:

{% highlight text %}
   id   data
0   1     Hi
1   2  Hello
{% endhighlight %}

By default the table schema is extracted from the data automatically. 

If the automatically generated table schema isn't satisfactory, you can specify it manually:

{% highlight python %}

table_without_schema = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
# by default the type of the "id" column is 64 bit int
default_type = table_without_schema.to_pandas()["id"].dtype
print('By default the type of the "id" column is %s.' % default_type)

from pyflink.table import DataTypes
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
                                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()),
                                               DataTypes.FIELD("data", DataTypes.STRING())]))
# now the type of the "id" column is 8 bit int
type = table.to_pandas()["id"].dtype
print('Now the type of the "id" column is %s.' % type)

{% endhighlight %}

The result is:

{% highlight text %}
By default the type of the "id" column is int64.
Now the type of the "id" column is int8.
{% endhighlight %}

### Create using a Connector

You can create a Table using connector DDL:

{% highlight python %}
# create a blink stream TableEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

table_env.execute_sql("""
    CREATE TABLE random_source (
        id BIGINT, 
        data TINYINT 
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='3',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='6'
    )
""")
table = table_env.from_path("random_source")
table.to_pandas()

{% endhighlight %}

The result is:

{% highlight text %}
   id  data
0   2     5
1   1     4
2   3     6
{% endhighlight %}

### Create using a Catalog

A `TableEnvironment` maintains a map of catalogs of tables which are created with an identifier.

The tables in a catalog may either be temporary, and tied to the lifecycle of a single Flink session, or permanent, and visible across multiple Flink sessions.

The tables and views created via SQL DDL, e.g. "create table ..." and "create view ..." are also stored in a catalog.

You can directly access the tables in a catalog via SQL.

If you want to use tables from a catalog with the Table API, you can use the "from_path" method to create the Table API objects:

{% highlight python %}

# prepare the catalog
# register Table API tables in the catalog
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view('source_table', table)

# create Table API table from catalog
new_table = table_env.from_path('source_table')
new_table.to_pandas()

{% endhighlight %}

The result is:

{% highlight text %}
   id   data
0   1     Hi
1   2  Hello
{% endhighlight %}

{% top %}

Write Queries
---------------

### Write Table API Queries

The `Table` object offers many methods for applying relational operations. 
These methods return new `Table` objects representing the result of applying the relational operations on the input `Table`. 
These relational operations may be composed of multiple method calls, such as `table.group_by(...).select(...)`.

The [Table API]({% link dev/table/tableApi.md %}?code_tab=python) documentation describes all Table API operations that are supported on streaming and batch tables.

The following example shows a simple Table API aggregation query:

{% highlight python %}

# using batch table environment to execute the queries
from pyflink.table import EnvironmentSettings, BatchTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

orders = table_env.from_elements([('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
                                 ['name', 'country', 'revenue'])

# compute revenue for all customers from France
revenue = orders \
    .select(orders.name, orders.country, orders.revenue) \
    .where(orders.country == 'FRANCE') \
    .group_by(orders.name) \
    .select(orders.name, orders.revenue.sum.alias('rev_sum'))
    
revenue.to_pandas()

{% endhighlight %}

The result is:

{% highlight text %}
   name  rev_sum
0  Jack       30
{% endhighlight %}

### Write SQL Queries

Flink's SQL integration is based on [Apache Calcite](https://calcite.apache.org), which implements the SQL standard. SQL queries are specified as Strings.

The [SQL]({% link dev/table/sql/index.md %}) documentation describes Flink's SQL support for streaming and batch tables.

The following example shows a simple SQL aggregation query:

{% highlight python %}

# use a StreamTableEnvironment to execute the queries
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)


table_env.execute_sql("""
    CREATE TABLE random_source (
        id BIGINT, 
        data TINYINT
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='8',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='11'
    )
""")

table_env.execute_sql("""
    CREATE TABLE print_sink (
        id BIGINT, 
        data_sum TINYINT 
    ) WITH (
        'connector' = 'print'
    )
""")

table_env.execute_sql("""
    INSERT INTO print_sink
        SELECT id, sum(data) as data_sum FROM 
            (SELECT id / 2 as id, data FROM random_source)
        WHERE id > 1
        GROUP BY id
""").wait()

{% endhighlight %}

The result is:

{% highlight text %}
2> +I(4,11)
6> +I(2,8)
8> +I(3,10)
6> -U(2,8)
8> -U(3,10)
6> +U(2,15)
8> +U(3,19)
{% endhighlight %}

In fact, this shows the change logs received by the print sink.
The output format of a change log is:
{% highlight text %}
{subtask id}> {message type}{string format of the value}
{% endhighlight %}
For example, "2> +I(4,11)" means this message comes from the 2nd subtask, and "+I" means it is an insert message. "(4, 11)" is the content of the message.
In addition, "-U" means a retract record (i.e. update-before), which means this message should be deleted or retracted from the sink. 
"+U" means this is an update record (i.e. update-after), which means this message should be updated or inserted by the sink.

So, we get this result from the change logs above:

{% highlight text %}
(4, 11)
(2, 15) 
(3, 19)
{% endhighlight %}

### Mix the Table API and SQL

The `Table` objects used in Table API and the tables used in SQL can be freely converted to each other.

The following example shows how to use a `Table` object in SQL:

{% highlight python %}

# create a sink table to emit results
table_env.execute_sql("""
    CREATE TABLE table_sink (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")

# convert the Table API table to a SQL view
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view('table_api_table', table)

# emit the Table API table
table_env.execute_sql("INSERT INTO table_sink SELECT * FROM table_api_table").wait()

{% endhighlight %}

The result is:

{% highlight text %}
6> +I(1,Hi)
6> +I(2,Hello)
{% endhighlight %}

And the following example shows how to use SQL tables in the Table API:

{% highlight python %}

# create a sql source table
table_env.execute_sql("""
    CREATE TABLE sql_source (
        id BIGINT, 
        data TINYINT 
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='4',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='7'
    )
""")

# convert the sql table to Table API table
table = table_env.from_path("sql_source")

# or create the table from a sql query
table = table_env.sql_query("SELECT * FROM sql_source")

# emit the table
table.to_pandas()

{% endhighlight %}

The result is:

{% highlight text %}
   id  data
0   2     5
1   1     4
2   4     7
3   3     6
{% endhighlight %}

{% top %}

Emit Results
----------------

### Collect Results to Client

You can call the "to_pandas" method to [convert a `Table` object to a pandas DataFrame]({% link dev/python/table-api-users-guide/conversion_of_pandas.md %}#convert-pyflink-table-to-pandas-dataframe):

{% highlight python %}

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table.to_pandas()

{% endhighlight %}

The result is:

{% highlight text %}
   id   data
0   1     Hi
1   2  Hello
{% endhighlight %}

<span class="label label-info">Note</span> "to_pandas" will trigger the materialization of the table and collect table content to the memory of the client, it's good practice to limit the number of rows collected via <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.Table.limit">Table.limit</a>.
<span class="label label-info">Note</span> "to_pandas" is not supported by the flink planner, and not all data types can be emitted to pandas DataFrames.

### Emit Results to One Sink Table

You can call the "execute_insert" method to emit the data in a `Table` object to a sink table:

{% highlight python %}

table_env.execute_sql("""
    CREATE TABLE sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table.execute_insert("sink_table").wait()

{% endhighlight %}

The result is:

{% highlight text %}
6> +I(1,Hi)
6> +I(2,Hello)
{% endhighlight %}

This could also be done using SQL:

{% highlight python %}

table_env.create_temporary_view("table_source", table)
table_env.execute_sql("INSERT INTO sink_table SELECT * FROM table_source").wait()

{% endhighlight %}

### Emit Results to Multiple Sink Tables

You can use a `StatementSet` to emit the `Table`s to multiple sink tables in one job:

{% highlight python %}

# prepare source tables and sink tables
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view("simple_source", table)
table_env.execute_sql("""
    CREATE TABLE first_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")
table_env.execute_sql("""
    CREATE TABLE second_sink_table (
        id BIGINT, 
        data VARCHAR
    ) WITH (
        'connector' = 'print'
    )
""")

# create a statement set
statement_set = table_env.create_statement_set()

# emit the "table" object to the "first_sink_table"
statement_set.add_insert("first_sink_table", table)

# emit the "simple_source" to the "second_sink_table" via a insert sql query
statement_set.add_insert_sql("INSERT INTO second_sink_table SELECT * FROM simple_source")

# execute the statement set
statement_set.execute().wait()

{% endhighlight %}

The result is:

{% highlight text %}
7> +I(1,Hi)
7> +I(1,Hi)
7> +I(2,Hello)
7> +I(2,Hello)
{% endhighlight %}

Explain Tables
-----------------

The Table API provides a mechanism to explain the logical and optimized query plans used to compute a `Table`. 
This is done through the `Table.explain()` or `StatementSet.explain()` methods. `Table.explain()`returns the plan of a `Table`. `StatementSet.explain()` returns the plan for multiple sinks. These methods return a string describing three things:

1. the Abstract Syntax Tree of the relational query, i.e., the unoptimized logical query plan,
2. the optimized logical query plan, and
3. the physical execution plan.

`TableEnvironment.explain_sql()` and `TableEnvironment.execute_sql()` support executing an `EXPLAIN` statement to get the plans. Please refer to the [EXPLAIN]({% link dev/table/sql/explain.md %}) page for more details.

The following code shows how to use the `Table.explain()` method:

{% highlight python %}

# using a StreamTableEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

table1 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table2 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table = table1 \
    .where(table1.data.like('H%')) \
    .union_all(table2)
print(table.explain())

{% endhighlight %}

The result is:

{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
:- LogicalFilter(condition=[LIKE($1, _UTF-16LE'H%')])
:  +- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_201907291, source: [PythonInputFormatTableSource(id, data)]]])
+- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_1709623525, source: [PythonInputFormatTableSource(id, data)]]])

== Optimized Logical Plan ==
Union(all=[true], union=[id, data])
:- Calc(select=[id, data], where=[LIKE(data, _UTF-16LE'H%')])
:  +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_201907291, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])
+- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_1709623525, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])

== Physical Execution Plan ==
Stage 133 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 134 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_201907291, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

                Stage 135 : Operator
                        content : Calc(select=[id, data], where=[(data LIKE _UTF-16LE'H%')])
                        ship_strategy : FORWARD

Stage 136 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 137 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_1709623525, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

{% endhighlight %}

The following code shows how to use the `StatementSet.explain()` method:

{% highlight python %}

# using a StreamTableEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

table1 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table2 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.execute_sql("""
    CREATE TABLE print_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")
table_env.execute_sql("""
    CREATE TABLE black_hole_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'blackhole'
    )
""")

statement_set = table_env.create_statement_set()

statement_set.add_insert("print_sink_table", table1.where(table1.data.like('H%')))
statement_set.add_insert("black_hole_sink_table", table2)

print(statement_set.explain())

{% endhighlight %}

The result is:

{% highlight text %}
== Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.print_sink_table], fields=[id, data])
+- LogicalFilter(condition=[LIKE($1, _UTF-16LE'H%')])
   +- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_541737614, source: [PythonInputFormatTableSource(id, data)]]])

LogicalSink(table=[default_catalog.default_database.black_hole_sink_table], fields=[id, data])
+- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_1437429083, source: [PythonInputFormatTableSource(id, data)]]])

== Optimized Logical Plan ==
Sink(table=[default_catalog.default_database.print_sink_table], fields=[id, data])
+- Calc(select=[id, data], where=[LIKE(data, _UTF-16LE'H%')])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_541737614, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])

Sink(table=[default_catalog.default_database.black_hole_sink_table], fields=[id, data])
+- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_1437429083, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])

== Physical Execution Plan ==
Stage 139 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 140 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_541737614, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

                Stage 141 : Operator
                        content : Calc(select=[id, data], where=[(data LIKE _UTF-16LE'H%')])
                        ship_strategy : FORWARD

Stage 143 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 144 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_1437429083, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

                Stage 142 : Data Sink
                        content : Sink: Sink(table=[default_catalog.default_database.print_sink_table], fields=[id, data])
                        ship_strategy : FORWARD

                        Stage 145 : Data Sink
                                content : Sink: Sink(table=[default_catalog.default_database.black_hole_sink_table], fields=[id, data])
                                ship_strategy : FORWARD
{% endhighlight %}
