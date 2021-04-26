---
title: "Connectors"
nav-parent_id: python_tableapi
nav-pos: 130
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


This page describes how to use connectors in PyFlink and highlights the details to be aware of when using Flink connectors in Python programs.

* This will be replaced by the TOC
{:toc}

<span class="label label-info">Note</span> For general connector information and common configuration, please refer to the corresponding [Java/Scala documentation]({{ site.baseurl }}/dev/table/connectors/index.html). 

## Download connector and format jars

Since Flink is a Java/Scala-based project, for both connectors and formats, implementations are available as jars that need to be specified as job [dependencies]({{ site.baseurl }}/dev/python/table-api-users-guide/dependency_management.html).

{% highlight python %}

table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/json.jar")

{% endhighlight %}

## How to use connectors

In PyFink's Table API, DDL is the recommended way to define sources and sinks, executed via the `execute_sql()` method on the `TableEnvironment`. This makes the table available for use by the application.

{% highlight python %}

source_ddl = """
        CREATE TABLE source_table(
            a VARCHAR,
            b INT
        ) WITH (
          'type' = 'kafka',
          'topic' = 'source_topic',
          'properties.bootstrap.servers' = 'kafka:9092',
          'properties.group.id' = 'test_3',
          'scan.startup.mode' = 'latest-offset',
          'format' = 'json'
        )
        """

sink_ddl = """
        CREATE TABLE sink_table(
            a VARCHAR
        ) WITH (
          'type' = 'kafka',
          'topic' = 'sink_topic',
          'properties.bootstrap.servers' = 'kafka:9092',
          'format' = 'json'
        )
        """

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

t_env.sql_query("SELECT a FROM source_table") \
    .insert_into("sink_table")
{% endhighlight %}

Below is a complete example of how to use a Kafka source/sink and the JSON format in PyFlink.

{% highlight python %}

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    # specify connector and format jars
    t_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/json.jar")
    
    source_ddl = """
            CREATE TABLE source_table(
                a VARCHAR,
                b INT
            ) WITH (
              'type' = 'kafka',
              'topic' = 'source_topic',
              'properties.bootstrap.servers' = 'kafka:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    sink_ddl = """
            CREATE TABLE sink_table(
                a VARCHAR
            ) WITH (
              'type' = 'kafka',
              'topic' = 'sink_topic',
              'properties.bootstrap.servers' = 'kafka:9092',
              'format' = 'json'
            )
            """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    t_env.sql_query("SELECT a FROM source_table") \
        .insert_into("sink_table")

    t_env.execute("payment_demo")


if __name__ == '__main__':
    log_processing()
{% endhighlight %}


## Predefined Sources and Sinks

Some data sources and sinks are built into Flink and are available out-of-the-box. These predefined data sources include reading from Pandas DataFrame, or ingesting data from collections. The predefined data sinks support writing to Pandas DataFrame.

### from/to Pandas

PyFlink Tables support conversion to and from Pandas DataFrame.

{% highlight python %}

import pandas as pd
import numpy as np

# Create a PyFlink Table
pdf = pd.DataFrame(np.random.rand(1000, 2))
table = t_env.from_pandas(pdf, ["a", "b"]).filter("a > 0.5")

# Convert the PyFlink Table to a Pandas DataFrame
pdf = table.to_pandas()
{% endhighlight %}

### from_elements()

`from_elements()` is used to create a table from a collection of elements. The element types must be acceptable atomic types or acceptable composite types.

{% highlight python %}

table_env.from_elements([(1, 'Hi'), (2, 'Hello')])

# use the second parameter to specify custom field names
table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['a', 'b'])

# use the second parameter to specify a custom table schema
table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
                        DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                       DataTypes.FIELD("b", DataTypes.STRING())]))
{% endhighlight %}

The above query returns a Table like:

{% highlight python %}
+----+-------+
| a  |   b   |
+====+=======+
| 1  |  Hi   |
+----+-------+
| 2  | Hello |
+----+-------+
{% endhighlight %}

## User-defined sources & sinks

In some cases, you may want to define custom sources and sinks. Currently, sources and sinks must be implemented in Java/Scala, but you can define a `TableFactory` to support their use via DDL. More details can be found in the [Java/Scala documentation]({{ site.baseurl }}/dev/table/sourceSinks.html).

