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


* This will be replaced by the TOC
{:toc}

This page describes how to use connectors in PyFlink Tale API. The main purpose of this page is to highlight the different parts between using connectors in PyFlink and Java/Scala. Below, we will guide you how to use connectors through an explicit example in which Kafka and Json format are used.

<span class="label label-info">Note</span> For the common parts of using connectors between PyFlink and Java/Scala, you can refer to the [Java/Scala document]({{ site.baseurl }}/zh/dev/table/connectors/index.html) for more details. 

## Download connector and format jars

Suppose you are using Kafka connector and Json format, you need first download the [Kafka]({{ site.baseurl }}/zh/dev/table/connectors/kafka.html) and [Json](https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/) jars. Once the connector and format jars are downloaded to local, specify them with the [Dependency Management]({{ site.baseurl }}/zh/dev/python/user-guide/table/dependency_management.html) APIs.

{% highlight python %}

table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/json.jar")

{% endhighlight %}

## How to use connectors

In the Table API of PyFlink, DDL is recommended to define source and sink. You can use the `execute_sql()` method on `TableEnvironment` to register source and sink with DDL. After that, you can select from the source table and insert into the sink table.

{% highlight python %}

source_ddl = """
        CREATE TABLE source_table(
            a VARCHAR,
            b INT
        ) WITH (
          'connector.type' = 'kafka',
          'connector.version' = 'universal',
          'connector.topic' = 'source_topic',
          'connector.properties.bootstrap.servers' = 'kafka:9092',
          'connector.properties.group.id' = 'test_3',
          'connector.startup-mode' = 'latest-offset',
          'format.type' = 'json'
        )
        """

sink_ddl = """
        CREATE TABLE sink_table(
            a VARCHAR
        ) WITH (
          'connector.type' = 'kafka',
          'connector.version' = 'universal',
          'connector.topic' = 'sink_topic',
          'connector.properties.bootstrap.servers' = 'kafka:9092',
          'format.type' = 'json'
        )
        """

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

t_env.sql_query("select a from source_table") \
    .insert_into("sink_table")
    
{% endhighlight %}

Below is a complete example of how to use the Kafka and Json format in PyFlink.

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
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'source_topic',
              'connector.properties.bootstrap.servers' = 'kafka:9092',
              'connector.properties.group.id' = 'test_3',
              'connector.startup-mode' = 'latest-offset',
              'format.type' = 'json'
            )
            """

    sink_ddl = """
            CREATE TABLE sink_table(
                a VARCHAR
            ) WITH (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'sink_topic',
              'connector.properties.bootstrap.servers' = 'kafka:9092',
              'format.type' = 'json'
            )
            """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    t_env.sql_query("select a from source_table") \
        .insert_into("sink_table")

    t_env.execute("payment_demo")


if __name__ == '__main__':
    log_processing()
{% endhighlight %}


## Predefined Sources and Sinks

A few basic data sources and sinks are built into Flink and are always available. The predefined data sources include reading from Pandas DataFrame, or ingesting data from collections. The predefined data sinks support writing to Pandas DataFrame.

### from/to Pandas

It supports to convert between PyFlink Table and Pandas DataFrame.

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

`from_elements()` is used to creates a table from a collection of elements. The elements types must be acceptable atomic types or acceptable composite types. 

{% highlight python %}

table_env.from_elements([(1, 'Hi'), (2, 'Hello')])

# use the second parameter to specify custom field names
table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['a', 'b'])

# use the second parameter to specify custom table schema
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

In some cases, you may want to defined your own sources and sinks. Currently, Python sources and sinks are not supported. However, you can write Java/Scala TableFactory and use your own sources and sinks in DDL. More details can be found in the [Java/Scala document]({{ site.baseurl }}/zh/dev/table/sourceSinks.html).

