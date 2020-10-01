---
title: "连接器"
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


这篇描述了如何在 PyFlink 中使用连接器，并强调了在 Python 程序中使用 Flink 连接器时需要注意的细节。

* This will be replaced by the TOC
{:toc}

<span class="label label-info">Note</span> 想要了解常见的连接器信息和通用配置，请查阅相关的 [Java/Scala 文档]({{ site.baseurl }}/zh/dev/table/connectors/index.html)。

## 下载连接器和格式 jar 包

由于 Flink 是一个基于 Java/Scala-based 的项目，对于连接器和格式的实现可以作为 jar 包使用，将其指定为作业的 [依赖]({{ site.baseurl }}/zh/dev/python/user-guide/table/dependency_management.html)。

{% highlight python %}

table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/json.jar")

{% endhighlight %}

## 如何使用连接器

在 PyFink Table API 中，DDL 是定义 sources 和 sinks 推荐的方式，通过执行在 `TableEnvironment` 中的  `execute_sql()` 方法。这样使得在应用程序可以使用这张表。

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

下面是如何在 PyFlink 中使用 Kafka source/sink 和 JSON 格式的完整例子。

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


## 内置的 Sources 和 Sinks

有些 sources 和 sinks 被内置在 Flink 中，并且可以直接使用。这些内置的 sources 包括可以从 Pandas DataFrame 读数据，或者从 collections 消费数据。内置的 sinks 支持将数据写到 Pandas DataFrame 中。

### 和 Pandas 之间转换

PyFlink 表支持和 Pandas DataFrame 之间转换数据。

{% highlight python %}

import pandas as pd
import numpy as np

# 创建一个 PyFlink 表
pdf = pd.DataFrame(np.random.rand(1000, 2))
table = t_env.from_pandas(pdf, ["a", "b"]).filter("a > 0.5")

# 将 PyFlink 表转换成 Pandas DataFrame
pdf = table.to_pandas()
{% endhighlight %}

### from_elements()

`from_elements()` 用于从一个元素集合中创建一张表。 元素类型必须是可支持的原子类型或者是可支持的复杂类型。

{% highlight python %}

table_env.from_elements([(1, 'Hi'), (2, 'Hello')])

# 使用第二个参数指定自定义字段名
table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['a', 'b'])

# 使用第二个参数指定自定义表模式
table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
                        DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                       DataTypes.FIELD("b", DataTypes.STRING())]))
{% endhighlight %}

以上查询返回的表如下:

{% highlight python %}
+----+-------+
| a  |   b   |
+====+=======+
| 1  |  Hi   |
+----+-------+
| 2  | Hello |
+----+-------+
{% endhighlight %}

## 用户自定义的 sources 和 sinks

在某些情况下，你可能想要自定义 sources 和 sinks。目前，sources 和 sinks 必须使用 Java/Scala 实现，但是你可以通过DDL定义一个 `TableFactory` 来使用它们。更多详情，可查阅 [Java/Scala 文档]({{ site.baseurl }}/zh/dev/table/sourceSinks.html)。

