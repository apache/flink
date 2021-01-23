---
title: "JDBC Connector"
nav-title: JDBC
nav-parent_id: connectors
nav-pos: 9
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

该连接器可以向 JDBC 数据库写入数据。

添加下面的依赖以便使用该连接器（同时添加 JDBC 驱动）：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-jdbc{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

注意该连接器目前还 __不是__ 二进制发行版的一部分，如何在集群中运行请参考 [这里]({% link dev/project-configuration.zh.md %})。

已创建的 JDBC Sink 能够保证至少一次的语义。
更有效的精确执行一次可以通过 upsert 语句或幂等更新实现。

用法示例：
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env
        .fromElements(...)
        .addSink(JdbcSink.sink(
                "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.id);
                    ps.setString(2, t.title);
                    ps.setString(3, t.author);
                    ps.setDouble(4, t.price);
                    ps.setInt(5, t.qty);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(getDbMetadata().getUrl())
                        .withDriverName(getDbMetadata().getDriverClass())
                        .build()));
env.execute();
{% endhighlight %}

更多细节请查看 [API documentation]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/connector/jdbc/JdbcSink.html) 。
