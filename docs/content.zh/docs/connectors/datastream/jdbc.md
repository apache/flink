---
title: JDBC
weight: 10
type: docs
aliases:
  - /zh/dev/connectors/jdbc.html
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

# JDBC Connector

该连接器可以向 JDBC 数据库写入数据。

添加下面的依赖以便使用该连接器（同时添加 JDBC 驱动）：

{{< artifact flink-connector-jdbc >}}

注意该连接器目前还 __不是__ 二进制发行版的一部分，如何在集群中运行请参考 [这里]({{< ref "docs/dev/configuration/overview" >}})。

已创建的 JDBC Sink 能够保证至少一次的语义。
更有效的精确执行一次可以通过 upsert 语句或幂等更新实现。

用法示例：
{{< tabs "4ab65f13-608a-411a-8d24-e303f384ab5d" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
type_info = Types.ROW([Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
env.from_collection(
    [(101, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
     (102, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
     (103, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
     (104, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
     ], type_info=type_info) \
    .add_sink(
    JdbcSink.sink(
        "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url('jdbc:postgresql://dbhost:5432/postgresdb')
            .with_driver_name('org.postgresql.Driver')
            .with_user_name('someUser')
            .with_password('somePassword')
            .build()
    ))

env.execute()
```
{{< /tab >}}
{{< /tabs >}}

更多细节请查看 API documentation 。
