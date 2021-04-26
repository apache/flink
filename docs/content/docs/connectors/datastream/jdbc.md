---
title: JDBC
weight: 10
type: docs
aliases:
  - /dev/connectors/jdbc.html
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

This connector provides a sink that writes data to a JDBC database.

To use it, add the following dependency to your project (along with your JDBC-driver):

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-jdbc{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

Note that the streaming connectors are currently __NOT__ part of the binary distribution. See how to link with them for cluster execution [here]({{< ref "docs/dev/datastream/project-configuration" >}}).

Created JDBC sink provides at-least-once guarantee.
Effectively exactly-once can be achieved using upsert statements or idempotent updates.

Example usage:

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

Please refer to the [API documentation]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/connector/jdbc/JdbcSink.html) for more details.

## Exactly-once

Since 1.13, Flink JDBC sink supports exactly-once mode. The implementation relies on the JDBC driver support of XA [standard](https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf).

To use it, create a sink using `exactlyOnceSink()` method as above and additionally provide:
- [exactly-once options]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/connector/jdbc/JdbcExactlyOnceOptions.html)
- [execution options]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/connector/jdbc/JdbcExecutionOptions.html)
- [XA DataSource](https://docs.oracle.com/javase/8/docs/api/javax/sql/XADataSource.html) Supplier

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env
        .fromElements(...)
        .addSink(JdbcSink.exactlyOnceSink(
                "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.id);
                    ps.setString(2, t.title);
                    ps.setString(3, t.author);
                    ps.setDouble(4, t.price);
                    ps.setInt(5, t.qty);
                },
                JdbcExecutionOptions.builder().build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(getDbMetadata().getUrl())
                        .withDriverName(getDbMetadata().getDriverClass())
                        .build()),
                JdbcExactlyOnceOptions.defaults(),
                () -> {
                    // create a driver-specific XA DataSource
                    EmbeddedXADataSource ds = new EmbeddedXADataSource();
                    ds.setDatabaseName("my_db");
                    return ds;
                });
env.execute();
```

Please refer to the `JdbcXaSinkFunction` documentation for more details.
