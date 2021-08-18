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

To use it, add the following dependency to your project (along with your JDBC driver):

{{< artifact flink-connector-jdbc withScalaVersion >}}

A driver dependency is also required to connect to a specified database. Here are drivers currently supported:

| Driver      |      Group Id      |      Artifact Id       |      JAR         |
| :-----------| :------------------| :----------------------| :----------------|
| MySQL       |       `mysql`      | `mysql-connector-java` | [Download](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/) |
| PostgreSQL  |  `org.postgresql`  |      `postgresql`      | [Download](https://jdbc.postgresql.org/download.html) |
| Derby       | `org.apache.derby` |        `derby`         | [Download](http://db.apache.org/derby/derby_downloads.html) |

Note that the streaming connectors are currently __NOT__ part of the binary distribution. See how to link with them for cluster execution [here]({{< ref "docs/dev/datastream/project-configuration" >}}).

## `JdbcSink.sink`

The JDBC sink provides at-least-once guarantee.
Effectively though, exactly-once can be achieved by crafting upsert SQL statements or idempotent SQL updates.
Configuration goes as follow (see also {{< javadoc file="org/apache/flink/connector/jdbc/JdbcSink.html" name="JdbcSink javadoc" >}}).

```java
JdbcSink.sink(
      	sqlDmlStatement,                       // mandatory
      	jdbcStatementBuilder,                  // mandatory   	
      	jdbcExecutionOptions,                  // optional
      	jdbcConnectionOptions                  // mandatory
);
```        	

### SQL DML statement and JDBC statement builder

The sink builds one [JDBC prepared statement](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/PreparedStatement.html) from a user-provider SQL string, e.g.:

```sql
INSERT INTO some_table field1, field2 values (?, ?)
```

It then repeatedly calls a user-provided function to update that prepared statement with each value of the stream, e.g.:

```
(preparedStatement, someRecord) -> { ... update here the preparedStatement with values from someRecord ... }
```

### JDBC execution options

The SQL DML statements are executed in batches, which can optionally be configured with the following instance (see also {{< javadoc name="JdbcExecutionOptions javadoc" file="org/apache/flink/connector/jdbc/JdbcExecutionOptions.html" >}})

```java
JdbcExecutionOptions.builder()
        .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
        .withBatchSize(1000)                  // optional: default = 5000 values
        .withMaxRetries(5)                    // optional: default = 3 
.build()
```

A JDBC batch is executed as soon as one of the following conditions is true:

* the configured batch interval time is elapsed
* the maximum batch size is reached 
* a Flink checkpoint has started

### JDBC connection parameters

The connection to the database is configured with a `JdbcConnectionOptions` instance. 
Please see {{< javadoc name="JdbcConnectionOptions javadoc" file="org/apache/flink/connector/jdbc/JdbcConnectionOptions.html" >}} for details

### Full example

```java
public class JdbcSinkExample {

    static class Book {
        public Book(Long id, String title, String authors, Integer year) {
            this.id = id;
            this.title = title;
            this.authors = authors;
            this.year = year;
        }
        final Long id;
        final String title;
        final String authors;
        final Integer year;
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                new Book(101L, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
                new Book(102L, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
                new Book(103L, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
                new Book(104L, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
        ).addSink(
                JdbcSink.sink(
                        "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
                        (statement, book) -> {
                            statement.setLong(1, book.id);
                            statement.setString(2, book.title);
                            statement.setString(3, book.authors);
                            statement.setInt(4, book.year);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:postgresql://dbhost:5432/postgresdb")
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("someUser")
                                .withPassword("somePassword")
                                .build()
                ));
                
        env.execute();
    }
}
```

## `JdbcSink.exactlyOnceSink`

Since 1.13, Flink JDBC sink supports exactly-once mode. 
The implementation relies on the JDBC driver support of XA 
[standard](https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf).

To use it, create a sink using `exactlyOnceSink()` method as above and additionally provide:
- {{< javadoc name="exactly-once options" file="org/apache/flink/connector/jdbc/JdbcExactlyOnceOptions.html" >}}
- {{< javadoc name="execution options" file="org/apache/flink/connector/jdbc/JdbcExecutionOptions.html" >}}
- [XA DataSource](https://docs.oracle.com/javase/8/docs/api/javax/sql/XADataSource.html) Supplier

**ATTENTION!** Currently `JdbcSink.exactlyOnceSink` can ensure exactly once semantics
with `JdbcExecutionOptions.maxRetries == 0`; otherwise, duplicated results maybe produced.

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
                JdbcExecutionOptions.builder()
                    .withMaxRetries(0)
                    .build(),
                JdbcExactlyOnceOptions.defaults(),
                () -> {
                    // create a driver-specific XA DataSource
                    // The following example is for derby 
                    EmbeddedXADataSource ds = new EmbeddedXADataSource();
                    ds.setDatabaseName("my_db");
                    return ds;
                });
env.execute();
```
Postgres XADataSource Example:
```java
PGXADataSource pgxaDataSource = new PGXADataSource();
pgxaDataSource.setUrl(
"jdbc:postgresql://localhost:5432/postgres");
```

Please refer to the `JdbcXaSinkFunction` documentation for more details.
