---
title: "SQL JDBC Driver"
weight: 91
type: docs
aliases:
- /dev/table/jdbcDriver.html
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

# Flink JDBC Driver

Flink JDBC Driver is a Java library for connecting and submitting SQL statements to [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}) as the JDBC server.

# Usage

Before using Flink JDBC driver, you need to start a SQL Gateway as the JDBC server and binds it with your Flink cluster. We now assume that you have a gateway started and connected to a running Flink cluster.

## Use with a JDBC Tool
### Use with Beeline

Beeline is the command line tool for accessing [Apache Hive](https://hive.apache.org/), but it also supports general JDBC drivers. To install Hive and beeline, see [Hive documentation](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-RunningHiveServer2andBeeline.1).

1. Download flink-jdbc-driver-bundle-{VERSION}.jar and add it to `$HIVE_HOME/lib`.
2. Run beeline and connect to a Flink SQL gateway. As Flink SQL gateway currently ignores user names and passwords, just leave them empty.
    ```
    beeline> !connect jdbc:flink://localhost:8083
    ```
3. Execute any statement you want.

**Sample Commands**
```
Beeline version 3.1.3 by Apache Hive
beeline> !connect jdbc:flink://localhost:8083
Connecting to jdbc:flink://localhost:8083
Enter username for jdbc:flink://localhost:8083: 
Enter password for jdbc:flink://localhost:8083: 
Connected to: Flink JDBC Driver (version 1.18-SNAPSHOT)
Driver: org.apache.flink.table.jdbc.FlinkDriver (version 1.18-SNAPSHOT)
0: jdbc:flink://localhost:8083> CREATE TABLE T(
. . . . . . . . . . . . . . . >     a INT,
. . . . . . . . . . . . . . . >     b VARCHAR(10)
. . . . . . . . . . . . . . . > ) WITH (
. . . . . . . . . . . . . . . >     'connector' = 'filesystem',
. . . . . . . . . . . . . . . >     'path' = 'file:///tmp/T.csv',
. . . . . . . . . . . . . . . >     'format' = 'csv'
. . . . . . . . . . . . . . . > );
No rows affected (0.108 seconds)
0: jdbc:flink://localhost:8083> INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello');
+-----------------------------------+
|              job id               |
+-----------------------------------+
| da22010cf1c962b377493fc4fc509527  |
+-----------------------------------+
1 row selected (0.952 seconds)
0: jdbc:flink://localhost:8083> SELECT * FROM T;
+----+--------+
| a  |   b    |
+----+--------+
| 1  | Hi     |
| 2  | Hello  |
+----+--------+
2 rows selected (1.142 seconds)
0: jdbc:flink://localhost:8083> 
```

## Use with Java

Flink JDBC driver is a library for accessing Flink clusters through the JDBC API. For the general usage of JDBC in Java, see [JDBC tutorial](https://docs.oracle.com/javase/tutorial/jdbc/index.html).

1. Add the following dependency in pom.xml of project or download flink-jdbc-driver-bundle-{VERSION}.jar and add it to your classpath.
```
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-sql-jdbc-driver-bundle</artifactId>
      <version>{VERSION}</version>
    </dependency>
```
2. Connect to a Flink SQL gateway in your Java code with specific url.
3. Execute any statement you want.

**Sample.java**
```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Sample {
	public static void main(String[] args) throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:flink://localhost:8083")) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("CREATE TABLE T(\n" +
						"  a INT,\n" +
						"  b VARCHAR(10)\n" +
						") WITH (\n" +
						"  'connector' = 'filesystem',\n" +
						"  'path' = 'file:///tmp/T.csv',\n" +
						"  'format' = 'csv'\n" +
						")");
				statement.execute("INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')");
				try (ResultSet rs = statement.executeQuery("SELECT * FROM T")) {
					while (rs.next()) {
						System.out.println(rs.getInt(1) + ", " + rs.getString(2));
					}
				}
			}
		}
	}
}
```

**Output**
```
1, Hi
2, Hello
```
