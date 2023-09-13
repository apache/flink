---
title: "SQL 客户端"
weight: 91
type: docs
aliases:
  - /zh/dev/table/sqlClient.html
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

# SQL 客户端

Flink 的 Table & SQL API 可以处理 SQL 语言编写的查询语句，但是这些查询需要嵌入用 Java 或 Scala 编写的表程序中。此外，这些程序在提交到集群前需要用构建工具打包。这或多或少限制了 Java/Scala 程序员对 Flink 的使用。

*SQL 客户端* 的目的是提供一种简单的方式来编写、调试和提交表程序到 Flink 集群上，而无需写一行 Java 或 Scala 代码。*SQL 客户端命令行界面（CLI）* 能够在命令行中检索和可视化分布式应用中实时产生的结果。

{{< img width="80%" src="/fig/sql_client_demo.gif" alt="Animated demo of the Flink SQL Client CLI running table programs on a cluster" >}}



入门
---------------

本节介绍如何在命令行里启动（setup）和运行你的第一个 Flink SQL 程序。

SQL 客户端捆绑在常规 Flink 发行版中，因此可以直接运行。它仅需要一个正在运行的 Flink 集群就可以在其中执行表程序。有关设置 Flink 群集的更多信息，请参见[集群和部署]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})部分。如果仅想试用 SQL 客户端，也可以使用以下命令启动本地集群：

```bash
./bin/start-cluster.sh
```

### 启动 SQL 客户端命令行界面

SQL Client 脚本也位于 Flink 的 bin 目录中。用户可以通过启动嵌入式 standalone 进程或通过连接到远程 [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}) 来启动 SQL 客户端命令行界面。SQL 客户端默认使用 `embedded` 模式，你可以通过以下方式启动 CLI：

```bash
./bin/sql-client.sh
```

或者显式使用 `embedded` 模式:

```bash
./bin/sql-client.sh embedded
```

若想使用 gateway 模式，你可以通过以下命令启动 SQL 客户端：

```bash
./bin/sql-client.sh gateway --endpoint <gateway address>
```

The `<gateway address>` can be provided in two formats: as a `host:port` combination or as a full URL.

If you need to pass custom HTTP headers, you can do so by setting the FLINK_REST_CLIENT_HEADERS environment variable. For example:

```bash
export FLINK_REST_CLIENT_HEADERS="Cookie:myauthcookie=foobar;othercookie=baz"
./bin/sql-client.sh gateway --endpoint https://your-sql-gateway.endpoint.com/authenticated/sql
```
For multiple headers, separate them with a newline:

```bash
export FLINK_REST_CLIENT_HEADERS=$(cat << EOF
Cookie:myauthcookie=foobar
Cache-Control: no-cache
EOF)
```

By default, the SQL Client will use the truststore configured using the `security.ssl.rest.truststore` and `security.ssl.rest.truststore-password` properties in the `flink-conf.yaml` file on the SQL client side. If these properties aren't explicitly configured, the client will use the default certificate stores provided by the JDK.


<span class="label label-danger">Note</span> SQL 客户端目前只支持和 REST API 版本大于 v1 的 [REST Endpoint]({{< ref "docs/dev/table/sql-gateway/rest" >}}#rest-api) 通信。

参阅 [SQL Client startup options](#sql-client-startup-options) 了解更多启动命令。

### 执行 SQL 查询

命令行界面启动后，你可以使用 `HELP` 命令列出所有可用的 SQL 语句。输入第一条 SQL 查询语句并按 `Enter` 键执行，可以验证你的设置及集群连接是否正确：

```sql
SELECT 'Hello World';
```

该查询不需要 table source，并且只产生一行结果。CLI 将从集群中检索结果并将其可视化。按 `Q` 键退出结果视图。

CLI 为维护和可视化结果提供**三种模式**。

**表格模式**（table mode）在内存中实体化结果，并将结果用规则的分页表格可视化展示出来。执行如下命令启用：

```text
SET 'sql-client.execution.result-mode' = 'table';
```

**变更日志模式**（changelog mode）不会实体化和可视化结果，而是由插入（`+`）和撤销（`-`）组成的持续查询产生结果流。

```text
SET 'sql-client.execution.result-mode' = 'changelog';
```

**Tableau模式**（tableau mode）更接近传统的数据库，会将执行的结果以制表的形式直接打在屏幕之上。具体显示的内容会取决于作业
执行模式的不同(`execution.type`)：

```text
SET 'sql-client.execution.result-mode' = 'tableau';
```

注意当你使用这个模式运行一个流式查询的时候，Flink 会将结果持续的打印在当前的屏幕之上。如果这个流式查询的输入是有限的数据集，
那么Flink在处理完所有的数据之后，会自动的停止作业，同时屏幕上的打印也会相应的停止。如果你想提前结束这个查询，那么可以直接使用
`CTRL-C` 按键，这个会停掉作业同时停止屏幕上的打印。

你可以用如下查询来查看三种结果模式的运行情况：

```sql
SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name;
```

此查询执行一个有限字数示例：

*变更日志模式* 下，看到的结果应该类似于：

```text
+ Bob, 1
+ Alice, 1
+ Greg, 1
- Bob, 1
+ Bob, 2
```

*表格模式* 下，可视化结果表将不断更新，直到表程序以如下内容结束：

```text
Bob, 2
Alice, 1
Greg, 1
```

*Tableau模式* 下，如果这个查询以流的方式执行，那么将显示以下内容：
```text
+-----+----------------------+----------------------+
| +/- |                 name |                  cnt |
+-----+----------------------+----------------------+
|   + |                  Bob |                    1 |
|   + |                Alice |                    1 |
|   + |                 Greg |                    1 |
|   - |                  Bob |                    1 |
|   + |                  Bob |                    2 |
+-----+----------------------+----------------------+
Received a total of 5 rows
```

如果这个查询以批的方式执行，显示的内容如下：
```text
+-------+-----+
|  name | cnt |
+-------+-----+
| Alice |   1 |
|   Bob |   2 |
|  Greg |   1 |
+-------+-----+
3 rows in set
```

这几种结果模式在 SQL 查询的原型设计过程中都非常有用。这些模式的结果都存储在 SQL 客户端 的 Java 堆内存中。为了保持 CLI
界面及时响应，变更日志模式仅显示最近的 1000 个更改。表格模式支持浏览更大的结果，这些结果仅受可用主内存和配置的[最大行数](#sql-client-execution-max-table-result-rows)（`sql-client.execution.max-table-result.rows`）的限制。

<span class="label label-danger">注意</span> 在批处理环境下执行的查询只能用表格模式或者Tableau模式进行检索。

定义查询语句后，可以将其作为长时间运行的独立 Flink 作业提交给集群。[配置部分](#configuration)解释如何声明读取数据的 table source，写入数据的 sink 以及配置其他表程序属性的方法。

{{< top >}}

### Key-strokes

There is a list of available key-strokes in SQL Client

| Key-Stroke (Linux, Windows(WSL)) | Key-Stroke (Mac) | Description                                                                            |
|:---------------------------------|------------------|:---------------------------------------------------------------------------------------|
| `alt-b`, `ctrl+⍇`                | `Esc-b`          | Backward word                                                                          |
| `alt-f`, `Ctrl+⍈`                | `Esc-f`          | Forward word                                                                           |
| `alt-c`                          | `Esc-c`          | Capitalize word                                                                        |
| `alt-l`                          | `Esc-l`          | Lowercase word                                                                         |
| `alt-u`                          | `Esc-u`          | Uppercase word                                                                         |
| `alt-d`                          | `Esc-d`          | Kill word                                                                              |
| `alt-n`                          | `Esc-n`          | History search forward (behaves same as down line from history in case of empty input) |
| `alt-p`                          | `Esc-p`          | History search backward (behaves same as up line from history in case of empty input)  |
| `alt-t`                          | `Esc-t`          | Transpose words                                                                        |
| `ctrl-a`                         | `⌘-a`            | To the beginning of line                                                               |
| `ctrl-e`                         | `⌘-e`            | To the end of line                                                                     |
| `ctrl-b`                         | `⌘-b`            | Backward char                                                                          |
| `ctrl-f`                         | `⌘-f`            | Forward char                                                                           |
| `ctrl-d`                         | `⌘-d`            | Delete char                                                                            |
| `ctrl-h`                         | `⌘-h`            | Backward delete char                                                                   |
| `ctrl-t`                         | `⌘-t`            | Transpose chars                                                                        |
| `ctrl-i`                         | `⌘-i`            | Invoke completion                                                                      |
| `ctrl-j`                         | `⌘-j`            | Submit a query                                                                         |
| `ctrl-m`                         | `⌘-m`            | Submit a query                                                                         |
| `ctrl-k`                         | `⌘-k`            | Kill the line to the right from the cursor                                             |
| `ctrl-w`                         | `⌘-w`            | Kill the line to the left from the cursor                                              |
| `ctrl-u`                         | `⌘-u`            | Kill the whole line                                                                    |
| `ctrl-l`                         | `⌘-l`            | Clear screen                                                                           |
| `ctrl-n`                         | `⌘-n`            | Down line from history                                                                 |
| `ctrl-p`                         | `⌘-p`            | Up line from history                                                                   |
| `ctrl-r`                         | `⌘-r`            | History incremental search backward                                                    |
| `ctrl-s`                         | `⌘-s`            | History incremental search forward                                                     |

### Getting help

The documentation of the SQL Client commands can be accessed by typing the `HELP` command.

See also the general [SQL]({{< ref "docs/dev/table/sql/overview" >}}) documentation.

{{< top >}}

Configuration
-------------

### SQL Client startup options

The SQL Client can be started with the following optional CLI commands. They are discussed in detail in the subsequent paragraphs.

```text
./sql-client [MODE] [OPTIONS]

The following options are available:

Mode "embedded" (default) submits Flink jobs from the local machine.

  Syntax: [embedded] [OPTIONS]
  "embedded" mode options:
     -D <session dynamic config key=val>        The dynamic config key=val for a
                                                session.
     -f,--file <script file>                    Script file that should be
                                                executed. In this mode, the
                                                client will not open an
                                                interactive terminal.
     -h,--help                                  Show the help message with
                                                descriptions of all options.
     -hist,--history <History file path>        The file which you want to save
                                                the command history into. If not
                                                specified, we will auto-generate
                                                one under your user's home
                                                directory.
     -i,--init <initialization file>            Script file that used to init
                                                the session context. If get
                                                error in execution, the sql
                                                client will exit. Notice it's
                                                not allowed to add query or
                                                insert into the init file.
     -j,--jar <JAR file>                        A JAR file to be imported into
                                                the session. The file might
                                                contain user-defined classes
                                                needed for the execution of
                                                statements such as functions,
                                                table sources, or sinks. Can be
                                                used multiple times.
     -l,--library <JAR directory>               A JAR file directory with which
                                                every new session is
                                                initialized. The files might
                                                contain user-defined classes
                                                needed for the execution of
                                                statements such as functions,
                                                table sources, or sinks. Can be
                                                used multiple times.
     -pyarch,--pyArchives <arg>                 Add python archive files for
                                                job. The archive files will be
                                                extracted to the working
                                                directory of python UDF worker.
                                                For each archive file, a target
                                                directory be specified. If the
                                                target directory name is
                                                specified, the archive file will
                                                be extracted to a directory with
                                                the specified name. Otherwise,
                                                the archive file will be
                                                extracted to a directory with
                                                the same name of the archive
                                                file. The files uploaded via
                                                this option are accessible via
                                                relative path. '#' could be used
                                                as the separator of the archive
                                                file path and the target
                                                directory name. Comma (',')
                                                could be used as the separator
                                                to specify multiple archive
                                                files. This option can be used
                                                to upload the virtual
                                                environment, the data files used
                                                in Python UDF (e.g.,
                                                --pyArchives
                                                file:///tmp/py37.zip,file:///tmp
                                                /data.zip#data --pyExecutable
                                                py37.zip/py37/bin/python). The
                                                data files could be accessed in
                                                Python UDF, e.g.: f =
                                                open('data/data.txt', 'r').
     -pyclientexec,--pyClientExecutable <arg>   The path of the Python
                                                interpreter used to launch the
                                                Python process when submitting
                                                the Python jobs via "flink run"
                                                or compiling the Java/Scala jobs
                                                containing Python UDFs.
     -pyexec,--pyExecutable <arg>               Specify the path of the python
                                                interpreter used to execute the
                                                python UDF worker (e.g.:
                                                --pyExecutable
                                                /usr/local/bin/python3). The
                                                python UDF worker depends on
                                                Python 3.8+, Apache Beam
                                                (version == 2.43.0), Pip
                                                (version >= 20.3) and SetupTools
                                                (version >= 37.0.0). Please
                                                ensure that the specified
                                                environment meets the above
                                                requirements.
     -pyfs,--pyFiles <pythonFiles>              Attach custom files for job. The
                                                standard resource file suffixes
                                                such as .py/.egg/.zip/.whl or
                                                directory are all supported.
                                                These files will be added to the
                                                PYTHONPATH of both the local
                                                client and the remote python UDF
                                                worker. Files suffixed with .zip
                                                will be extracted and added to
                                                PYTHONPATH. Comma (',') could be
                                                used as the separator to specify
                                                multiple files (e.g., --pyFiles
                                                file:///tmp/myresource.zip,hdfs:
                                                ///$namenode_address/myresource2
                                                .zip).
     -pyreq,--pyRequirements <arg>              Specify a requirements.txt file
                                                which defines the third-party
                                                dependencies. These dependencies
                                                will be installed and added to
                                                the PYTHONPATH of the python UDF
                                                worker. A directory which
                                                contains the installation
                                                packages of these dependencies
                                                could be specified optionally.
                                                Use '#' as the separator if the
                                                optional parameter exists (e.g.,
                                                --pyRequirements
                                                file:///tmp/requirements.txt#fil
                                                e:///tmp/cached_dir).
     -s,--session <session identifier>          The identifier for a session.
                                                'default' is the default
                                                identifier.
     -u,--update <SQL update statement>         Deprecated Experimental (for
                                                testing only!) feature:
                                                Instructs the SQL Client to
                                                immediately execute the given
                                                update statement after starting
                                                up. The process is shut down
                                                after the statement has been
                                                submitted to the cluster and
                                                returns an appropriate return
                                                code. Currently, this feature is
                                                only supported for INSERT INTO
                                                statements that declare the
                                                target sink table.Please use
                                                option -f to submit update
                                                statement.


Mode "gateway" mode connects to the SQL gateway for submission.

  Syntax: gateway [OPTIONS]
  "gateway" mode options:
     -D <session dynamic config key=val>   The dynamic config key=val for a
                                           session.
     -e,--endpoint <SQL Gateway address>   The address of the remote SQL Gateway
                                           to connect.
     -f,--file <script file>               Script file that should be executed.
                                           In this mode, the client will not
                                           open an interactive terminal.
     -h,--help                             Show the help message with
                                           descriptions of all options.
     -hist,--history <History file path>   The file which you want to save the
                                           command history into. If not
                                           specified, we will auto-generate one
                                           under your user's home directory.
     -i,--init <initialization file>       Script file that used to init the
                                           session context. If get error in
                                           execution, the sql client will exit.
                                           Notice it's not allowed to add query
                                           or insert into the init file.
     -s,--session <session identifier>     The identifier for a session.
                                           'default' is the default identifier.
     -u,--update <SQL update statement>    Deprecated Experimental (for testing
                                           only!) feature: Instructs the SQL
                                           Client to immediately execute the
                                           given update statement after starting
                                           up. The process is shut down after
                                           the statement has been submitted to
                                           the cluster and returns an
                                           appropriate return code. Currently,
                                           this feature is only supported for
                                           INSERT INTO statements that declare
                                           the target sink table.Please use
                                           option -f to submit update statement.
```

### SQL Client Configuration

You can configure the SQL Client by setting the options below, or any valid [Flink configuration]({{< ref "docs/dev/table/config" >}}) entry:

```sql
SET 'key' = 'value';
```

{{< generated/sql_client_configuration >}}

### SQL Client result modes

The CLI supports **three modes** for maintaining and visualizing results.

The **table mode** materializes results in memory and visualizes them in a regular, paginated table representation.
It can be enabled by executing the following command in the CLI:

```text
SET 'sql-client.execution.result-mode' = 'table';
```

The result of a query would then look like this, you can use the keys indicated at the bottom of the screen as well
as the arrows keys to navigate and open the various records:

```text

                           name         age isHappy        dob                         height
                          user1          20    true 1995-12-03                            1.7
                          user2          30    true 1972-08-02                           1.89
                          user3          40   false 1983-12-23                           1.63
                          user4          41    true 1977-11-13                           1.72
                          user5          22   false 1998-02-20                           1.61
                          user6          12    true 1969-04-08                           1.58
                          user7          38   false 1987-12-15                            1.6
                          user8          62    true 1996-08-05                           1.82




Q Quit                     + Inc Refresh              G Goto Page                N Next Page                O Open Row
R Refresh                  - Dec Refresh              L Last Page                P Prev Page
```

The **changelog mode** does not materialize results and visualizes the result stream that is produced
by a [continuous query]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#continuous-queries) consisting of insertions (`+`) and retractions (`-`).

```text
SET 'sql-client.execution.result-mode' = 'changelog';
```

The result of a query would then look like this:

```text
 op                           name         age isHappy        dob                         height
 +I                          user1          20    true 1995-12-03                            1.7
 +I                          user2          30    true 1972-08-02                           1.89
 +I                          user3          40   false 1983-12-23                           1.63
 +I                          user4          41    true 1977-11-13                           1.72
 +I                          user5          22   false 1998-02-20                           1.61
 +I                          user6          12    true 1969-04-08                           1.58
 +I                          user7          38   false 1987-12-15                            1.6
 +I                          user8          62    true 1996-08-05                           1.82




Q Quit                                       + Inc Refresh                                O Open Row
R Refresh                                    - Dec Refresh

```

The **tableau mode** is more like a traditional way which will display the results in the screen directly with a tableau format.
The displaying content will be influenced by the query execution type (`execution.type`).

```text
SET 'sql-client.execution.result-mode' = 'tableau';
```

The result of a query would then look like this:

```text
+----+--------------------------------+-------------+---------+------------+--------------------------------+
| op |                           name |         age | isHappy |        dob |                         height |
+----+--------------------------------+-------------+---------+------------+--------------------------------+
| +I |                          user1 |          20 |    true | 1995-12-03 |                            1.7 |
| +I |                          user2 |          30 |    true | 1972-08-02 |                           1.89 |
| +I |                          user3 |          40 |   false | 1983-12-23 |                           1.63 |
| +I |                          user4 |          41 |    true | 1977-11-13 |                           1.72 |
| +I |                          user5 |          22 |   false | 1998-02-20 |                           1.61 |
| +I |                          user6 |          12 |    true | 1969-04-08 |                           1.58 |
| +I |                          user7 |          38 |   false | 1987-12-15 |                            1.6 |
| +I |                          user8 |          62 |    true | 1996-08-05 |                           1.82 |
+----+--------------------------------+-------------+---------+------------+--------------------------------+
Received a total of 8 rows
```

Note that when you use this mode with streaming query, the result will be continuously printed on the console. If the input data of
this query is bounded, the job will terminate after Flink processed all input data, and the printing will also be stopped automatically.
Otherwise, if you want to terminate a running query, just type `CTRL-C` in this case, the job and the printing will be stopped.

All these result modes can be useful during the prototyping of SQL queries. In all these modes,
results are stored in the Java heap memory of the SQL Client. In order to keep the CLI interface responsive,
the changelog mode only shows the latest 1000 changes. The table mode allows for navigating through
bigger results that are only limited by the available main memory and the configured
[maximum number of rows](#sql-client-execution-max-table-result-rows) (`sql-client.execution.max-table-result.rows`).

<span class="label label-danger">Attention</span> Queries that are executed in a batch environment, can only be retrieved using the `table` or `tableau` result mode.

### Initialize Session Using SQL Files

A SQL query needs a configuration environment in which it is executed. SQL Client supports the `-i`
startup option to execute an initialization SQL file to setup environment when starting up the SQL Client.
The so-called *initialization SQL file* can use DDLs to define available catalogs, table sources and sinks,
user-defined functions, and other properties required for execution and deployment.

An example of such a file is presented below.

```sql
-- Define available catalogs

CREATE CATALOG MyCatalog
  WITH (
    'type' = 'hive'
  );

USE CATALOG MyCatalog;

-- Define available database

CREATE DATABASE MyDatabase;

USE MyDatabase;

-- Define TABLE

CREATE TABLE MyTable(
  MyField1 INT,
  MyField2 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '/path/to/something',
  'format' = 'csv'
);

-- Define VIEW

CREATE VIEW MyCustomView AS SELECT MyField2 FROM MyTable;

-- Define user-defined functions here.

CREATE FUNCTION myUDF AS 'foo.bar.AggregateUDF';

-- Properties that change the fundamental execution behavior of a table program.

SET 'execution.runtime-mode' = 'streaming'; -- execution mode either 'batch' or 'streaming'
SET 'sql-client.execution.result-mode' = 'table'; -- available values: 'table', 'changelog' and 'tableau'
SET 'sql-client.execution.max-table-result.rows' = '10000'; -- optional: maximum number of maintained rows
SET 'parallelism.default' = '1'; -- optional: Flink's parallelism (1 by default)
SET 'pipeline.auto-watermark-interval' = '200'; --optional: interval for periodic watermarks
SET 'pipeline.max-parallelism' = '10'; -- optional: Flink's maximum parallelism
SET 'table.exec.state.ttl' = '1000'; -- optional: table program's idle state time
SET 'restart-strategy.type' = 'fixed-delay';

-- Configuration options for adjusting and tuning table programs.

SET 'table.optimizer.join-reorder-enabled' = 'true';
SET 'table.exec.spill-compression.enabled' = 'true';
SET 'table.exec.spill-compression.block-size' = '128kb';
```

This configuration:

- connects to Hive catalogs and uses `MyCatalog` as the current catalog with `MyDatabase` as the current database of the catalog,
- defines a table `MyTable` that can read data from a CSV file,
- defines a view `MyCustomView` that declares a virtual table using a SQL query,
- defines a user-defined function `myUDF` that can be instantiated using the class name,
- uses streaming mode for running statements and a parallelism of 1,
- runs exploratory queries in the `table` result mode,
- and makes some planner adjustments around join reordering and spilling via configuration options.

When using `-i <init.sql>` option to initialize SQL Client session, the following statements are allowed in an initialization SQL file:
- DDL(CREATE/DROP/ALTER),
- USE CATALOG/DATABASE,
- LOAD/UNLOAD MODULE,
- SET command,
- RESET command.

When execute queries or insert statements, please enter the interactive mode or use the -f option to submit the SQL statements.

<span class="label label-danger">Attention</span> If SQL Client receives errors during initialization, SQL Client will exit with error messages.

### Dependencies

The SQL Client does not require setting up a Java project using Maven, Gradle, or sbt. Instead, you
can pass the dependencies as regular JAR files that get submitted to the cluster. You can either specify
each JAR file separately (using `--jar`) or define entire library directories (using `--library`). For
connectors to external systems (such as Apache Kafka) and corresponding data formats (such as JSON),
Flink provides **ready-to-use JAR bundles**. These JAR files can be downloaded for each release from
the Maven central repository.

The full list of offered SQL JARs can be found on the [connection to external systems page]({{< ref "docs/connectors/table/overview" >}}).

You can refer to the [configuration]({{< ref "docs/dev/configuration/connector" >}}) section for
information on how to configure connector and format dependencies.

{{< top >}}

Usage
----------------------------

SQL Client allows users to submit jobs either within the interactive command line or using `-f` option to execute sql file.

In both modes, SQL Client supports to parse and execute all types of the Flink supported SQL statements.

### Interactive Command Line

In interactive Command Line, the SQL Client reads user inputs and executes the statement terminated by a semicolon (`;`).

SQL Client will print success message if the statement is executed successfully. When getting errors, SQL Client will also print error messages.
By default, the error message only contains the error cause. In order to print the full exception stack for debugging, please set the
`sql-client.verbose` to true through command `SET 'sql-client.verbose' = 'true';`.

### Execute SQL Files

SQL Client supports to execute a SQL script file with the `-f` option. SQL Client will execute
statements one by one in the SQL script file and print execution messages for each executed statements.
Once a statement fails, the SQL Client will exit and all the remaining statements will not be executed.

An example of such a file is presented below.

```sql
CREATE TEMPORARY TABLE users (
  user_id BIGINT,
  user_name STRING,
  user_level STRING,
  region STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'users',
  'properties.bootstrap.servers' = '...',
  'key.format' = 'csv',
  'value.format' = 'avro'
);

-- set sync mode
SET 'table.dml-sync' = 'true';

-- set the job name
SET 'pipeline.name' = 'SqlJob';

-- set the queue that the job submit to
SET 'yarn.application.queue' = 'root';

-- set the job parallelism
SET 'parallelism.default' = '100';

-- restore from the specific savepoint path
SET 'execution.savepoint.path' = '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab';

INSERT INTO pageviews_enriched
SELECT *
FROM pageviews AS p
LEFT JOIN users FOR SYSTEM_TIME AS OF p.proctime AS u
ON p.user_id = u.user_id;
```
This configuration:

- defines a temporal table source `users` that reads from a CSV file,
- set the properties, e.g job name,
- set the savepoint path,
- submit a sql job that load the savepoint from the specified savepoint path.

<span class="label label-danger">Attention</span> Compared to the interactive mode, SQL Client will stop execution and exits when there are errors.

### Execute a set of SQL statements

SQL Client execute each INSERT INTO statement as a single Flink job. However, this is sometimes not
optimal because some part of the pipeline can be reused. SQL Client supports STATEMENT SET syntax to
execute a set of SQL statements. This is an equivalent feature with StatementSet in Table API. The
`STATEMENT SET` syntax encloses one or more `INSERT INTO` statements. All statements in a `STATEMENT SET`
block are holistically optimized and executed as a single Flink job. Joint optimization and execution
allows for reusing common intermediate results and can therefore significantly improve the efficiency
of executing multiple queries.

#### Syntax
```sql
EXECUTE STATEMENT SET 
BEGIN
  -- one or more INSERT INTO statements
  { INSERT INTO|OVERWRITE <select_statement>; }+
END;
```

<span class="label label-danger">Attention</span> The statements of enclosed in the `STATEMENT SET` must be separated by a semicolon (;).
The old syntax `BEGIN STATEMENT SET; ... END;` is deprecated, may be removed in the future version.

{{< tabs "statement set" >}}

{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE pageviews (
>   user_id BIGINT,
>   page_id BIGINT,
>   viewtime TIMESTAMP,
>   proctime AS PROCTIME()
> ) WITH (
>   'connector' = 'kafka',
>   'topic' = 'pageviews',
>   'properties.bootstrap.servers' = '...',
>   'format' = 'avro'
> );
[INFO] Execute statement succeed.

Flink SQL> CREATE TABLE pageview (
>   page_id BIGINT,
>   cnt BIGINT
> ) WITH (
>   'connector' = 'jdbc',
>   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
>   'table-name' = 'pageview'
> );
[INFO] Execute statement succeed.

Flink SQL> CREATE TABLE uniqueview (
>   page_id BIGINT,
>   cnt BIGINT
> ) WITH (
>   'connector' = 'jdbc',
>   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
>   'table-name' = 'uniqueview'
> );
[INFO] Execute statement succeed.

Flink SQL> EXECUTE STATEMENT SET
> BEGIN
>
> INSERT INTO pageview
> SELECT page_id, count(1)
> FROM pageviews
> GROUP BY page_id;
>
> INSERT INTO uniqueview
> SELECT page_id, count(distinct user_id)
> FROM pageviews
> GROUP BY page_id;
>
> END;
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 6b1af540c0c0bb3fcfcad50ac037c862
```
{{< /tab >}}

{{< tab "SQL File" >}}
```sql
CREATE TABLE pageviews (
  user_id BIGINT,
  page_id BIGINT,
  viewtime TIMESTAMP,
  proctime AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'pageviews',
  'properties.bootstrap.servers' = '...',
  'format' = 'avro'
);

CREATE TABLE pageview (
  page_id BIGINT,
  cnt BIGINT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/mydatabase',
  'table-name' = 'pageview'
);

CREATE TABLE uniqueview (
  page_id BIGINT,
  cnt BIGINT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/mydatabase',
  'table-name' = 'uniqueview'
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO pageview
SELECT page_id, count(1)
FROM pageviews
GROUP BY page_id;

INSERT INTO uniqueview
SELECT page_id, count(distinct user_id)
FROM pageviews
GROUP BY page_id;

END;
```
{{< /tab >}}
{{< /tabs >}}

### Execute DML statements sync/async

By default, SQL Client executes DML statements asynchronously. That means, SQL Client will submit a
job for the DML statement to a Flink cluster, and not wait for the job to finish.
So SQL Client can submit multiple jobs at the same time. This is useful for streaming jobs, which are long-running in general.

SQL Client makes sure that a statement is successfully submitted to the cluster. Once the statement
is submitted, the CLI will show information about the Flink job.

```sql
Flink SQL> INSERT INTO MyTableSink SELECT * FROM MyTableSource;
[INFO] Table update statement has been successfully submitted to the cluster:
Cluster ID: StandaloneClusterId
Job ID: 6f922fe5cba87406ff23ae4a7bb79044
```

<span class="label label-danger">Attention</span> The SQL Client does not track the status of the
running Flink job after submission. The CLI process can be shutdown after the submission without
affecting the detached query. Flink's `restart strategy` takes care of the fault-tolerance. Please
use the job statements to [monitor the detached query status]({{< ref "docs/dev/table/sqlClient" >}}#monitoring-job-status)
or [stop the detached query]({{< ref "docs/dev/table/sqlClient" >}}#terminating-a-job).

However, for batch users, it's more common that the next DML statement requires waiting until the
previous DML statement finishes. In order to execute DML statements synchronously, you can set
`table.dml-sync` option to `true` in SQL Client.

```sql
Flink SQL> SET 'table.dml-sync' = 'true';
[INFO] Session property has been set.

Flink SQL> INSERT INTO MyTableSink SELECT * FROM MyTableSource;
[INFO] Submitting SQL update statement to the cluster...
[INFO] Execute statement in sync mode. Please wait for the execution finish...
[INFO] Complete execution of the SQL update statement.
```

<span class="label label-danger">Attention</span>  If you want to terminate the job, just type `CTRL-C` to cancel the execution.

### Start a SQL Job from a savepoint

Flink supports to start the job with specified savepoint. In SQL Client, it's allowed to use `SET` command to specify the path of the savepoint.

```sql
Flink SQL> SET 'execution.savepoint.path' = '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab';
[INFO] Session property has been set.

-- all the following DML statements will be restroed from the specified savepoint path
Flink SQL> INSERT INTO ...
```

When the path to savepoint is specified, Flink will try to restore the state from the savepoint when executing all the following DML statements.

Because the specified savepoint path will affect all the following DML statements, you can use `RESET` command to reset this config option, i.e. disable restoring from savepoint.

```sql
Flink SQL> RESET execution.savepoint.path;
[INFO] Session property has been reset.
```

For more details about creating and managing savepoints, please refer to [Job Lifecycle Management]({{< ref "docs/deployment/cli" >}}#job-lifecycle-management).

### Define a Custom Job Name

SQL Client supports to define job name for queries and DML statements through `SET` command.

```sql
Flink SQL> SET 'pipeline.name' = 'kafka-to-hive';
[INFO] Session property has been set.

-- all the following DML statements will use the specified job name.
Flink SQL> INSERT INTO ...
```

Because the specified job name will affect all the following queries and DML statements, you can also use `RESET` command to reset this configuration, i.e. use default job names.

```sql
Flink SQL> RESET pipeline.name;
[INFO] Session property has been reset.
```

If the option `pipeline.name` is not specified, SQL Client will generate a default name for the submitted job, e.g. `insert-into_<sink_table_name>` for `INSERT INTO` statements.

### Monitoring Job Status

SQL Client supports to list jobs status in the cluster through `SHOW JOBS` statements.

```sql
Flink SQL> SHOW JOBS;
+----------------------------------+---------------+----------+-------------------------+
|                           job id |      job name |   status |              start time |
+----------------------------------+---------------+----------+-------------------------+
| 228d70913eab60dda85c5e7f78b5782c | kafka-to-hive |  RUNNING | 2023-02-11T05:03:51.523 |
+----------------------------------+---------------+----------+-------------------------+
```

### Terminating a Job

SQL Client supports to stop jobs with or without savepoints through `STOP JOB` statements.

```sql
Flink SQL> STOP JOB '228d70913eab60dda85c5e7f78b5782c' WITH SAVEPOINT;
+-----------------------------------------+
|                          savepoint path |
+-----------------------------------------+
| file:/tmp/savepoint-3addd4-0b224d9311e6 |
+-----------------------------------------+
```

The savepoint path could be specified with [state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir)
either in the cluster configuration or session configuration (the latter would take precedence).

For more details about stopping jobs, please refer to [Job Statements]({{< ref "docs/dev/table/sql/job" >}}#stop-job).

### SQL Syntax highlighting

SQL Client can highlight SQL syntax with several color schemes.
With `sql-client.display.color-schema` it could be set a color scheme.
Available color schemes: `chester`, `dracula`, `solarized`, `vs2010`, `obsidian`, `geshi`, `dark`, `light`, `default` (no highlighting).
In case of wrong name the fallback is to `default`.

| Color schema \ Style | Keyword      | Default | Comment        | Hint         | Quoted  | SQL Identifier |
|:---------------------|--------------|:--------|:---------------|:-------------|:--------|:---------------|
| `Default`            | Default      | Default | Default        | Default      | Default | Default        |
| `Chester`            | Bold blue    | White   | Italic green   | Bold green   | Red     | Cyan           |
| `Dark`               | Bold blue    | White   | Italic bright  | Bold bright  | Green   | Cyan           |
| `Dracula`            | Bold magenta | White   | Italic cyan    | Bold cyan    | Green   | Red            |
| `Geshi`              | Bold #993333 | White   | Italic #808080 | Bold #808080 | #66CC66 | #000099        |
| `Light`              | Bold red     | Black   | Italic bright  | Bold bright  | Green   | Cyan           |
| `Obsidian`           | Bold green   | White   | Italic bright  | Bold bright  | Red     | Magenta        |
| `VS2010`             | Bold blue    | White   | Italic green   | Bold green   | Red     | Magenta        |
| `Solarized`          | Bold yellow  | Blue    | Italic bright  | Bold bright  | Green   | Red            |

{{< top >}}
