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

在 gateway 模式下，客户端将 SQL 提交到指定的远端 gateway 并执行。
<span class="label label-danger">Note</span> SQL 客户端目前只支持和 REST API 版本大于 v1 的 [REST Endpoint]({{< ref "docs/dev/table/sql-gateway/rest" >}}#rest-api) 通信。

参阅 [SQL Client startup options](#sql-client-startup-options) 了解更多启动命令。

### 执行 SQL 查询

输入以下简单查询并按 `Enter` 键执行，可以验证你的设置及集群连接是否正确：

```sql
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

SELECT
    name,
    COUNT(*) AS cnt
FROM
        (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name)
GROUP BY name;
```

SQL 客户端会从集群获取结果并将其可视化（按 `Q` 键退出结果视图）：

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

你可以通过 `SET` 命令调整作业执行和 sql 客户端的行为。参阅 [SQL Client Configuration](#sql-client-configuration) 了解更多信息。
一个查询可以作为独立的长时间运行的 Flink 作业被提交到集群。[configuration section](#configuration) 描述了如何声明读取数据的 table source，写入
数据的 sink 以及配置其他表程序属性的方法。

### 快捷键

SQL 客户端中可用的快捷键列表如下

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

### 获取帮助

可以在 SQL 客户端输入 `HELP` 命令获得文档中的命令列表。也可以参阅文档 [SQL]({{< ref "docs/dev/table/sql/overview" >}})。

{{< top >}}

配置
-------------

### SQL 客户端启动参数

可以通过以下 CLI 命令启动 SQL 客户端。它们将在后续的章节中详细讨论。

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
                                                Python 3.7+, Apache Beam
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

### SQL 客户端配置

你可以通过以下方式设置 SQL 客户端参数，或者任意合法的 [Flink configuration]({{< ref "docs/dev/table/config" >}}) 配置项：

```sql
SET 'key' = 'value';
```

{{< generated/sql_client_configuration >}}

### SQL 客户端结果模式

CLI 为维护和可视化结果提供**三种模式**。

**表格模式**（table mode）在内存中实体化结果，并将结果用规则的分页表格可视化展示出来。执行如下命令启用：

```text
SET 'sql-client.execution.result-mode' = 'table';
```

查询结果最终展示如下，你可以使用屏幕底部指示的按键和箭头键来选取和打开不同记录：

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

**变更日志模式**（changelog mode）不会实体化和可视化结果，而是由插入（`+`）和撤销（`-`）组成的持续查询 [continuous query]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#continuous-queries) 产生结果流。

```text
SET 'sql-client.execution.result-mode' = 'changelog';
```

查询结果最终展示如下：

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

**Tableau模式**（tableau mode）更接近传统的数据库，会将执行的结果以制表的形式直接打在屏幕之上。具体显示的内容会取决于作业执行模式的不同(`execution.type`)。

```text
SET 'sql-client.execution.result-mode' = 'tableau';
```

查询结果最终展示如下：

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

注意，当你使用这个模式运行一个流式查询的时候，Flink 会将结果持续的打印在当前的屏幕之上。如果这个流式查询的输入是有限的数据集，那么Flink在处理完所有的
数据之后，会自动的停止作业，同时屏幕上的打印也会相应的停止。如果你想提前结束这个查询，那么可以直接使用 `CTRL-C` 按键，这个会停掉作业同时停止屏幕上的打印。

这几种结果模式在 SQL 查询的原型设计过程中都非常有用。这些模式的结果都存储在 SQL 客户端 的 Java 堆内存中。为了保持 CLI
界面及时响应，变更日志模式仅显示最近的 1000 个更改。表格模式支持浏览更大的结果，这些结果仅受可用主内存和配置的[最大行数](#sql-client-execution-max-table-result-rows)（`sql-client.execution.max-table-result.rows`）的限制。

<span class="label label-danger">注意</span> 在批处理环境下执行的查询只能用表格模式或者Tableau模式进行检索。

### 使用 SQL 文件初始化 Session

SQL 查询执行需要配置环境。SQL 客户端支持 `-i` 参数，在启动时执行一个 SQL 文件来初始化环境。*初始化 SQL 文件* 中可以使用 DDL 定义 catalogs，
table sources 和 sinks，用户自定义函数以及其他查询执行和部署所需的配置。

初始化文件的示例如下所示。

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

CREATE FUNCTION foo.bar.AggregateUDF AS myUDF;

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

内容包括：

- 连接到 Hive catalogs 并使用 `MyCatalog` 和 `MyDatabase` 作为当前的 catalog 和数据库，
- 定义从 CSV 文件读取数据的表 `MyTable`，
- 使用 SQL 查询定义视图 `MyCustomView`，
- 定义自定义函数 `myUDF`并且可以用类名实例化该函数，
- 使用流模式执行 SQL 语句并设置并发度为 1，
- 用 `table` 结果模式执行查询，
- 设置一些调整执行计划 join 顺序和数据溢出的配置项。

使用 `-i <init.sql>` 参数初始化 session 的 SQL 文件可以包含以下 SQL 语句：
- DDL(CREATE/DROP/ALTER),
- USE CATALOG/DATABASE,
- LOAD/UNLOAD MODULE,
- SET command,
- RESET command.

如果需要执行查询或者插入的 SQL 语句，请使用交互模式或者使用 -f 参数提交 SQL 文件。

<span class="label label-danger">注意</span> 如果 SQL 客户端在执行初始化文件时出错，会报错并退出。

### 依赖

SQL 客户端可以直接将依赖的 JAR 文件提交到集群，不需要使用 Maven、Gradle 或者 sbt 启动一个 Java 项目。你可以使用 `--jar`
指定每个 JAR 文件，也可以使用 `--library` 指定整个依赖目录。对于外部的连接器（例如 Apache Kafka）和对应的 format（例如 JSON），
Flink 提供 **即用型 JAR 包**，你可以根据 Flink 的发布版本从 Maven 仓库下载这些 JAR 文件。

Flink 提供的 SQL JAR 文件可以在这里查看 [connection to external systems page]({{< ref "docs/connectors/table/overview" >}})。

你可以查看 [configuration]({{< ref "docs/dev/configuration/connector" >}}) 章节了解如何配置连接器和 format 的依赖。

{{< top >}}

使用
----------------------------

SQL 客户端允许用户通过交互式命令或者 `-f` sql 文件提交作业。在这两种模式中，SQL 客户端支持解析和执行 Flink 支持的所有 SQL 语句。

### 交互式命令行

SQL 客户端通过交互式命令行读取用户的输入，并执行以分号（`;`）结尾的SQL语句。SQL 语句执行成功后，客户端会打印成功信息。如果执行出错，客户端
也会输出错误信息。默认情况下错误信息只包含错误原因，如果需要打印整个异常栈，可以通过命令行 `SET 'sql-client.verbose' = 'true';` 将 `sql-client.verbose` 设置为 true。

### 执行 SQL 文件

SQL 客户端支持 `-f` 参数提交 SQL 脚本，执行脚本中的每条 SQL 语句并打印执行消息。如果其中某条 SQL 执行失败，客户端会直接退出，剩余的 SQL 语句不会被执行。

SQL 脚本文件示例如下。

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
内容包括：

- 定义读取 CSV 文件的 temporal 源头表，
- 设置参数，例如作业名字，
- 设置 savepoint 路径，
- 提交一个从指定路径加载 savepoint 的 sql 作业。

<span class="label label-danger">注意</span> 和交互式模式相比，文件模式的 SQL 客户端遇到错误时会直接退出。

### 执行 SQL 语句集合

SQL 客户端将每条 INSERT INTO 语句作为独立的 Flink 作业，但这不是最佳方式，因为有些中间结果可以复用。SQL 客户端支持 STATEMENT SET 语法
执行 SQL 语句集合，这和 Table API 的 StatementSet 等价。`STATEMENT SET` 可以包含多条 `INSERT INTO` 语句，这些语句被优化成
一个 Flink 作业。这可以复用很多中间结果，相比多个作业独立运行，可以显著提升性能。

#### Syntax
```sql
EXECUTE STATEMENT SET 
BEGIN
  -- one or more INSERT INTO statements
  { INSERT INTO|OVERWRITE <select_statement>; }+
END;
```

<span class="label label-danger">注意</span> `STATEMENT SET` 语法中的多个 SQL 语句必须以分号（;）分隔。旧的语法 `BEGIN STATEMENT SET; ... END;` 已经不推荐使用，可能会在后续版本中删除。

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

### 同步/异步执行 DML 语句

SQL 客户端默认异步执行 DML 语句，客户端提交 DML 语句到 Flink 集群后，不会等待作业结束。这意味着 SQL 客户端可以同时提交多个作业，这对长时间运行的流式作业很有用。

SQL 客户端确保 SQL 语句被成功的提交到集群，并在控制台输出 Flink 作业信息。

```sql
Flink SQL> INSERT INTO MyTableSink SELECT * FROM MyTableSource;
[INFO] Table update statement has been successfully submitted to the cluster:
Cluster ID: StandaloneClusterId
Job ID: 6f922fe5cba87406ff23ae4a7bb79044
```

<span class="label label-danger">注意</span> SQL 客户端在作业提交后不会追踪作业的运行状态，作业提交后退出
客户端不会影响作业。Flink 的 `重启策略` 处理 fault-tolerance。可以参阅 [monitor the detached query status]({{< ref "docs/dev/table/sqlClient" >}}#monitoring-job-status)
或 [stop the detached query]({{< ref "docs/dev/table/sqlClient" >}}#terminating-a-job).

对于批式作业用户，通常上一条 DML 语句执行完成后再执行下一条。为了同步执行 SQL 语句，你可以在 SQL 客户端中将 `table.dml-sync` 设置为 `true`。

```sql
Flink SQL> SET 'table.dml-sync' = 'true';
[INFO] Session property has been set.

Flink SQL> INSERT INTO MyTableSink SELECT * FROM MyTableSource;
[INFO] Submitting SQL update statement to the cluster...
[INFO] Execute statement in sync mode. Please wait for the execution finish...
[INFO] Complete execution of the SQL update statement.
```

<span class="label label-danger">注意</span> 使用 `CTRL-C` 可以直接结束作业。

### 从 savepoint 启动 SQL 作业

Flink 支持从指定的 savepoint 启动作业。SQL 客户端支持使用 `SET` 命令设置 savepoint 的路径。

```sql
Flink SQL> SET 'execution.savepoint.path' = '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab';
[INFO] Session property has been set.

-- all the following DML statements will be restroed from the specified savepoint path
Flink SQL> INSERT INTO ...
```

Flink 后续所有的 DML 语句都会从这个指定的 savepoint 路径恢复状态并执行。

由于指定 savepoint 路径会影响后续所有提交的作业，你可以通过 `RESET` 命令取消这个配置项，例如关闭从 savepoint 恢复。

```sql
Flink SQL> RESET execution.savepoint.path;
[INFO] Session property has been reset.
```

更多创建和管理 savepoint 的细节请参阅 [Job Lifecycle Management]({{< ref "docs/deployment/cli" >}}#job-lifecycle-management).

### 自定义作业名称

SQL 客户端支持通过 `SET` 命令设置查询和 DML 语句的作业名。

```sql
Flink SQL> SET 'pipeline.name' = 'kafka-to-hive';
[INFO] Session property has been set.

-- all the following DML statements will use the specified job name.
Flink SQL> INSERT INTO ...
```

设置作业名会影响所有后续提交的作业，你也可以使用 `RESET` 命令取消设置，例如使用默认的作业名。

```sql
Flink SQL> RESET pipeline.name;
[INFO] Session property has been reset.
```

如果没有设置 `pipeline.name`，SQL 客户端会为提交的作业生成默认名字，例如为 `INSERT INTO` 语句生成 `insert-into_<sink_table_name>`。

### 监控作业状态

SQL 客户端支持通过 `SHOW JOBS` 语句获取集群中的作业状态列表。

```sql
Flink SQL> SHOW JOBS;
+----------------------------------+---------------+----------+-------------------------+
|                           job id |      job name |   status |              start time |
+----------------------------------+---------------+----------+-------------------------+
| 228d70913eab60dda85c5e7f78b5782c | kafka-to-hive |  RUNNING | 2023-02-11T05:03:51.523 |
+----------------------------------+---------------+----------+-------------------------+
```

### 结束作业

SQL 客户端支持通过 `STOP JOB` 结束作业，可以指定或者不指定 savepoint。

```sql
Flink SQL> STOP JOB '228d70913eab60dda85c5e7f78b5782c' WITH SAVEPOINT;
+-----------------------------------------+
|                          savepoint path |
+-----------------------------------------+
| file:/tmp/savepoint-3addd4-0b224d9311e6 |
+-----------------------------------------+
```

可以通过 [state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir) 在集群配置或者 session 配置（优先级更高）中指定 savepoint 路径。

更多停止作业，可以参阅 [Job Statements]({{< ref "docs/dev/table/sql/job" >}}#stop-job).

{{< top >}}
