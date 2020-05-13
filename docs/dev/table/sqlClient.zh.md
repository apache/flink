---
title: "SQL 客户端"
nav-parent_id: tableapi
nav-pos: 90
is_beta: true
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

Flink 的 Table & SQL API 可以处理 SQL 语言编写的查询语句，但是这些查询需要嵌入用 Java 或 Scala 编写的表程序中。此外，这些程序在提交到集群前需要用构建工具打包。这或多或少限制了 Java/Scala 程序员对 Flink 的使用。

*SQL 客户端* 的目的是提供一种简单的方式来编写、调试和提交表程序到 Flink 集群上，而无需写一行 Java 或 Scala 代码。*SQL 客户端命令行界面（CLI）* 能够在命令行中检索和可视化分布式应用中实时产生的结果。

<a href="{{ site.baseurl }}/fig/sql_client_demo.gif"><img class="offset" src="{{ site.baseurl }}/fig/sql_client_demo.gif" alt="Animated demo of the Flink SQL Client CLI running table programs on a cluster" width="80%" /></a>

<span class="label label-danger">注意</span> SQL 客户端正处于早期开发阶段。虽然还没准备好用于生产，但是它对于原型设计和玩转 Flink SQL 还是很实用的工具。将来，社区计划通过提供基于 REST 的 [SQL 客户端网关（Gateway）](sqlClient.html#limitations--future)的来扩展它的功能。

* This will be replaced by the TOC
{:toc}

入门
---------------

本节介绍如何在命令行里启动（setup）和运行你的第一个 Flink SQL 程序。

SQL 客户端捆绑在常规 Flink 发行版中，因此可以直接运行。它仅需要一个正在运行的 Flink 集群就可以在其中执行表程序。有关设置 Flink 群集的更多信息，请参见[集群和部署]({{ site.baseurl }}/zh/ops/deployment/cluster_setup.html)部分。如果仅想试用 SQL 客户端，也可以使用以下命令启动本地集群：

{% highlight bash %}
./bin/start-cluster.sh
{% endhighlight %}

### 启动 SQL 客户端命令行界面

SQL Client 脚本也位于 Flink 的 bin 目录中。[将来](sqlClient.html#limitations--future)，用户可以通过启动嵌入式 standalone 进程或通过连接到远程 SQL 客户端网关来启动 SQL 客户端命令行界面。目前仅支持 `embedded` 模式。可以通过以下方式启动 CLI：

{% highlight bash %}
./bin/sql-client.sh embedded
{% endhighlight %}

默认情况下，SQL 客户端将从 `./conf/sql-client-defaults.yaml` 中读取配置。有关环境配置文件结构的更多信息，请参见[配置部分](sqlClient.html#environment-files)。

### 执行 SQL 查询

命令行界面启动后，你可以使用 `HELP` 命令列出所有可用的 SQL 语句。输入第一条 SQL 查询语句并按 `Enter` 键执行，可以验证你的设置及集群连接是否正确：

{% highlight sql %}
SELECT 'Hello World';
{% endhighlight %}

该查询不需要 table source，并且只产生一行结果。CLI 将从集群中检索结果并将其可视化。按 `Q` 键退出结果视图。

CLI 为维护和可视化结果提供**两种模式**。

**表格模式**（table mode）在内存中实体化结果，并将结果用规则的分页表格可视化展示出来。执行如下命令启用：

{% highlight text %}
SET execution.result-mode=table;
{% endhighlight %}

**变更日志模式**（changelog mode）不会实体化和可视化结果，而是由插入（`+`）和撤销（`-`）组成的持续查询产生结果流。

{% highlight text %}
SET execution.result-mode=changelog;
{% endhighlight %}

你可以用如下查询来查看两种结果模式的运行情况：

{% highlight sql %}
SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name;
{% endhighlight %}

此查询执行一个有限字数示例：

*变更日志模式* 下，看到的结果应该类似于：

{% highlight text %}
+ Bob, 1
+ Alice, 1
+ Greg, 1
- Bob, 1
+ Bob, 2
{% endhighlight %}

*表格模式* 下，可视化结果表将不断更新，直到表程序以如下内容结束：

{% highlight text %}
Bob, 2
Alice, 1
Greg, 1
{% endhighlight %}

这两种结果模式在 SQL 查询的原型设计过程中都非常有用。这两种模式结果都存储在 SQL 客户端 的 Java 堆内存中。为了保持 CLI 界面及时响应，变更日志模式仅显示最近的 1000 个更改。表格模式支持浏览更大的结果，这些结果仅受可用主内存和配置的[最大行数](sqlClient.html#configuration)（`max-table-result-rows`）的限制。

<span class="label label-danger">注意</span> 在批处理环境下执行的查询只能用表格模式进行检索。

定义查询语句后，可以将其作为长时间运行的独立 Flink 作业提交给集群。为此，其目标系统需要使用 [INSERT INTO 语句](sqlClient.html#detached-sql-queries)指定存储结果。[配置部分](sqlClient.html#configuration)解释如何声明读取数据的 table source，写入数据的 sink 以及配置其他表程序属性的方法。

{% top %}

<a name="configuration"></a>

配置
-------------

SQL 客户端启动时可以添加 CLI 选项，具体如下。

{% highlight text %}
./bin/sql-client.sh embedded --help

Mode "embedded" submits Flink jobs from the local machine.

  Syntax: embedded [OPTIONS]
  "embedded" mode options:
     -d,--defaults <environment file>      The environment properties with which
                                           every new session is initialized.
                                           Properties might be overwritten by
                                           session properties.
     -e,--environment <environment file>   The environment properties to be
                                           imported into the session. It might
                                           overwrite default environment
                                           properties.
     -h,--help                             Show the help message with
                                           descriptions of all options.
     -hist,--history <History file path>   The file which you want to save the
                                           command history into. If not
                                           specified, we will auto-generate one
                                           under your user's home directory.
     -j,--jar <JAR file>                   A JAR file to be imported into the
                                           session. The file might contain
                                           user-defined classes needed for the
                                           execution of statements such as
                                           functions, table sources, or sinks.
                                           Can be used multiple times.
     -l,--library <JAR directory>          A JAR file directory with which every
                                           new session is initialized. The files
                                           might contain user-defined classes
                                           needed for the execution of
                                           statements such as functions, table
                                           sources, or sinks. Can be used
                                           multiple times.
     -pyarch,--pyArchives <arg>            Add python archive files for job. The
                                           archive files will be extracted to
                                           the working directory of python UDF
                                           worker. Currently only zip-format is
                                           supported. For each archive file, a
                                           target directory be specified. If the
                                           target directory name is specified,
                                           the archive file will be extracted to
                                           a name can directory with the
                                           specified name. Otherwise, the
                                           archive file will be extracted to a
                                           directory with the same name of the
                                           archive file. The files uploaded via
                                           this option are accessible via
                                           relative path. '#' could be used as
                                           the separator of the archive file
                                           path and the target directory name.
                                           Comma (',') could be used as the
                                           separator to specify multiple archive
                                           files. This option can be used to
                                           upload the virtual environment, the
                                           data files used in Python UDF (e.g.:
                                           --pyArchives
                                           file:///tmp/py37.zip,file:///tmp/data
                                           .zip#data --pyExecutable
                                           py37.zip/py37/bin/python). The data
                                           files could be accessed in Python
                                           UDF, e.g.: f = open('data/data.txt',
                                           'r').
     -pyexec,--pyExecutable <arg>          Specify the path of the python
                                           interpreter used to execute the
                                           python UDF worker (e.g.:
                                           --pyExecutable
                                           /usr/local/bin/python3). The python
                                           UDF worker depends on Python 3.5+,
                                           Apache Beam (version == 2.19.0), Pip
                                           (version >= 7.1.0) and SetupTools
                                           (version >= 37.0.0). Please ensure
                                           that the specified environment meets
                                           the above requirements.
     -pyfs,--pyFiles <pythonFiles>         Attach custom python files for job.
                                           These files will be added to the
                                           PYTHONPATH of both the local client
                                           and the remote python UDF worker. The
                                           standard python resource file
                                           suffixes such as .py/.egg/.zip or
                                           directory are all supported. Comma
                                           (',') could be used as the separator
                                           to specify multiple files (e.g.:
                                           --pyFiles
                                           file:///tmp/myresource.zip,hdfs:///$n
                                           amenode_address/myresource2.zip).
     -pyreq,--pyRequirements <arg>         Specify a requirements.txt file which
                                           defines the third-party dependencies.
                                           These dependencies will be installed
                                           and added to the PYTHONPATH of the
                                           python UDF worker. A directory which
                                           contains the installation packages of
                                           these dependencies could be specified
                                           optionally. Use '#' as the separator
                                           if the optional parameter exists
                                           (e.g.: --pyRequirements
                                           file:///tmp/requirements.txt#file:///
                                           tmp/cached_dir).
     -s,--session <session identifier>     The identifier for a session.
                                           'default' is the default identifier.
     -u,--update <SQL update statement>    Experimental (for testing only!):
                                           Instructs the SQL Client to
                                           immediately execute the given update
                                           statement after starting up. The
                                           process is shut down after the
                                           statement has been submitted to the
                                           cluster and returns an appropriate
                                           return code. Currently, this feature
                                           is only supported for INSERT INTO
                                           statements that declare the target
                                           sink table.
{% endhighlight %}

{% top %}

<a name="environment-files"></a>

### 环境配置文件

SQL 查询执行前需要配置相关环境变量。*环境配置文件* 定义了 catalog、table sources、table sinks、用户自定义函数和其他执行或部署所需属性。

每个环境配置文件是常规的 [YAML 文件](http://yaml.org/)，例子如下。

{% highlight yaml %}
# 定义表，如 source、sink、视图或临时表。

tables:
  - name: MyTableSource
    type: source-table
    update-mode: append
    connector:
      type: filesystem
      path: "/path/to/something.csv"
    format:
      type: csv
      fields:
        - name: MyField1
          type: INT
        - name: MyField2
          type: VARCHAR
      line-delimiter: "\n"
      comment-prefix: "#"
    schema:
      - name: MyField1
        type: INT
      - name: MyField2
        type: VARCHAR
  - name: MyCustomView
    type: view
    query: "SELECT MyField2 FROM MyTableSource"

# 定义用户自定义函数

functions:
  - name: myUDF
    from: class
    class: foo.bar.AggregateUDF
    constructor:
      - 7.6
      - false

# 定义 catalogs

catalogs:
   - name: catalog_1
     type: hive
     property-version: 1
     hive-conf-dir: ...
   - name: catalog_2
     type: hive
     property-version: 1
     default-database: mydb2
     hive-conf-dir: ...

# 改变表程序基本的执行行为属性。

execution:
  planner: blink                    # 可选： 'blink' （默认）或 'old'
  type: streaming                   # 必选：执行模式为 'batch' 或 'streaming'
  result-mode: table                # 必选：'table' 或 'changelog'
  max-table-result-rows: 1000000    # 可选：'table' 模式下可维护的最大行数（默认为 1000000，小于 1 则表示无限制）
  time-characteristic: event-time   # 可选： 'processing-time' 或 'event-time' （默认）
  parallelism: 1                    # 可选：Flink 的并行数量（默认为 1）
  periodic-watermarks-interval: 200 # 可选：周期性 watermarks 的间隔时间（默认 200 ms）
  max-parallelism: 16               # 可选：Flink 的最大并行数量（默认 128）
  min-idle-state-retention: 0       # 可选：表程序的最小空闲状态时间
  max-idle-state-retention: 0       # 可选：表程序的最大空闲状态时间
  current-catalog: catalog_1        # 可选：当前会话 catalog 的名称（默认为 'default_catalog'）
  current-database: mydb1           # 可选：当前 catalog 的当前数据库名称
                                    #   （默认为当前 catalog 的默认数据库）
  restart-strategy:                 # 可选：重启策略（restart-strategy）
    type: fallback                  #   默认情况下“回退”到全局重启策略

# 用于调整和调优表程序的配置选项。

# 在专用的”配置”页面上可以找到完整的选项列表及其默认值。
configuration:
  table.optimizer.join-reorder-enabled: true
  table.exec.spill-compression.enabled: true
  table.exec.spill-compression.block-size: 128kb

# 描述表程序提交集群的属性。

deployment:
  response-timeout: 5000
{% endhighlight %}

上述配置：

- 定义一个从 CSV 文件中读取的 table source `MyTableSource` 所需的环境，
- 定义了一个视图 `MyCustomView` ，该视图是用 SQL 查询声明的虚拟表，
- 定义了一个用户自定义函数 `myUDF`，该函数可以使用类名和两个构造函数参数进行实例化，
- 连接到两个 Hive catalogs 并用 `catalog_1` 来作为当前目录，用 `mydb1` 来作为该目录的当前数据库，
- streaming 模式下用 blink planner 来运行时间特征为 event-time 和并行度为 1 的语句，
- 在 `table` 结果模式下运行试探性的（exploratory）的查询，
- 并通过配置选项对联结（join）重排序和溢出进行一些计划调整。

根据使用情况，配置可以被拆分为多个文件。因此，一般情况下（用 `--defaults` 指定*默认环境配置文件*）以及基于每个会话（用 `--environment` 指定*会话环境配置文件*）来创建环境配置文件。每个 CLI 会话均会被属于 session 属性的默认属性初始化。例如，默认环境配置文件可以指定在每个会话中都可用于查询的所有 table source，而会话环境配置文件仅声明特定的状态保留时间和并行性。启动 CLI 应用程序时，默认环境配置文件和会话环境配置文件都可以被指定。如果未指定默认环境配置文件，则 SQL 客户端将在 Flink 的配置目录中搜索 `./conf/sql-client-defaults.yaml`。

<span class="label label-danger">注意</span> 在 CLI 会话中设置的属性（如 `SET` 命令）优先级最高：

{% highlight text %}
CLI commands > session environment file > defaults environment file
{% endhighlight %}

#### 重启策略（Restart Strategies）

重启策略控制 Flink 作业失败时的重启方式。与 Flink 集群的[全局重启策略]({{ site.baseurl }}/zh/dev/restart_strategies.html)相似，更细精度的重启配置可以在环境配置文件中声明。

Flink 支持以下策略：

{% highlight yaml %}
execution:
  # 退回到 flink-conf.yaml 中定义的全局策略
  restart-strategy:
    type: fallback

  # 作业直接失败并且不尝试重启
  restart-strategy:
    type: none

  # 最多重启作业的给定次数
  restart-strategy:
    type: fixed-delay
    attempts: 3      # 作业被宣告失败前的重试次数（默认：Integer.MAX_VALUE）
    delay: 10000     # 重试之间的间隔时间，以毫秒为单位（默认：10 秒）

  # 只要不超过每个时间间隔的最大故障数就继续尝试
  restart-strategy:
    type: failure-rate
    max-failures-per-interval: 1   # 每个间隔重试的最大次数（默认：1）
    failure-rate-interval: 60000   # 监测失败率的间隔时间，以毫秒为单位
    delay: 10000                   # 重试之间的间隔时间，以毫秒为单位（默认：10 秒）
{% endhighlight %}

{% top %}

### 依赖

SQL 客户端不要求用 Maven 或者 SBT 设置 Java 项目。相反，你可以以常规的 JAR 包给集群提交依赖项。你也可以分别（用 `--jar`）指定每一个 JAR 包或者（用 `--library`）定义整个 library 依赖库。为连接扩展系统（如 Apache Kafka）和相应的数据格式（如 JSON），Flink提供了**开箱即用型 JAR 捆绑包（ready-to-use JAR bundles）**。这些 JAR 包各个发行版都可以从 Maven 中央库中下载到。

提供的 SQL JARs 和使用文档的完整清单可以在[连接扩展系统页面](connect.html)中找到。

如下例子展示了从 Apache Kafka 中读取 JSON 文件并作为 table source 的环境配置文件。

{% highlight yaml %}
tables:
  - name: TaxiRides
    type: source-table
    update-mode: append
    connector:
      property-version: 1
      type: kafka
      version: "0.11"
      topic: TaxiRides
      startup-mode: earliest-offset
      properties:
        bootstrap.servers: localhost:9092
        group.id: testGroup
    format:
      property-version: 1
      type: json
      schema: "ROW<rideId LONG, lon FLOAT, lat FLOAT, rideTime TIMESTAMP>"
    schema:
      - name: rideId
        data-type: BIGINT
      - name: lon
        data-type: FLOAT
      - name: lat
        data-type: FLOAT
      - name: rowTime
        data-type: TIMESTAMP(3)
        rowtime:
          timestamps:
            type: "from-field"
            from: "rideTime"
          watermarks:
            type: "periodic-bounded"
            delay: "60000"
      - name: procTime
        data-type: TIMESTAMP(3)
        proctime: true
{% endhighlight %}

`TaxiRide` 表的结果格式与绝大多数的 JSON 格式相似。此外，它还添加了 rowtime 属性 `rowTime` 和 processing-time 属性 `procTime`。

`connector` 和 `format` 都允许定义属性版本（当前版本为 `1` ）以便将来向后兼容。

{% top %}

### 自定义函数（User-defined Functions）
SQL 客户端允许用户创建用户自定义的函数来进行 SQL 查询。当前，这些自定义函数仅限于 Java/Scala 编写的类以及 Python 文件。

为提供 Java/Scala 的自定义函数，你首先需要实现和编译函数类，该函数继承自 `ScalarFunction`、 `AggregateFunction` 或 `TableFunction`（见[自定义函数]({{ site.baseurl }}/zh/dev/table/functions/udfs.html)）。一个或多个函数可以打包到 SQL 客户端的 JAR 依赖中。

为提供 Python 的自定义函数，你需要编写 Python 函数并且用装饰器 `pyflink.table.udf.udf` 或 `pyflink.table.udf.udtf` 来装饰（见 [Python UDFs]({{ site.baseurl }}/zh/dev/table/python/python_udfs.html))）。Python 文件中可以放置一个或多个函数。其Python 文件和相关依赖需要通过在环境配置文件中或命令行选项（见 [命令行用法]({{ site.baseurl }}/zh/ops/cli.html#usage)）配置中特别指定（见 [Python 配置]({{ site.baseurl }}/zh/dev/table/python/python_config.html)）。

所有函数在被调用之前，必须在环境配置文件中提前声明。`functions` 列表中每个函数类都必须指定

- 用来注册函数的 `name`，
- 函数的来源 `from`（目前仅限于 `class`（Java/Scala UDF）或 `python`（Python UDF）），

Java/Scala UDF 必须指定：
- 声明了全限定名的函数类 `class` 以及用于实例化的 `constructor` 参数的可选列表。

Python UDF 必须指定：

- 声明全程名称的 `fully-qualified-name`，即函数的 “[module name].[object name]” 

{% highlight yaml %}
functions:
  - name: java_udf               # required: name of the function
    from: class                  # required: source of the function
    class: ...                   # required: fully qualified class name of the function
    constructor:                 # optional: constructor parameters of the function class
      - ...                      # optional: a literal parameter with implicit type
      - class: ...               # optional: full class name of the parameter
        constructor:             # optional: constructor parameters of the parameter's class
          - type: ...            # optional: type of the literal parameter
            value: ...           # optional: value of the literal parameter
  - name: python_udf             # required: name of the function
    from: python                 # required: source of the function 
    fully-qualified-name: ...    # required: fully qualified class name of the function      
{% endhighlight %}

对于 Java/Scala UDF，要确保函数类指定的构造参数顺序和类型都要严格匹配。

#### 构造函数参数

根据用户自定义函数可知，在用到 SQL 语句中之前，有必要将构造参数匹配对应的类型。

如上述示例所示，当声明一个用户自定义函数时，可以使用构造参数来配置相应的类，有以下三种方式：

**隐式类型的文本值：**SQL 客户端将自动根据文本推导对应的类型。目前，只支持 `BOOLEAN`、`INT`、 `DOUBLE` 和 `VARCHAR` 。

如果自动推导的类型与期望不符（例如，你需要 VARCHAR 类型的 `false`），可以改用显式类型。

{% highlight yaml %}
- true         # -> BOOLEAN (case sensitive)
- 42           # -> INT
- 1234.222     # -> DOUBLE
- foo          # -> VARCHAR
{% endhighlight %}

**显式类型的文本值：**为保证类型安全，需明确声明 `type` 和 `value` 属性的参数。

{% highlight yaml %}
- type: DECIMAL
  value: 11111111111111111
{% endhighlight %}

下表列出支持的 Java 参数类型和与之相对应的 SQL 类型。

| Java 类型              | SQL 类型         |
| :--------------------- | :--------------- |
| `java.math.BigDecimal` | `DECIMAL`        |
| `java.lang.Boolean`    | `BOOLEAN`        |
| `java.lang.Byte`       | `TINYINT`        |
| `java.lang.Double`     | `DOUBLE`         |
| `java.lang.Float`      | `REAL`, `FLOAT`  |
| `java.lang.Integer`    | `INTEGER`, `INT` |
| `java.lang.Long`       | `BIGINT`         |
| `java.lang.Short`      | `SMALLINT`       |
| `java.lang.String`     | `VARCHAR`        |

其他类型 （例如 `TIMESTAMP` 和 `ARRAY`）、原始类型和 `null` 目前还不支持。

**（嵌套）类实例：**除了文本值外，还可以通过指定构造参数的 `class` 和 `constructor` 属性来创建（嵌套）类实例。这个过程可以递归执行，直到最后的构造参数是用文本值来描述的。

{% highlight yaml %}
- class: foo.bar.paramClass
  constructor:
    - StarryName
    - class: java.lang.Integer
      constructor:
        - class: java.lang.String
          constructor:
            - type: VARCHAR
              value: 3
{% endhighlight %}

{% top %}

Catalogs
--------

Catalogs 可以由 YAML 属性集合定义，并且在 SQL 客户端启动之前自动注册到运行环境中。

用户可以指定在 SQL CLI 中哪些 catalog 要被作为当前的 catalog，以及哪个数据库的 catalog 可以用于当前数据库。

{% highlight yaml %}
catalogs:
   - name: catalog_1
     type: hive
     property-version: 1
     default-database: mydb2
     hive-conf-dir: <path of Hive conf directory>
   - name: catalog_2
     type: hive
     property-version: 1
     hive-conf-dir: <path of Hive conf directory>

execution:
   ...
   current-catalog: catalog_1
   current-database: mydb1
{% endhighlight %}

更多关于 catalog 的内容，参考 [Catalogs]({{ site.baseurl }}/zh/dev/table/catalogs.html)。

<a name="detached-sql-queries"></a>

分离的 SQL 查询
--------------------

为定义端到端的 SQL 管道，SQL 的 `INSERT INTO` 语句可以向 Flink 集群提交长时间运行的分离查询。查询产生的结果输出到除 SQL 客户端外的扩展系统中。这样可以应对更高的并发和更多的数据。CLI 自身在提交后不对分离查询做任何控制。

{% highlight sql %}
INSERT INTO MyTableSink SELECT * FROM MyTableSource
{% endhighlight %}

sink `MyTableSink` 必须在环境配置文件中声明。查看更多关于 Flink 支持的外部系统及其配置信息，参见 [connection page](connect.html)。如下展示 Apache Kafka 的 sink 示例。

{% highlight yaml %}
tables:
  - name: MyTableSink
    type: sink-table
    update-mode: append
    connector:
      property-version: 1
      type: kafka
      version: "0.11"
      topic: OutputTopic
      properties:
        bootstrap.servers: localhost:9092
        group.id: testGroup
    format:
      property-version: 1
      type: json
      derive-schema: true
    schema:
      - name: rideId
        data-type: BIGINT
      - name: lon
        data-type: FLOAT
      - name: lat
        data-type: FLOAT
      - name: rideTime
        data-type: TIMESTAMP(3)
{% endhighlight %}

SQL 客户端要确保语句成功提交到集群上。一旦提交查询，CLI 将展示关于 Flink 作业的相关信息。

{% highlight text %}
[INFO] Table update statement has been successfully submitted to the cluster:
Cluster ID: StandaloneClusterId
Job ID: 6f922fe5cba87406ff23ae4a7bb79044
Web interface: http://localhost:8081
{% endhighlight %}

<span class="label label-danger">注意</span> 提交后，SQL 客户端不追踪正在运行的 Flink 作业状态。提交后可以关闭 CLI 进程，并且不会影响分离的查询。Flink 的[重启策略]({{ site.baseurl }}/zh/dev/restart_strategies.html)负责容错。取消查询可以用 Flink 的 web 接口、命令行或 REST API 。

{% top %}

SQL 视图
---------

视图是一张虚拟表，允许通过 SQL 查询来定义。视图的定义会被立即解析与验证。然而，提交常规 `INSERT INTO` 或 `SELECT` 语句后不会立即执行，在访问视图时才会真正执行。

视图可以用[环境配置文件](sqlClient.html#environment-files)或者 CLI 会话来定义。

下例展示如何在一个文件里定义多张视图。视图注册的顺序和定义它们的环境配置文件一致。支持诸如 _视图 A 依赖视图 B ，视图 B 依赖视图 C_ 的引用链。

{% highlight yaml %}
tables:
  - name: MyTableSource
    # ...
  - name: MyRestrictedView
    type: view
    query: "SELECT MyField2 FROM MyTableSource"
  - name: MyComplexView
    type: view
    query: >
      SELECT MyField2 + 42, CAST(MyField1 AS VARCHAR)
      FROM MyTableSource
      WHERE MyField2 > 200
{% endhighlight %}

相较于 table soruce 和 sink，会话环境配置文件中定义的视图具有最高优先级。

视图还可以在 CLI 会话中用 `CREATE VIEW` 语句来创建：

{% highlight text %}
CREATE VIEW MyNewView AS SELECT MyField2 FROM MyTableSource;
{% endhighlight %}

视图能在 CLI 会话中创建，也能用 `DROP VIEW` 语句删除：

{% highlight text %}
DROP VIEW MyNewView;
{% endhighlight %}

<span class="label label-danger">注意</span> CLI 中视图的定义仅限于上述语法。将来版本会支持定义视图结构以及在表名中加入转义的空格。

{% top %}

临时表（Temporal Table）
---------------

[临时表](./streaming/temporal_tables.html)是在变化的历史记录表上的（参数化）视图，该视图在某个特定时间点返回表的内容。这对于在特定的时间戳将一张表的内容联结另一张表是非常有用的。更多信息见[联结临时表](./streaming/joins.html#join-with-a-temporal-table)页面。

下例展示如何定义一张临时表 `SourceTemporalTable`：

{% highlight yaml %}
tables:

  # 定义包含对临时表的更新的 table source （或视图）

  - name: HistorySource
    type: source-table
    update-mode: append
    connector: # ...
    format: # ...
    schema:
      - name: integerField
        data-type: INT
      - name: stringField
        data-type: STRING
      - name: rowtimeField
        data-type: TIMESTAMP(3)
        rowtime:
          timestamps:
            type: from-field
            from: rowtimeField
          watermarks:
            type: from-source

  # 在具有时间属性和主键的变化历史记录表上定义临时表
  - name: SourceTemporalTable
    type: temporal-table
    history-table: HistorySource
    primary-key: integerField
    time-attribute: rowtimeField  # could also be a proctime field
{% endhighlight %}

如例子中所示，table source，视图和临时表的定义可以相互混合。它们按照在环境配置文件中定义的顺序进行注册。例如，临时表可以引用一个视图，该视图依赖于另一个视图或 table source。

{% top %}

<a name="limitations--future"></a>

局限与未来
--------------------

当前的 SQL 客户端仍处于非常早期的开发阶段，作为更大的 Flink 改进提案 24（[FLIP-24](https://cwiki.apache.org/confluence/display/FLINK/FLIP-24+-+SQL+Client)）的一部分，将来可能会发生变化。如果你发现了 bug 可以随时创建 issue，或者如果（如邮件列表、Pull requests中）发现有用的特性，欢迎积极参与讨论。

{% top %}
