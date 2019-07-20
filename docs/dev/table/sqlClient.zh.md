---
title: "SQL 客户端"
nav-parent_id: tableapi
nav-pos: 100
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


Flink的Table & SQL API可以处理SQL语言编写的查询，但这些查询需要嵌入用Java或Scala编写的程序中。 而且，这些程序需要在提交到集群之前使用构建工具打包。 这或多或少地限制了Flink只能为Java/Scala程序员而使用。

*SQL Client*旨在提供一种简单的方法来编写，调试和提交表程序到Flink集群，而无需单独编写Java或Scala代码。 *SQL Client CLI*允许在命令行上提交分布式应用程序来获得可视化的实时结果。

<a href="{{ site.baseurl }}/fig/sql_client_demo.gif"><img class="offset" src="{{ site.baseurl }}/fig/sql_client_demo.gif" alt="在集群上运行表程序的Flink SQL Client CLI的动画演示" width="80%" /></a>

<span class="label label-danger">注意</span> SQL客户端处于早期开发阶段。 尽管它还没有生产就绪，它也可以是一个非常有用的工具，用于开发原型和测试Flink SQL。 在未来，社区计划通过提供基于REST的功能来扩展其功能 [SQL Client Gateway](sqlClient.html#limitations--future).


* This will be replaced by the TOC
{:toc}

入门
---------------

本节介绍如何从命令行设置和运行第一个Flink SQL程序。

SQL客户端捆绑在常规Flink分发中，因此可以开箱即用。 它只需要一个可以执行表程序的正在运行的Flink集群，其中。 有关设置Flink群集的详细信息，请参阅
 [集群与发布]({{ site.baseurl }}/ops/deployment/cluster_setup.html) 部分。 如果您只想尝试SQL客户端，还可以使用以下命令启动具有一个工作线程的本地集群:

{% highlight bash %}
./bin/start-cluster.sh
{% endhighlight %}

### 启动SQL客户端CLI

SQL客户端脚本也位于Flink的二进制目录中。 [将来](sqlClient_zh.html#局限与未来)，用户可以通过启动嵌入式独立进程或连接到远程SQL客户端网关来启动SQL Client CLI。 目前只支持`embedded`模式。 您可以通过调用以下命令启动CLI：

{% highlight bash %}
./bin/sql-client.sh embedded
{% endhighlight %}

默认情况下，SQL客户端将从位于`./conf/sql-client-defaults.yaml`的配置文件中读取其配置 . 有关配置文件的更多信息请参阅 [配置部分](sqlClient.html#environment-files)。

### 运行SQL查询

CLI启动后，您可以使用`HELP`命令列出所有可用的SQL语句。 要验证您的设置和集群连接，您可以输入第一个SQL查询并按`Enter`键执行它:

{% highlight sql %}
SELECT 'Hello World';
{% endhighlight %}

此查询不需要表源，并生成单行结果。 CLI将从集群中检索结果并将其可视化。 您可以按`Q`键关闭结果视图。

CLI支持**两种模式**，用于维护和可视化结果。

**表模式**在内存中生成结果，并以常规的分页表格形式显示它们。 可以通过在CLI中执行以下命令来启用它:

{% highlight text %}
SET execution.result-mode=table;
{% endhighlight %}

**更改日志模式**不会产生结果并可视化由[连续查询](streaming/dynamic_tables.html#continuous-queries)生成的结果流，其中包含插入(`+`)和撤消(`-`)。

{% highlight text %}
SET execution.result-mode=changelog;
{% endhighlight %}

您可以使用以下查询来查看两种结果模式:

{% highlight sql %}
SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name;
{% endhighlight %}

此查询执行有界字数统计示例。

在*更改日志模式*中，可视化的更改日志应类似于：

{% highlight text %}
+ Bob, 1
+ Alice, 1
+ Greg, 1
- Bob, 1
+ Bob, 2
{% endhighlight %}

在*表模式*中，可视化结果表不断更新，直到表程序结束为止：

{% highlight text %}
Bob, 2
Alice, 1
Greg, 1
{% endhighlight %}

在SQL查询的原型设计期间，两种结果模式都很有用。 在这两种模式下，结果都存储在SQL客户端的Java堆内存中。 为了使CLI界面保持响应，更改日志模式仅显示最新的 1000 个更改。 表模式允许导航更大的结果，这些结果仅受可用主存储器和配置的[最大行数](sqlClient.html#configuration) (`max-table-result-rows`)的限制。

<span class="label label-danger">注意</span> 只能使用`table`结果模式在批处理环境中进行检索查询。

定义查询后，可以将其作为长时间运行的不同的Flink作业提交给集群。 为此，需要使用[INSERT INTO语句](sqlClient.html#configuration) 指定存储结果的目标系统。 [配置部分](sqlClient.html#configuration) 解释了如何声明用于读取数据的表源，如何声明用于写入数据的表接收器以及如何配置其他表程序属性。

{% top %}

配置
-------------

可以使用以下可选CLI命令启动SQL Client。 它们将在随后的段落中详细讨论。

{% highlight text %}
./bin/sql-client.sh embedded --help

"嵌入式"模式从本地计算机提交Flink作业。

  Syntax: embedded [OPTIONS]
  "embedded" mode options:
     -d,--defaults <environment file>      每个新会话会初始化环境属性。会话属性可能会覆盖属性。
     -e,--environment <environment file>   将环境属性导入会话中。 它可能会覆盖默认环境属性。
     -h,--help                             显示包含所有选项说明的帮助消息。
     -j,--jar <JAR file>                   要导入会话的JAR文件。 该文件可能包含可以多次使用的用户自定义的执行语句（如函数，表源或接收器）。
     -l,--library <JAR directory>          每个新会话初始化的JAR文件目录。 该文件可能包含可以多次使用的用户自定义的执行语句（如函数，表源或接收器）。
     -s,--session <session identifier>     会话'default'的标识符是默认标识符。
{% endhighlight %}

{% top %}

### 环境文件

SQL查询需要一个执行它的配置环境。 所谓的*环境文件*定义了可用的目录，表源和接收器，用户定义的函数以及执行和部署所需的其他属性。

每个环境文件都是常规[YAML文件](http://yaml.org/)。 下面给出了这种文件的一个例子。

{% highlight yaml %}
# 在此定义表，例如源，接收器，视图或临时表。

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

# 在此处定义用户自定义的函数。

functions:
  - name: myUDF
    from: class
    class: foo.bar.AggregateUDF
    constructor:
      - 7.6
      - false

# 允许更改表程序行为的执行属性。

execution:
  type: streaming                   # 必需：执行模式'批量'或'流'
  result-mode: table                # 必需：'table'或'changelog'
  max-table-result-rows: 1000000    # 可选：最大维护行数
                                    #   'table'模式(默认为1000000，小1表示无限制)
  time-characteristic: event-time   # 可选：'处理时间'或'事件时间'(默认)
  parallelism: 1                    # 可选：Flink的并行性(默认为1)
  periodic-watermarks-interval: 200 # 可选：定期水印的间隔(默认为200毫秒)
  max-parallelism: 16               # 可选：Flink的最大并行度(默认为128)
  min-idle-state-retention: 0       # 可选：表程序的最小空闲状态时间
  max-idle-state-retention: 0       # 可选：表程序的最大空闲状态时间
  restart-strategy:                 # 可选：重启策略
    type: fallback                  #   默认情况下，"回退"到全局重启策略
  current-catalog: catalog_1        # 可选：会话当前目录的名称（默认为"default_catalog"）
  current-database: mydb1           # 可选：当前目录的当前数据库的名称（默认值是当前目录的默认数据库名称）

# 部署属性允许描述向其提交表程序的集群。

deployment:
  response-timeout: 5000

# 目录

catalogs:
   - name: catalog_1
     type: hive
     property-version: 1
     hive-conf-dir: ...
   - name: catalog_2
     type: hive
     property-version: 1
     default-database: mydb2        # 可选：此目录的默认数据库的名称
     hive-conf-dir: ...             # 可选：Hive conf目录的路径。 (默认值由HiveConf创建)
     hive-version: 1.2.1            # 可选：Hive版本(默认为2.3.4)
{% endhighlight %}

配置:

- 定义一个具有从CSV文件读取数据的表源`MyTableSource`环境,
- 定义一个视图`MyCustomView`，它使用SQL查询声明一个虚拟表,
- 定义了一个用户自定义的函数`myUDF`，它可以使用类名和两个构造函数参数进行实例化,
- 为在流式环境中执行的查询指定1的并行度,
- 指定事件时间特征，和
- 在`table`结果模式下运行查询。
- (类型：hive)。 第一个`HiveCatalog`的Hive版本默认为`2.3.4`，第二个的Hive版本指定为`1.2.1`。
- 在开始时使用`catalog_1`作为环境的当前目录，并使用`mydb1`作为目录的当前数据库。

根据用例，配置可以拆分为多个文件。 因此，可以为一般用途（*默认环境文件*使用`--defaults`）以及每个会话（*会话环境文件*使用`--environment`）创建环境文件。 每个CLI会话在会使用默认属性进行初始化之后，在加上会话属性。 例如，defaults环境文件可以指定在每个会话中可用于查询的所有表源，而会话环境文件仅声明特定的状态保留时间和并行性。 启动CLI应用程序时，可以传递默认文件和会话环境文件。 如果未指定缺省环境文件，则SQL Client将在Flink的配置目录中搜索`./conf/sql-client-defaults.yaml`。

<span class="label label-danger">注意</span> 已在CLI会话中设置的属性（例如，使用`SET`命令）具有最高优先级：

{% highlight text %}
CLI命令>会话环境文件>默认环境文件
{% endhighlight %}

#### 重启策略

重新启动策略控制在发生故障时如何重新启动Flink作业。 与Flink集群的[全局重启策略]({{ site.baseurl }}/dev/restart_strategies.html)类似，可以在环境文件中声明更细粒度的重新启动配置。

支持以下策略：

{% highlight yaml %}
execution:
  # 回到flink-conf.yaml中定义的全局策略
  restart-strategy:
    type: fallback

  # 作业直接失败，不尝试重启
  restart-strategy:
    type: none

  # 尝试给定次数重新启动作业
  restart-strategy:
    type: fixed-delay
    attempts: 3      # 在将作业声明为失败之前重试（默认值：Integer.MAX_VALUE）
    delay: 10000     # 重试之间的延迟时间（默认值：10秒）

  # 只要未超过每个时间间隔的最大故障数，就会尝试尝试
  restart-strategy:
    type: failure-rate
    max-failures-per-interval: 1   # 间隔重试直到失败（默认值：1）
    failure-rate-interval: 60000   # 测量故障率的间隔（ms）
    delay: 10000                   # 重试之间的延迟时间（默认值：10秒）
{% endhighlight %}

{% top %}

### 依赖

SQL客户端不需要使用Maven或SBT设置Java项目。 相反，您可以将依赖项作为提交到集群的常规JAR文件传递。 您可以单独指定每个JAR文件（使用`--jar`）或定义整个库目录（使用`--library`）。 对于连接外部系统（如Apache Kafka）和相应数据格式（如JSON）的连接器，Flink提供**即用型JAR包**。 可以从Maven中央存储库为每个版本下载这些JAR文件。

可以在[连接到外部系统页面](connect.html)上找到提供的SQL JAR的完整列表以及有关如何使用它们的文档。

以下示例显示了一个环境文件，该文件定义从Apache Kafka读取JSON数据的表源。

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
        - key: zookeeper.connect
          value: localhost:2181
        - key: bootstrap.servers
          value: localhost:9092
        - key: group.id
          value: testGroup
    format:
      property-version: 1
      type: json
      schema: "ROW<rideId LONG, lon FLOAT, lat FLOAT, rideTime TIMESTAMP>"
    schema:
      - name: rideId
        type: LONG
      - name: lon
        type: FLOAT
      - name: lat
        type: FLOAT
      - name: rowTime
        type: TIMESTAMP
        rowtime:
          timestamps:
            type: "from-field"
            from: "rideTime"
          watermarks:
            type: "periodic-bounded"
            delay: "60000"
      - name: procTime
        type: TIMESTAMP
        proctime: true
{% endhighlight %}

`TaxiRide`表的结果模式包含JSON模式的大多数字段。 此外，它添加了rowtime属性`rowTime`和processing-time属性`procTime`。

`connector`和`format`为了将来能向后兼容，都允许定义属性版本（当前版本为`1`）。

{% top %}

### 用户定义的函数

SQL Client允许用户创建用于SQL查询的自定义用户函数。 目前，这些函数仅限于在Java/Scala类中以编程方式定义。

为了提供用户定义的函数，您需要首先实现和编译扩展`ScalarFunction`，`AggregateFunction`或`TableFunction`的函数类（参见[自定义函数]({{ site.baseurl }}/dev/table/udfs.html))。 然后可以将一个或多个函数打包到SQL客户端的依赖项JAR中。

在调用之前，必须在环境文件中声明所有函数。 对于`functions`列表中的每个项目，必须指定

- 注册函数的`name`,
- 使用`from`的函数的源代码（现在限制为`class`）,
- `class`表示函数的完全限定类名，以及用于实例化的`constructor`参数的可选列表。

{% highlight yaml %}
functions:
  - name: ...               # 必需：函数的名称
    from: class             # 必需：函数的来源（现在只能是"class"）
    class: ...              # 必需：函数的完全限定类名
    constructor:            # 最佳：函数类的构造函数参数
      - ...                 # 最佳：具有隐式类型的文字参数
      - class: ...          # 最佳：参数的完整类名
        constructor:        # 最佳：参数类的构造函数参数
          - type: ...       # 最佳：文字参数的类型
            value: ...      # 最佳：文字参数的值
{% endhighlight %}

确保指定参数的顺序和类型能够严格匹配上函数类的一个构造函数。

#### 构造函数参数

根据用户定义的函数，可能需要在SQL语句中使用它之前参数化实现。

如前面的示例所示，在声明用户定义的函数时，可以使用以下三种方法之一使用构造函数参数来配置类：

**具有隐式类型的文字值：** SQL客户端将根据文字值本身自动派生类型。 目前，此处仅支持`BOOLEAN`，`INT`，`DOUBLE`和`VARCHAR`的值。
如果自动派生不能按预期工作（例如，您需要VARCHAR`false`），请改用显式类型。

{% highlight yaml %}
- true         # -> BOOLEAN (区分大小写)
- 42           # -> INT
- 1234.222     # -> DOUBLE
- foo          # -> VARCHAR
{% endhighlight %}

**具有显式类型的文字值：**显式声明带有类型安全的`type`和`value`属性的参数。

{% highlight yaml %}
- type: DECIMAL
  value: 11111111111111111
{% endhighlight %}

下表说明了受支持的Java参数类型和相应的SQL类型字符串。

| Java type               |  SQL type         |
| :---------------------- | :---------------- |
| `java.math.BigDecimal`  | `DECIMAL`         |
| `java.lang.Boolean`     | `BOOLEAN`         |
| `java.lang.Byte`        | `TINYINT`         |
| `java.lang.Double`      | `DOUBLE`          |
| `java.lang.Float`       | `REAL`, `FLOAT`   |
| `java.lang.Integer`     | `INTEGER`, `INT`  |
| `java.lang.Long`        | `BIGINT`          |
| `java.lang.Short`       | `SMALLINT`        |
| `java.lang.String`      | `VARCHAR`         |

More types (e.g., `TIMESTAMP` or `ARRAY`), primitive types, and `null` are not supported yet.
如下类型还不支持（例如，`TIMESTAMP`或`ARRAY`），原始类型和`null`。

**（嵌套）类实例：**除了文字值，您还可以通过指定`class`和`constructor`属性为构造函数参数创建（嵌套）类实例。
可以递归地执行此过程，直到所有构造函数参数都用文字值表示。

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

目录可以定义为一组yaml属性，并在启动SQL Client时自动注册到环境中。

用户可以在`execution`部分指定他们想要将哪个catalog用作SQL CLI中的当前catalog，以及他们希望将哪个目录数据库用作当前数据库。

{% highlight yaml %}
execution:
   ...
   current-catalog: catalog_1
   current-database: mydb1

catalogs:
   - name: catalog_1
     type: hive
     property-version: 1
     default-database: mydb2
     hive-version: 1.2.1
     hive-conf-dir: <path of Hive conf directory>
   - name: catalog_2
     type: hive
     property-version: 1
     hive-conf-dir: <path of Hive conf directory>
{% endhighlight %}

目前Flink支持两种类型的目录 - `FlinkInMemoryCatalog`和`HiveCatalog`。

有关目录的更多信息，请参阅[目录]({{ site.baseurl }}/dev/table/catalog.html)。

分离的SQL查询
--------------------

为了定义端到端SQL管道，SQL的 `INSERT INTO` 语句可用于向Flink集群提交长时间运行的分离查询。 这些查询将结果生成到外部系统而不是SQL客户端。 这允许处理更高的并行性和更大量的数据。 CLI本身在提交后对分离的查询没有任何控制权。

{% highlight sql %}
INSERT INTO MyTableSink SELECT * FROM MyTableSource
{% endhighlight %}

表sink `MyTableSink` 必须在环境文件中声明。 有关支持的外部系统及其配置的详细信息，请参阅[连接页面](connect.html)。 Apache Kafka表接收器的示例如下所示。

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
        - key: zookeeper.connect
          value: localhost:2181
        - key: bootstrap.servers
          value: localhost:9092
        - key: group.id
          value: testGroup
    format:
      property-version: 1
      type: json
      derive-schema: true
    schema:
      - name: rideId
        type: LONG
      - name: lon
        type: FLOAT
      - name: lat
        type: FLOAT
      - name: rideTime
        type: TIMESTAMP
{% endhighlight %}

SQL客户端确保将语句成功提交到群集。 提交查询后，CLI将显示有关Flink作业的信息。

{% highlight text %}
[INFO] Table update statement has been successfully submitted to the cluster:
Cluster ID: StandaloneClusterId
Job ID: 6f922fe5cba87406ff23ae4a7bb79044
Web interface: http://localhost:8081
{% endhighlight %}

<span class="label label-danger">注意</span> 提交后，SQL Client不会跟踪正在运行的Flink作业的状态。 提交后可以关闭CLI进程，而不会影响分离的查询。 Flink的[重新启动策略]({{ site.baseurl }}/dev/restart_strategies.html)负责容错。 可以使用Flink的Web界面，命令行或REST API取消查询。

{% top %}

SQL视图
---------

视图允许从SQL查询定义虚拟表。 视图定义将立即进行解析和验证。 但是，在提交通用 `INSERT INTO` or `SELECT` 语句期间访问视图时会发生实际执行。

视图可以在[环境文件](sqlClient.html#environment-files) 中定义，也可以在CLI会话中定义。

以下示例显示如何在文件中定义多个视图。 视图按照在环境文件中定义的顺序进行注册。 诸如_view A之类的参考链取决于视图B和视图C_是否受支持。

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

与表源和接收器类似，会话环境文件中定义的视图具有最高优先级。

也可以使用 `CREATE VIEW` 语句在CLI会话中创建视图：

{% highlight text %}
CREATE VIEW MyNewView AS SELECT MyField2 FROM MyTableSource;
{% endhighlight %}

也可以使用`DROP VIEW`语句再次删除在CLI会话中创建的视图：

{% highlight text %}
DROP VIEW MyNewView;
{% endhighlight %}

<span class="label label-danger">注意</span> CLI中视图的定义仅限于上面提到的语法。 在将来的版本中，将支持为视图定义schema或转义表名中的空格。

{% top %}

历史表
---------------

[历史表](./streaming/temporal_tables.html) 允许在更改历史记录表上的（参数化）视图，该视图返回特定时间点的表的内容。 这对于在特定时间戳下加入具有另一个具有内容的表特别有用。 可以在[加入历史表](./streaming/joins.html#join-with-a-temporal-table) 页面中找到更多信息。

以下示例显示如何定义历史表 `SourceTemporalTable`：

{% highlight yaml %}
tables:

  # 定义包含历史表更新的表源（或视图）
  - name: HistorySource
    type: source-table
    update-mode: append
    connector: # ...
    format: # ...
    schema:
      - name: integerField
        type: INT
      - name: stringField
        type: VARCHAR
      - name: rowtimeField
        type: TIMESTAMP
        rowtime:
          timestamps:
            type: from-field
            from: rowtimeField
          watermarks:
            type: from-source

  # 使用时间属性和主键在更改历史记录表上定义历史表
  - name: SourceTemporalTable
    type: temporal-table
    history-table: HistorySource
    primary-key: integerField
    time-attribute: rowtimeField  # 也可以是一个proctime字段
{% endhighlight %}

如示例所示，表源，视图和历史表的定义可以相互混合。 它们按照在环境文件中定义的顺序进行注册。 例如，历史表可以引用可以依赖于另一个视图或table source。

{% top %}

局限与未来
--------------------

当前的SQL客户端实现处于非常早期的开发阶段，并且作为更大的Flink改进提案 24 ([FLIP-24](https://cwiki.apache.org/confluence/display/FLINK/FLIP-24+-+SQL+Client))
的一部分，将来可能会发生变化。 欢迎加入讨论并公开有关您认为有用的错误和功能的问题。

{% top %}
