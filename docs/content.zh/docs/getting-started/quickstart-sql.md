---
title: "Flink SQL 教程"
weight: 2
type: docs
aliases:
  - /zh/dev/table/sql/gettingStarted.html
  - /zh/docs/getting-started/quickstart-sql/
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

# Flink SQL 教程

Flink SQL 使得使用标准 SQL 开发流应用程序变的简单。如果你曾经在工作中使用过兼容 ANSI-SQL 2011 的数据库或类似的 SQL 系统，那么就很容易学习 Flink。

## 你将学到什么

在本教程中，你将：

- 启动 Flink SQL 客户端
- 运行交互式 SQL 查询
- 从外部数据创建 source 表
- 编写处理流数据的连续查询
- 将结果输出到 sink 表

{{< hint info >}}
**无需编码！** SQL 客户端提供了一个交互式环境，你可以输入 SQL 查询并立即看到结果。
{{< /hint >}}

## 先决条件

- SQL 基础知识
- 运行中的 Flink 集群（按照[第一步]({{< ref "docs/getting-started/local_installation" >}})进行设置）

## 启动 SQL 客户端

[SQL 客户端]({{< ref "docs/sql/interfaces/sql-client" >}})是一个交互式的客户端，用于向 Flink 提交 SQL 查询并将结果可视化。

如果你在第一步中使用了 **Docker**：

```bash
$ docker compose run sql-client
```

如果你在第一步中使用了**本地安装**：

```bash
$ ./bin/sql-client.sh
```

要退出 SQL 客户端，输入 `exit;` 并按回车键。 

### Hello World

SQL 客户端（查询编辑器）启动并运行后，就可以开始编写查询了。
使用以下简单查询打印出 'Hello World'：
 
```sql
SELECT 'Hello World';
```

运行 `HELP` 命令会列出所有支持的 SQL 语句。运行一个 `SHOW` 命令，来查看 Flink [内置函数]({{< ref "docs/sql/functions/built-in-functions" >}})的完整列表。

```sql
SHOW FUNCTIONS;
```

这些函数为用户在开发 SQL 查询时提供了一个功能强大的工具箱。
例如，`CURRENT_TIMESTAMP` 将在执行时打印出机器的当前系统时间。

```sql
SELECT CURRENT_TIMESTAMP;
```

---------------

{{< top >}}

## Source 表

与所有 SQL 引擎一样，Flink 查询操作是在表上进行。与传统数据库不同，Flink 不在本地管理静态数据；相反，它的查询在外部表上连续运行。

Flink 数据处理流水线开始于 source 表。source 表产生在查询执行期间可以被操作的行；它们是查询时 `FROM` 子句中引用的表。这些表可能是 Kafka 的 topics，数据库，文件系统，或者任何其它 Flink 知道如何消费的系统。

可以通过 SQL 客户端或使用环境配置文件来定义表。SQL 客户端支持类似于传统 SQL 的 [SQL DDL 命令]({{< ref "docs/sql/reference/overview" >}})。标准 SQL DDL 用于[创建]({{< ref "docs/sql/reference/ddl/create" >}})，[修改]({{< ref "docs/sql/reference/ddl/alter" >}})，[删除]({{< ref "docs/sql/reference/ddl/drop" >}})表。

Flink 支持不同的[连接器]({{< ref "docs/connectors/table/overview" >}})和[格式]({{< ref "docs/connectors/table/formats/overview" >}})相结合以定义表。下面是一个使用 [DataGen 连接器]({{< ref "docs/connectors/table/datagen" >}})定义 source 表的示例，该连接器可以自动生成示例数据 - 非常适合在不需要外部数据源的情况下尝试查询。

```sql
CREATE TABLE employee_information (
    emp_id INT,
    name STRING,
    dept_id INT
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.emp_id.kind' = 'sequence',
    'fields.emp_id.start' = '1',
    'fields.emp_id.end' = '1000000',
    'fields.name.length' = '8',
    'fields.dept_id.min' = '1',
    'fields.dept_id.max' = '5'
);
```

这将创建一个每秒生成 10 行数据的表，包含顺序递增的员工 ID、随机的 8 字符名称以及 1 到 5 之间的部门 ID。 

可以从该表中定义一个连续查询，当新行可用时读取并立即输出它们的结果。
例如，你可以过滤出只在部门 `1` 中工作的员工。

```sql
SELECT * from employee_information WHERE dept_id = 1;
``` 

---------------

{{< top >}}

## 连续查询

虽然最初设计时没有考虑流语义，但 SQL 是用于构建连续数据流水线的强大工具。Flink SQL 与传统数据库查询的不同之处在于，Flink SQL 持续消费到达的行并对其结果进行更新。

一个[连续查询]({{< ref "docs/concepts/sql-table-concepts/dynamic_tables" >}}#continuous-queries)永远不会终止，并会产生一个动态表作为结果。[动态表]({{< ref "docs/concepts/sql-table-concepts/dynamic_tables" >}}#continuous-queries)是 Flink 中 Table API 和 SQL 对流数据支持的核心概念。

连续流上的聚合需要在查询执行期间不断地存储聚合的结果。例如，假设你需要从传入的数据流中计算每个部门的员工人数。查询需要维护每个部门最新的计算总数，以便在处理新行时及时输出结果。

 ```sql
SELECT 
	dept_id,
	COUNT(*) as emp_count 
FROM employee_information 
GROUP BY dept_id;
 ``` 

这样的查询被认为是 _有状态的_。Flink 的高级容错机制将维持内部状态和一致性，因此即使遇到硬件故障，查询也始终返回正确结果。

## Sink 表

当运行此查询时，SQL 客户端实时但是以只读方式提供输出。存储结果，作为报表或仪表板的数据来源，需要写到另一个表。这可以使用 `INSERT INTO` 语句来实现。本节中引用的表称为 sink 表。`INSERT INTO` 语句将作为一个独立查询被提交到 Flink 集群中。

首先，创建 sink 表。本示例使用 `print` 连接器，它会将结果写入 TaskManager 的日志：

```sql
CREATE TABLE department_counts (
    dept_id INT,
    emp_count BIGINT
) WITH (
    'connector' = 'print'
);
```

现在将聚合结果插入到该表中：

```sql
INSERT INTO department_counts
SELECT
    dept_id,
    COUNT(*) as emp_count
FROM employee_information
GROUP BY dept_id;
```

提交后，它将作为后台作业运行，并持续将结果写入 sink 表。你可以在 TaskManager 日志中查看输出：

```bash
$ docker compose logs -f taskmanager
```

在生产环境中，你通常会使用 [JDBC]({{< ref "docs/connectors/table/jdbc" >}})、[Kafka]({{< ref "docs/connectors/table/kafka" >}}) 或[文件系统]({{< ref "docs/connectors/table/filesystem" >}})等连接器写入外部系统。

---------------

{{< top >}}

## 下一步

现在你已经体验了 Flink SQL，以下是一些继续学习的路径：

### 深入了解 Flink SQL

- [SQL 参考]({{< ref "docs/sql/reference/overview" >}})：完整的 SQL 语法和支持的操作
- [SQL 客户端]({{< ref "docs/sql/interfaces/sql-client" >}})：高级 SQL 客户端功能和配置
- [内置函数]({{< ref "docs/sql/functions/built-in-functions" >}})：所有可用的 SQL 查询函数
- [连接器]({{< ref "docs/connectors/table/overview" >}})：连接 Kafka、数据库、文件系统等

### 理解流式概念

- [动态表]({{< ref "docs/concepts/sql-table-concepts/dynamic_tables" >}})：Flink SQL 如何处理流数据
- [时间属性]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}})：使用事件时间和处理时间

### 尝试其他教程

- [Table API 教程]({{< ref "docs/getting-started/table_api" >}})：使用 Java 或 Scala 构建完整的流处理管道（需要 Docker）
- [Flink 运维练习场]({{< ref "docs/getting-started/flink-operations-playground" >}})：学习操作 Flink 集群（需要 Docker）

## 获取帮助

如果你遇到困难，请查看[社区支持资源](https://flink.apache.org/zh/community.html)。
Apache Flink 的[用户邮件列表](https://flink.apache.org/zh/community.html#mailing-lists)是任何 Apache 项目中最活跃的项目之一，也是快速获得帮助的好方法。

---------------

{{< top >}}
