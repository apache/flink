---
title: "入门"
weight: 2
type: docs
aliases:
  - /zh/dev/table/sql/gettingStarted.html
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

# 入门

Flink SQL 使得使用标准 SQL 开发流应用程序变的简单。如果你曾经在工作中使用过兼容 ANSI-SQL 2011 的数据库或类似的 SQL 系统，那么就很容易学习 Flink。本教程将帮助你在 Flink SQL 开发环境下快速入门。

### 先决条件

你只需要具备 SQL 的基础知识即可，不需要其他编程经验。

### 安装

安装 Flink 有多种方式。对于实验而言，最常见的选择是下载二进制包并在本地运行。你可以按照[本地模式安装]({{< ref "docs/try-flink/local_installation" >}})中的步骤为本教程的剩余部分设置环境。

完成所有设置后，在安装文件夹中使用以下命令启动本地集群：

```bash
./bin/start-cluster.sh
```
 
启动完成后，就可以在本地访问 Flink WebUI [localhost:8081](localhost:8081)，通过它，你可以监控不同的作业。

### SQL 客户端

[SQL 客户端]({{< ref "docs/dev/table/sqlClient" >}})是一个交互式的客户端，用于向 Flink 提交 SQL 查询并将结果可视化。
在安装文件夹中运行 `sql-client` 脚本来启动 SQL 客户端。

 ```bash
./bin/sql-client.sh
 ``` 

### Hello World

SQL 客户端（我们的查询编辑器）启动并运行后，就可以开始编写查询了。
让我们使用以下简单查询打印出 'Hello World'：
 
```sql
SELECT 'Hello World';
```

运行 `HELP` 命令会列出所有支持的 SQL 语句。让我们运行一个 `SHOW` 命令，来查看 Flink [内置函数]({{< ref "docs/dev/table/functions/systemFunctions" >}})的完整列表。

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

可以通过 SQL 客户端或使用环境配置文件来定义表。SQL 客户端支持类似于传统 SQL 的 [SQL DDL 命令]({{< ref "docs/dev/table/sql/overview" >}})。标准 SQL DDL 用于[创建]({{< ref "docs/dev/table/sql/create" >}})，[修改]({{< ref "docs/dev/table/sql/alter" >}})，[删除]({{< ref "docs/dev/table/sql/drop" >}})表。

Flink 支持不同的[连接器]({{< ref "docs/connectors/table/overview" >}})和[格式]({{< ref "docs/connectors/table/formats/overview" >}})相结合以定义表。下面是一个示例，定义一个以 [CSV 文件]({{< ref "docs/connectors/table/formats/csv" >}})作为存储格式的 source 表，其中 `emp_id`，`name`，`dept_id` 作为 `CREATE` 表语句中的列。

```sql
CREATE TABLE employee_information (
    emp_id INT,
    name VARCHAR,
    dept_id INT
) WITH ( 
    'connector' = 'filesystem',
    'path' = '/path/to/something.csv',
    'format' = 'csv'
);
``` 

可以从该表中定义一个连续查询，当新行可用时读取并立即输出它们的结果。
例如，我们可以过滤出只在部门 `1` 中工作的员工。

```sql
SELECT * from employee_information WHERE DeptId = 1;
``` 

---------------

{{< top >}}

## 连续查询

虽然最初设计时没有考虑流语义，但 SQL 是用于构建连续数据流水线的强大工具。Flink SQL 与传统数据库查询的不同之处在于，Flink SQL 持续消费到达的行并对其结果进行更新。

一个[连续查询]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#continuous-queries)永远不会终止，并会产生一个动态表作为结果。[动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#continuous-queries)是 Flink 中 Table API 和 SQL 对流数据支持的核心概念。

连续流上的聚合需要在查询执行期间不断地存储聚合的结果。例如，假设你需要从传入的数据流中计算每个部门的员工人数。查询需要维护每个部门最新的计算总数，以便在处理新行时及时输出结果。

 ```sql
SELECT 
	dept_id,
	COUNT(*) as emp_count 
FROM employee_information 
GROUP BY dep_id;
 ``` 

这样的查询被认为是 _有状态的_。Flink 的高级容错机制将维持内部状态和一致性，因此即使遇到硬件故障，查询也始终返回正确结果。

## Sink 表

当运行此查询时，SQL 客户端实时但是以只读方式提供输出。存储结果，作为报表或仪表板的数据来源，需要写到另一个表。这可以使用 `INSERT INTO` 语句来实现。本节中引用的表称为 sink 表。`INSERT INTO` 语句将作为一个独立查询被提交到 Flink 集群中。

 ```sql
INSERT INTO department_counts
 SELECT 
	dept_id,
	COUNT(*) as emp_count 
FROM employee_information;
 ``` 
 
提交后，它将运行并将结果直接存储到 sink 表中，而不是将结果加载到系统内存中。

---------------

{{< top >}}

## 寻求帮助！

如果你有疑惑，可以查阅[社区支持资源](https://flink.apache.org/zh/community.html)。
特别是，Apache Flink 的[用户邮件列表](https://flink.apache.org/zh/community.html#mailing-lists)一直被评为是任何 Apache 项目中最活跃的项目之一，也是快速获得帮助的好方法。

## 了解更多资源

* [SQL]({{< ref "docs/dev/table/sql/overview" >}})：SQL 支持的操作和语法。
* [SQL 客户端]({{< ref "docs/dev/table/sqlClient" >}})：不用编写代码就可以尝试 Flink SQL，可以直接提交 SQL 任务到集群上。
* [概念与通用 API]({{< ref "docs/dev/table/common" >}})：Table API 和 SQL 公共概念以及 API。
* [流式概念]({{< ref "docs/dev/table/concepts/overview" >}})：Table API 和 SQL 中流式相关的文档，比如配置时间属性和如何处理更新结果。
* [内置函数]({{< ref "docs/dev/table/functions/systemFunctions" >}})：Table API 和 SQL 中的内置函数。
* [连接外部系统]({{< ref "docs/connectors/table/overview" >}})：读写外部系统的连接器和格式。

---------------

{{< top >}}
