---
title: 'Table API 教程'
nav-title: 'Table API 教程'
weight: 3
type: docs
aliases:
  - /zh/try-flink/table_api.html
  - /zh/getting-started/walkthroughs/table_api.html
  - /zh/dev/python/table_api_tutorial.html
  - /zh/tutorials/python_table_api.html
  - /zh/getting-started/walkthroughs/python_table_api.html
  - /zh/try-flink/python_table_api.html
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

# Table API 教程

Apache Flink 提供了 Table API 作为批流统一的关系型 API。也就是说，在无界的实时流数据或者有界的批数据集上进行查询具有相同的语义，得到的结果一致。Flink 的 Table API 可以简化数据分析、构建数据流水线以及 ETL 应用的定义。

## 你将构建什么

在本教程中，你将构建一个消费报表，按账户和小时聚合交易金额：

```
交易数据（生成的数据） → Flink（Table API 聚合） → 控制台（结果）
```

你将学习：

- 设置 Table API 流处理环境
- 使用 Table API 创建表
- 使用 Table API 操作编写连续聚合
- 实现用户自定义函数 (UDF)
- 使用基于时间的窗口进行聚合
- 在批处理模式下测试流处理应用

## 准备条件

本教程假设你对 Java 或 Python 有一定了解，当然如果你使用的是其他编程语言，也可以继续学习。同时也假设你了解基本的关系型概念，例如 `SELECT`、`GROUP BY` 等语句。

## 困难求助

如果遇到问题，可以参考 [社区支持资源](https://flink.apache.org/community.html)。
Flink 的 [用户邮件列表](https://flink.apache.org/community.html#mailing-lists) 是 Apache 项目中最活跃的一个，这也是快速寻求帮助的重要途径。

## 如何跟着教程练习

本教程依赖如下运行环境：

{{< tabs "prerequisites" >}}
{{< tab "Java" >}}
* Java 11、17 或 21
* Maven
{{< /tab >}}
{{< tab "Python" >}}
* Java 11、17 或 21
* Python 3.9、3.10、3.11 或 3.12
{{< /tab >}}
{{< /tabs >}}

{{< tabs "setup" >}}
{{< tab "Java" >}}

Flink 提供了 Maven Archetype 来快速创建包含所有必要依赖的项目骨架，这样你只需要专注于填写业务逻辑。

{{< unstable >}}
{{< hint warning >}}
**注意：** Maven archetype 仅适用于 Apache Flink 发行版。

由于你目前正在浏览快照版的文档，因此下文中引用的版本可能已经不存在了，请先通过左侧菜单下方的版本选择器切换到发行版文档再查看。
{{< /hint >}}
{{< /unstable >}}

```bash
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-table-java \
    -DarchetypeVersion={{< version >}} \
    -DgroupId=spendreport \
    -DartifactId=spendreport \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
```

你可以根据需要修改 `groupId`、`artifactId` 和 `package`。使用上述参数，Maven 将创建一个名为 `spendreport` 的目录，其中包含完成本教程所需的所有依赖项目。

将项目导入编辑器后，你可以找到文件 `SpendReport.java`，其中包含以下代码，可以直接在 IDE 中运行。

{{< hint info >}}
**在 IDE 中运行：** 如果遇到 `java.lang.NoClassDefFoundError` 异常，这可能是因为类路径中没有包含所有必需的 Flink 依赖项。

* **IntelliJ IDEA：** 转到 Run > Edit Configurations > Modify options > 选择 "include dependencies with 'Provided' scope"。
{{< /hint >}}

{{< /tab >}}
{{< tab "Python" >}}

使用 Python Table API 需要安装 PyFlink，它已经被发布到 [PyPi](https://pypi.org/project/apache-flink/)，你可以通过如下方式安装：

```bash
$ python -m pip install apache-flink
```

{{< hint info >}}
**提示：** 建议在 [虚拟环境](https://docs.python.org/zh-cn/3/library/venv.html) 中安装 PyFlink，以保持项目依赖隔离。
{{< /hint >}}

安装 PyFlink 后，创建一个名为 `spend_report.py` 的新文件来编写 Table API 程序。

{{< /tab >}}
{{< /tabs >}}

## 完整代码

以下是消费报表程序的完整代码：

{{< tabs "complete-program" >}}
{{< tab "Java" >}}

```java
public class SpendReport {

    public static void main(String[] args) throws Exception {
        // 创建流处理的 Table 环境
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 创建源表
        // DataGen 连接器生成无限的交易数据流
        tEnv.createTemporaryTable("transactions",
            TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                    .column("accountId", DataTypes.BIGINT())
                    .column("amount", DataTypes.BIGINT())
                    .column("transactionTime", DataTypes.TIMESTAMP(3))
                    .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
                    .build())
                .option("rows-per-second", "100")
                .option("fields.accountId.min", "1")
                .option("fields.accountId.max", "5")
                .option("fields.amount.min", "1")
                .option("fields.amount.max", "1000")
                .build());

        // 从源表读取数据
        Table transactions = tEnv.from("transactions");

        // 应用业务逻辑
        Table result = report(transactions);

        // 将结果打印到控制台
        result.execute().print();
    }

    public static Table report(Table transactions) {
        return transactions
            .select(
                $("accountId"),
                $("transactionTime").floor(TimeIntervalUnit.HOUR).as("logTs"),
                $("amount"))
            .groupBy($("accountId"), $("logTs"))
            .select(
                $("accountId"),
                $("logTs"),
                $("amount").sum().as("amount"));
    }
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table import TableEnvironment, EnvironmentSettings, TableDescriptor, Schema, DataTypes
from pyflink.table.expression import TimeIntervalUnit
from pyflink.table.expressions import col

def main():
    # 创建流处理的 Table 环境
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # 将所有结果写入一个文件以便查看
    t_env.get_config().set("parallelism.default", "1")

    # 创建源表
    # DataGen 连接器生成无限的交易数据流
    t_env.create_temporary_table(
        "transactions",
        TableDescriptor.for_connector("datagen")
            .schema(Schema.new_builder()
                .column("accountId", DataTypes.BIGINT())
                .column("amount", DataTypes.BIGINT())
                .column("transactionTime", DataTypes.TIMESTAMP(3))
                .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
                .build())
            .option("rows-per-second", "100")
            .option("fields.accountId.min", "1")
            .option("fields.accountId.max", "5")
            .option("fields.amount.min", "1")
            .option("fields.amount.max", "1000")
            .build())

    # 从源表读取数据
    transactions = t_env.from_path("transactions")

    # 应用业务逻辑
    result = report(transactions)

    # 将结果打印到控制台
    result.execute().print()

def report(transactions):
    return transactions \
        .select(
            col("accountId"),
            col("transactionTime").floor(TimeIntervalUnit.HOUR).alias("logTs"),
            col("amount")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs"),
            col("amount").sum.alias("amount"))

if __name__ == '__main__':
    main()
```

{{< /tab >}}
{{< /tabs >}}

## 代码分析

### 执行环境

前几行代码创建了 `TableEnvironment`（表环境）。创建表环境时，你可以设置作业属性，定义应用的批流模式，以及创建数据源。本示例先创建一个标准的表环境，并选择流式执行器。

{{< tabs "environment" >}}
{{< tab "Java" >}}

```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

{{< /tab >}}
{{< tab "Python" >}}

```python
settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)
```

{{< /tab >}}
{{< /tabs >}}

### 创建表

接下来，创建表来表示交易数据。DataGen 连接器生成无限的随机交易数据流。

{{< tabs "create-table" >}}
{{< tab "Java" >}}

使用 `TableDescriptor`，可以以编程方式定义表：

```java
tEnv.createTemporaryTable("transactions",
    TableDescriptor.forConnector("datagen")
        .schema(Schema.newBuilder()
            .column("accountId", DataTypes.BIGINT())
            .column("amount", DataTypes.BIGINT())
            .column("transactionTime", DataTypes.TIMESTAMP(3))
            .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
            .build())
        .option("rows-per-second", "100")
        .option("fields.accountId.min", "1")
        .option("fields.accountId.max", "5")
        .option("fields.amount.min", "1")
        .option("fields.amount.max", "1000")
        .build());
```

{{< /tab >}}
{{< tab "Python" >}}

使用 `TableDescriptor`，可以以编程方式定义表：

```python
t_env.create_temporary_table(
    "transactions",
    TableDescriptor.for_connector("datagen")
        .schema(Schema.new_builder()
            .column("accountId", DataTypes.BIGINT())
            .column("amount", DataTypes.BIGINT())
            .column("transactionTime", DataTypes.TIMESTAMP(3))
            .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
            .build())
        .option("rows-per-second", "100")
        .option("fields.accountId.min", "1")
        .option("fields.accountId.max", "5")
        .option("fields.amount.min", "1")
        .option("fields.amount.max", "1000")
        .build())
```

{{< /tab >}}
{{< /tabs >}}

交易表生成信用卡交易数据，包含：
- `accountId`：1 到 5 之间的账户 ID
- `amount`：1 到 1000 之间的交易金额
- `transactionTime`：带有水印的时间戳，用于处理延迟数据

### 查询数据

配置好环境并注册好表后，你就可以开始开发你的第一个应用了。通过 `TableEnvironment` 实例，你可以使用函数 `from` 从输入表读取数据并应用 Table API 操作。函数 `report` 用于实现具体的业务逻辑。

{{< tabs "query" >}}
{{< tab "Java" >}}

```java
Table transactions = tEnv.from("transactions");
Table result = report(transactions);
result.execute().print();
```

{{< /tab >}}
{{< tab "Python" >}}

```python
transactions = t_env.from_path("transactions")
result = report(transactions)
result.execute().print()
```

{{< /tab >}}
{{< /tabs >}}

## 实现报表

在作业拉起来的大体处理框架下，你可以再添加一些业务逻辑。现在的目标是创建一个报表，报表按照账户显示一天中每个小时的总支出。因此，毫秒粒度的时间戳字段需要向下舍入到小时。

Flink 支持使用纯 [SQL]({{< ref "docs/sql/reference/overview" >}}) 或者 [Table API]({{< ref "docs/dev/table/tableApi" >}}) 开发关系型数据应用。Table API 是受 SQL 启发设计出的一套链式 DSL，可以用 Java 或 Python 开发，在 IDE 中也集成的很好。同时也如 SQL 查询一样，Table 应用可以按列查询，或者按列分组。通过类似 `floor` 以及 `sum` 这样的 [系统函数]({{< ref "docs/sql/built-in-functions" >}})，你已经可以开发这个报表了。

{{< tabs "report-impl" >}}
{{< tab "Java" >}}

```java
public static Table report(Table transactions) {
    return transactions
        .select(
            $("accountId"),
            $("transactionTime").floor(TimeIntervalUnit.HOUR).as("logTs"),
            $("amount"))
        .groupBy($("accountId"), $("logTs"))
        .select(
            $("accountId"),
            $("logTs"),
            $("amount").sum().as("amount"));
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
def report(transactions):
    return transactions \
        .select(
            col("accountId"),
            col("transactionTime").floor(TimeIntervalUnit.HOUR).alias("logTs"),
            col("amount")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs"),
            col("amount").sum.alias("amount"))
```

{{< /tab >}}
{{< /tabs >}}

## 测试

{{< tabs "testing" >}}
{{< tab "Java" >}}

项目包含一个测试类 `SpendReportTest`，使用批处理模式和静态数据来验证报表逻辑。

```java
EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
TableEnvironment tEnv = TableEnvironment.create(settings);

// 使用 fromValues 创建测试数据
Table transactions = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("accountId", DataTypes.BIGINT()),
        DataTypes.FIELD("amount", DataTypes.BIGINT()),
        DataTypes.FIELD("transactionTime", DataTypes.TIMESTAMP(3))
    ),
    Row.of(1L, 188L, LocalDateTime.of(2024, 1, 1, 9, 0, 0)),
    Row.of(1L, 374L, LocalDateTime.of(2024, 1, 1, 9, 30, 0)),
    // ... 更多测试数据
);
```

{{< /tab >}}
{{< tab "Python" >}}

你可以通过切换到批处理模式并使用静态测试数据来测试 `report` 函数。创建一个单独的测试文件（例如 `test_spend_report.py`）：

```python
from datetime import datetime
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes

from spend_report import report

def test_report():
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)

    # 使用 from_elements 创建测试数据
    transactions = t_env.from_elements(
        [
            (1, 188, datetime(2024, 1, 1, 9, 0, 0)),
            (1, 374, datetime(2024, 1, 1, 9, 30, 0)),
            (2, 200, datetime(2024, 1, 1, 9, 15, 0)),
        ],
        DataTypes.ROW([
            DataTypes.FIELD("accountId", DataTypes.BIGINT()),
            DataTypes.FIELD("amount", DataTypes.BIGINT()),
            DataTypes.FIELD("transactionTime", DataTypes.TIMESTAMP(3))
        ])
    )

    # 测试 report 函数
    result = report(transactions)

    # 收集结果并验证
    rows = [row for row in result.execute().collect()]
    assert len(rows) == 2  # 两个账户

if __name__ == '__main__':
    test_report()
    print("All tests passed!")
```

使用 `python test_spend_report.py` 或 `pytest test_spend_report.py` 运行测试。

{{< /tab >}}
{{< /tabs >}}

提供批流统一的语义是 Flink 的重要特性，这意味着应用的开发和测试可以在批模式下使用静态数据集完成，而实际部署到生产时再切换为流式。

## 用户自定义函数

Flink 内置的函数是有限的，有时是需要通过 [用户自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})来拓展这些函数。假如 `floor` 函数不是系统预设函数，也可以自己实现。

{{< tabs "udf" >}}
{{< tab "Java" >}}

```java
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class MyFloor extends ScalarFunction {

    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
        @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}
```

然后就可以在你的应用中使用了：

```java
public static Table report(Table transactions) {
    return transactions
        .select(
            $("accountId"),
            call(MyFloor.class, $("transactionTime")).as("logTs"),
            $("amount"))
        .groupBy($("accountId"), $("logTs"))
        .select(
            $("accountId"),
            $("logTs"),
            $("amount").sum().as("amount"));
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table.udf import udf
from datetime import datetime

@udf(result_type=DataTypes.TIMESTAMP(3))
def my_floor(timestamp: datetime) -> datetime:
    return timestamp.replace(minute=0, second=0, microsecond=0)
```

然后就可以在你的应用中使用了：

```python
def report(transactions):
    return transactions \
        .select(
            col("accountId"),
            my_floor(col("transactionTime")).alias("logTs"),
            col("amount")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs"),
            col("amount").sum.alias("amount"))
```

{{< /tab >}}
{{< /tabs >}}

这条查询会从表 `transactions` 消费所有的记录，然后计算报表所需内容，最后将结果以高效、可拓展的方式输出。按此逻辑实现，可以通过测试。

## Process Table Functions（仅 Java）

对于更高级的逐行处理，Flink 提供了 [Process Table Functions]({{< ref "docs/dev/table/functions/ptfs" >}})（PTFs）。PTFs 可以转换表的每一行，并且能够访问强大的功能，如状态和定时器。下面是一个简单的无状态示例，用于过滤和格式化高额交易：

```java
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

public class HighValueAlerts extends ProcessTableFunction<String> {

    private static final long HIGH_VALUE_THRESHOLD = 500;

    public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row transaction) {
        Long amount = transaction.getFieldAs("amount");
        if (amount > HIGH_VALUE_THRESHOLD) {
            Long accountId = transaction.getFieldAs("accountId");
            collect("Alert: Account " + accountId + " made a high-value transaction of " + amount);
        }
    }
}
```

你可以通过修改 `main()` 方法来尝试这个 PTF。替换或添加到现有的 `report()` 调用旁边：

```java
// 替换（或添加到）聚合报表：
// Table result = report(transactions);
// result.execute().print();

// 尝试使用 PTF 查看高额交易警报：
Table alerts = transactions.process(HighValueAlerts.class);
alerts.execute().print();
```

这将只输出超过 500 的交易警报。当 PTFs 与状态和定时器结合使用时，可以实现复杂的事件驱动逻辑——更多高级示例请参阅 [PTF 文档]({{< ref "docs/dev/table/functions/ptfs" >}})。

{{< hint info >}}
**注意：** Process Table Functions 目前仅在 Java 中可用。对于 Python，可以使用 [用户自定义表函数]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions)（UDTFs）来实现类似的逐行处理。
{{< /hint >}}

## 添加窗口函数

在数据处理中，按照时间做分组是常见操作，在处理无限流时更是如此。按时间分组的函数叫 [window]({{< ref "docs/dev/table/tableApi" >}}#groupby-window-aggregation)，Flink 提供了灵活的窗口函数语法。最常见的窗口是 `Tumble`，窗口区间长度固定，并且区间不重叠。

**尝试修改你的 `report()` 方法，使用窗口代替 `floor()`：**

{{< tabs "windows" >}}
{{< tab "Java" >}}

```java
public static Table report(Table transactions) {
    return transactions
        .window(Tumble.over(lit(10).seconds()).on($("transactionTime")).as("logTs"))
        .groupBy($("accountId"), $("logTs"))
        .select(
            $("accountId"),
            $("logTs").start().as("logTs"),
            $("amount").sum().as("amount"));
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

def report(transactions):
    return transactions \
        .window(Tumble.over(lit(10).seconds).on(col("transactionTime")).alias("logTs")) \
        .group_by(col("accountId"), col("logTs")) \
        .select(
            col("accountId"),
            col("logTs").start.alias("logTs"),
            col("amount").sum.alias("amount"))
```

{{< /tab >}}
{{< /tabs >}}

上面的代码含义为：使用滚动窗口，窗口按照指定的时间戳字段划分，区间为 10 秒。比如，时间戳为 `2024-01-01 01:23:47` 的行会进入窗口 `2024-01-01 01:23:40` 中。

不同于其他属性，时间在一个持续不断的流式应用中总是向前移动，因此基于时间的聚合总是不重复的。不同于 `floor` 以及 UDF，窗口函数是 [内部的](https://en.wikipedia.org/wiki/Intrinsic_function)，可以运行时优化。批环境中，如果需要按照时间属性分组数据，窗口函数也有便利的 API。

完成修改后，运行应用程序，你将看到每 10 秒输出一次窗口结果。

## 运行应用

这次编写的应用是一个功能齐全、有状态的分布式流式应用！查询语句持续生成交易数据，然后计算每小时的消费，最后当窗口结束时立刻输出结果。由于输入是无边界的，停止作业需要手工操作。

{{< tabs "running" >}}
{{< tab "Java" >}}

在 IDE 中运行 `SpendReport` 类，查看打印到控制台的流式结果。

{{< /tab >}}
{{< tab "Python" >}}

在命令行运行程序：

```bash
$ python spend_report.py
```

上述命令会构建 Python Table API 程序，并在本地 mini cluster 中运行。如果想将作业提交到远端集群执行，可以参考[作业提交示例]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs)。

{{< /tab >}}
{{< /tabs >}}

## 下一步

恭喜你完成本教程！以下是一些继续学习的路径：

### 深入了解 Table API

- [Table API 概述]({{< ref "docs/dev/table/tableApi" >}})：完整的 Table API 参考
- [用户自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})：为你的流水线创建自定义函数
- [流式概念]({{< ref "docs/concepts/sql-table-concepts/overview" >}})：了解动态表、时间属性等概念

### 探索其他教程

- [Flink SQL 教程]({{< ref "docs/getting-started/quickstart-sql" >}})：无需编码的交互式 SQL 查询
- [DataStream API 教程]({{< ref "docs/getting-started/datastream" >}})：使用 DataStream API 构建有状态流处理应用
- [Flink 运维练习场]({{< ref "docs/getting-started/flink-operations-playground" >}})：学习操作 Flink 集群

### 生产部署

- [部署概述]({{< ref "docs/deployment/overview" >}})：在生产环境部署 Flink
- [连接器]({{< ref "docs/connectors/table/overview" >}})：连接 Kafka、数据库、文件系统等
