---
title: 'DataStream API 教程'
nav-title: 'DataStream API 教程'
weight: 4
type: docs
aliases:
  - /zh/try-flink/datastream_api.html
  - /zh/getting-started/walkthroughs/datastream_api.html
  - /zh/quickstart/run_example_quickstart.html
  - /zh/dev/python/datastream_tutorial.html
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

# DataStream API 教程

Apache Flink 提供了 DataStream API 作为其最底层的流处理 API。DataStream API 提供了对状态、时间和自定义处理逻辑的细粒度控制。这使其非常适合构建高级事件驱动应用程序。

## 你将构建什么

在本教程中，你将构建一个处理信用卡交易的欺诈检测系统：

```
交易数据（数据源） → Flink（KeyedProcessFunction） → 警报（Sink）
```

你将学习：

- 设置 DataStream 执行环境
- 创建数据源进行数据输入
- 使用 `keyBy` 对流进行分区以实现并行处理
- 使用 `KeyedProcessFunction` 实现业务逻辑
- 使用托管状态（`ValueState`）实现容错处理

## 准备条件

本教程假设你对 Java 或 Python 有一定了解，当然如果你使用的是其他编程语言，也可以继续学习。

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
这些依赖包括 `flink-streaming-java`（所有 Flink 流处理应用的核心依赖）和 `flink-walkthrough-common`（包含本教程所需的数据生成器和其他类）。

{{< unstable >}}
{{< hint warning >}}
**注意：** Maven archetype 仅适用于 Apache Flink 发行版。

由于你目前正在浏览快照版的文档，因此下文中引用的版本可能已经不存在了，请先通过左侧菜单下方的版本选择器切换到发行版文档再查看。
{{< /hint >}}
{{< /unstable >}}

```bash
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion={{< version >}} \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=frauddetection \
    -DinteractiveMode=false
```

你可以根据需要修改 `groupId`、`artifactId` 和 `package`。使用上述参数，Maven 将创建一个名为 `frauddetection` 的目录，其中包含完成本教程所需的所有依赖项目。

将项目导入编辑器后，你可以找到文件 `FraudDetectionJob.java`，其中包含以下代码，可以直接在 IDE 中运行。

{{< hint info >}}
**在 IDE 中运行：** 如果遇到 `java.lang.NoClassDefFoundError` 异常，这可能是因为类路径中没有包含所有必需的 Flink 依赖项。

* **IntelliJ IDEA：** 转到 Run > Edit Configurations > Modify options > 选择 "include dependencies with 'Provided' scope"。
{{< /hint >}}

{{< /tab >}}
{{< tab "Python" >}}

使用 Python DataStream API 需要安装 PyFlink，它已经被发布到 [PyPi](https://pypi.org/project/apache-flink/)，你可以通过如下方式安装：

```bash
$ python -m pip install apache-flink
```

{{< hint info >}}
**提示：** 建议在 [虚拟环境](https://docs.python.org/zh-cn/3/library/venv.html) 中安装 PyFlink，以保持项目依赖隔离。
{{< /hint >}}

安装 PyFlink 后，创建一个名为 `fraud_detection.py` 的新文件来编写 DataStream 程序。

{{< /tab >}}
{{< /tabs >}}

## 完整代码

以下是欺诈检测程序的完整代码：

{{< tabs "complete-program" >}}
{{< tab "Java" >}}

```java
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
            .fromSource(
                TransactionSource.unbounded(),
                WatermarkStrategy.noWatermarks(),
                "transactions");

        DataStream<Alert> alerts = transactions
            .keyBy(Transaction::getAccountId)
            .process(new FraudDetector())
            .name("fraud-detector");

        alerts
            .addSink(new AlertSink())
            .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
```

```java
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;

    private transient ValueState<Boolean> flagState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            flagState.clear();
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);
        }
    }
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


class FraudDetector(KeyedProcessFunction):

    SMALL_AMOUNT = 1.00
    LARGE_AMOUNT = 500.00

    def __init__(self):
        self.flag_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("flag", Types.BOOLEAN())
        self.flag_state = runtime_context.get_state(descriptor)

    def process_element(self, transaction, ctx: 'KeyedProcessFunction.Context'):
        # transaction 是一个元组: (account_id, timestamp, amount)
        account_id = transaction[0]
        amount = transaction[2]

        last_transaction_was_small = self.flag_state.value()

        if last_transaction_was_small is not None:
            if amount > self.LARGE_AMOUNT:
                yield f"Alert{{id={account_id}}}"
            self.flag_state.clear()

        if amount < self.SMALL_AMOUNT:
            self.flag_state.update(True)


def fraud_detection():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 示例交易数据: (account_id, timestamp, amount)
    transactions_data = [
        (1, 1000, 188.23),
        (2, 1001, 0.50),    # 小额交易
        (2, 1002, 600.00),  # 大额交易 - 警报！
        (3, 1003, 42.00),
        (1, 1004, 0.89),    # 小额交易
        (1, 1005, 300.00),  # 金额不够大 - 无警报
        (4, 1006, 0.10),    # 小额交易
        (4, 1007, 520.00),  # 大额交易 - 警报！
        (3, 1008, 0.75),    # 小额交易
        (3, 1009, 800.00),  # 大额交易 - 警报！
    ]

    transactions = env.from_collection(
        transactions_data,
        type_info=Types.TUPLE([Types.LONG(), Types.LONG(), Types.DOUBLE()])
    )

    alerts = transactions \
        .key_by(lambda t: t[0]) \
        .process(FraudDetector())

    alerts.print()

    env.execute("Fraud Detection")


if __name__ == '__main__':
    fraud_detection()
```

{{< /tab >}}
{{< /tabs >}}

## 代码分析

下面逐步分析代码。主类定义了应用的数据流，而 `FraudDetector` 类定义了检测欺诈交易的业务逻辑。

### 执行环境

第一行代码设置了 `StreamExecutionEnvironment`。执行环境用于设置作业属性、创建数据源，以及触发作业执行。

{{< tabs "environment" >}}
{{< tab "Java" >}}

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

{{< /tab >}}
{{< tab "Python" >}}

```python
env = StreamExecutionEnvironment.get_execution_environment()
```

{{< /tab >}}
{{< /tabs >}}

### 创建数据源

数据源从外部系统（如 Apache Kafka、Rabbit MQ 或 Apache Pulsar）获取数据并送入 Flink 作业。

{{< tabs "source" >}}
{{< tab "Java" >}}

本教程使用 `TransactionSource`，它封装了 `DataGeneratorSource` 来生成无限的信用卡交易数据流。每笔交易包含账户 ID（`accountId`）、交易发生的时间戳（`timestamp`）和交易金额（`amount`）。

```java
DataStream<Transaction> transactions = env
    .fromSource(
        TransactionSource.unbounded(),
        WatermarkStrategy.noWatermarks(),
        "transactions");
```

`fromSource` 方法接受三个参数：数据源本身、水印策略（本示例使用 `noWatermarks()` 因为使用的是处理时间），以及用于调试的名称。

{{< /tab >}}
{{< tab "Python" >}}

本教程使用示例交易数据集合。每笔交易是一个包含账户 ID、时间戳和金额的元组。

```python
transactions_data = [
    (1, 1000, 188.23),
    (2, 1001, 0.50),    # 小额交易
    (2, 1002, 600.00),  # 大额交易 - 警报！
    # ...更多交易
]

transactions = env.from_collection(
    transactions_data,
    type_info=Types.TUPLE([Types.LONG(), Types.LONG(), Types.DOUBLE()])
)
```

在生产系统中，你通常会使用 Kafka 等数据源连接器。`from_collection` 方法便于测试和教程使用。

{{< /tab >}}
{{< /tabs >}}

### 对事件分区与欺诈检测

`transactions` 流包含大量来自众多用户的交易数据，需要由多个欺诈检测任务并行处理。由于欺诈行为是基于账户发生的，你必须确保同一账户的所有交易都由同一个欺诈检测算子的并行任务处理。

为了确保同一物理任务处理特定 key 的所有记录，你可以使用 `keyBy` 对流进行分区。`process()` 调用添加了一个算子，该算子对流中的每个分区元素应用函数。通常说紧跟在 `keyBy` 之后的算子（在这个例子中是 `FraudDetector`）在 _keyed context_ 中执行。

{{< tabs "keyby" >}}
{{< tab "Java" >}}

```java
DataStream<Alert> alerts = transactions
    .keyBy(Transaction::getAccountId)
    .process(new FraudDetector())
    .name("fraud-detector");
```

{{< /tab >}}
{{< tab "Python" >}}

```python
alerts = transactions \
    .key_by(lambda t: t[0]) \
    .process(FraudDetector())
```

{{< /tab >}}
{{< /tabs >}}

### 输出结果

Sink 将 `DataStream` 写入外部系统，如 Apache Kafka、Cassandra 或 AWS Kinesis。

{{< tabs "sink" >}}
{{< tab "Java" >}}

`AlertSink` 使用 **INFO** 日志级别记录每个 `Alert` 记录，而不是写入持久存储，这样你可以方便地查看结果。

```java
alerts.addSink(new AlertSink());
```

{{< /tab >}}
{{< tab "Python" >}}

`print()` 方法将警报输出到控制台，方便查看。

```python
alerts.print()
```

{{< /tab >}}
{{< /tabs >}}

### 欺诈检测器

欺诈检测器实现为 `KeyedProcessFunction`。它的 `processElement` 方法会在每个交易事件上被调用。这个初始版本对每笔交易都产生警报，有些人可能会说这过于保守了。

下一节将指导你使用更有意义的业务逻辑来扩展欺诈检测器。

{{< tabs "detector-basic" >}}
{{< tab "Java" >}}

```java
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
class FraudDetector(KeyedProcessFunction):

    SMALL_AMOUNT = 1.00
    LARGE_AMOUNT = 500.00

    def process_element(self, transaction, ctx: 'KeyedProcessFunction.Context'):
        account_id = transaction[0]
        yield f"Alert{{id={account_id}}}"
```

{{< /tab >}}
{{< /tabs >}}

## 实现业务逻辑

对于第一个版本，欺诈检测器应该对任何小额交易紧跟大额交易的账户输出警报。其中小额是指低于 1.00 的交易，大额是指超过 500 的交易。

假设你的欺诈检测器处理某个特定账户的以下交易流。

{{< img src="/fig/fraud-transactions.svg" alt="Fraud Transaction" >}}

交易 3 和 4 应该被标记为欺诈，因为这是一笔 0.09 的小额交易紧跟着一笔 510 的大额交易。另外，交易 7、8 和 9 不是欺诈，因为 0.02 的小额交易之后没有紧跟大额交易；相反，中间有一笔交易打断了这个模式。

为此，欺诈检测器必须 _记住_ 跨事件的信息；只有当上一笔交易是小额交易时，大额交易才是欺诈交易。跨事件记住信息需要使用 [状态]({{< ref "docs/concepts/glossary#managed-state" >}})，这就是本教程使用 [KeyedProcessFunction]({{< ref "docs/dev/datastream/operators/process_function" >}}) 的原因。它提供了对状态和时间的细粒度控制，这将允许你使用更复杂的需求来演进算法。

最直接的实现是一个布尔标志，每当处理小额交易时就设置它。当大额交易到来时，你只需检查该账户的标志是否已设置。

然而，仅仅将标志作为 `FraudDetector` 类的成员变量来实现是行不通的。Flink 使用同一个 `FraudDetector` 对象实例处理多个账户的交易，这意味着如果账户 A 和 B 被路由到同一个 `FraudDetector` 实例，账户 A 的交易可能会将标志设置为 true，然后账户 B 的交易可能会触发误报。你当然可以使用 `Map` 这样的数据结构来跟踪各个 key 的标志，但是简单的成员变量不是容错的，如果发生故障，所有信息都会丢失。因此，如果应用程序需要重启以从故障中恢复，欺诈检测器可能会漏掉警报。

为了应对这些挑战，Flink 提供了容错状态的原语，这些原语几乎和普通成员变量一样易于使用。

Flink 中最基本的状态类型是 [ValueState]({{< ref "docs/dev/datastream/fault-tolerance/state" >}})，一种为其包装的任何变量添加容错能力的数据类型。`ValueState` 是一种 _keyed state_，这意味着它只能在 _keyed context_ 中的算子中使用；任何紧跟在 `keyBy` 之后调用的算子。算子的 _keyed state_ 自动限定在当前处理记录的 key 范围内。在这个例子中，key 是当前交易的账户 ID（由 `keyBy()` 声明），`FraudDetector` 为每个账户维护独立的状态。

`ValueState` 使用 `ValueStateDescriptor` 创建，其中包含 Flink 应该如何管理变量的元数据。状态应该在函数开始处理数据之前注册。正确的钩子是 `open()` 方法。

{{< tabs "state-setup" >}}
{{< tab "Java" >}}

```java
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Boolean> flagState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
    }
```

{{< /tab >}}
{{< tab "Python" >}}

```python
class FraudDetector(KeyedProcessFunction):

    def __init__(self):
        self.flag_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("flag", Types.BOOLEAN())
        self.flag_state = runtime_context.get_state(descriptor)
```

{{< /tab >}}
{{< /tabs >}}

`ValueState` 是一个包装类，类似于 Java 标准库中的 `AtomicReference` 或 `AtomicLong`。它提供了三个与其内容交互的方法；`update` 设置状态，`value` 获取当前值，`clear` 删除其内容。如果特定 key 的状态为空（例如在应用程序开始时或调用 `clear` 之后），那么 `value` 将返回 `null`（Java）或 `None`（Python）。对 `value` 返回对象的修改不保证被系统识别，因此所有更改必须通过 `update` 执行。否则，容错由 Flink 在后台自动管理，因此你可以像与任何标准变量一样与其交互。

下面，你可以看到如何使用标志状态来跟踪潜在的欺诈交易的示例。

{{< tabs "state-usage" >}}
{{< tab "Java" >}}

```java
@Override
public void processElement(
        Transaction transaction,
        Context context,
        Collector<Alert> collector) throws Exception {

    // 获取当前 key 的当前状态
    Boolean lastTransactionWasSmall = flagState.value();

    // 检查标志是否已设置
    if (lastTransactionWasSmall != null) {
        if (transaction.getAmount() > LARGE_AMOUNT) {
            // 向下游输出警报
            Alert alert = new Alert();
            alert.setId(transaction.getAccountId());

            collector.collect(alert);
        }

        // 清理状态
        flagState.clear();
    }

    if (transaction.getAmount() < SMALL_AMOUNT) {
        // 将标志设置为 true
        flagState.update(true);
    }
}
```

{{< /tab >}}
{{< tab "Python" >}}

```python
def process_element(self, transaction, ctx: 'KeyedProcessFunction.Context'):
    account_id = transaction[0]
    amount = transaction[2]

    # 获取当前 key 的当前状态
    last_transaction_was_small = self.flag_state.value()

    # 检查标志是否已设置
    if last_transaction_was_small is not None:
        if amount > self.LARGE_AMOUNT:
            # 向下游输出警报
            yield f"Alert{{id={account_id}}}"

        # 清理状态
        self.flag_state.clear()

    if amount < self.SMALL_AMOUNT:
        # 将标志设置为 true
        self.flag_state.update(True)
```

{{< /tab >}}
{{< /tabs >}}

对于每笔交易，欺诈检测器都会检查该账户的标志状态。请记住，`ValueState` 始终限定在当前 key（即账户）的范围内。如果标志非空，则该账户上一笔交易是小额的，因此如果当前交易金额很大，检测器将输出欺诈警报。

在该检查之后，标志状态被无条件清除。要么当前交易触发了欺诈警报，模式结束，要么当前交易没有触发警报，模式被打断需要重新开始。

最后，检查当前交易金额是否为小额。如果是，则设置标志以便下一个事件可以检查它。注意 `ValueState<Boolean>` 有三种状态：未设置（`null`/`None`）、`true`/`True` 和 `false`/`False`，因为所有 `ValueState` 都是可空的。这个作业只使用未设置和 `true` 来检查标志是否已设置。

## 完整实现

{{< tabs "complete-impl" >}}
{{< tab "Java" >}}

以下是带有状态欺诈检测的完整 `FraudDetector` 实现：

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;

    private transient ValueState<Boolean> flagState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        // 获取当前 key 的当前状态
        Boolean lastTransactionWasSmall = flagState.value();

        // 检查标志是否已设置
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                // 向下游输出警报
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }

            // 清理状态
            flagState.clear();
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // 将标志设置为 true
            flagState.update(true);
        }
    }
}
```

{{< /tab >}}
{{< tab "Python" >}}

以下是完整的欺诈检测程序：

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


class FraudDetector(KeyedProcessFunction):

    SMALL_AMOUNT = 1.00
    LARGE_AMOUNT = 500.00

    def __init__(self):
        self.flag_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("flag", Types.BOOLEAN())
        self.flag_state = runtime_context.get_state(descriptor)

    def process_element(self, transaction, ctx: 'KeyedProcessFunction.Context'):
        # transaction 是一个元组: (account_id, timestamp, amount)
        account_id = transaction[0]
        amount = transaction[2]

        # 获取当前 key 的当前状态
        last_transaction_was_small = self.flag_state.value()

        # 检查标志是否已设置
        if last_transaction_was_small is not None:
            if amount > self.LARGE_AMOUNT:
                # 向下游输出警报
                yield f"Alert{{id={account_id}}}"

            # 清理状态
            self.flag_state.clear()

        if amount < self.SMALL_AMOUNT:
            # 将标志设置为 true
            self.flag_state.update(True)


def fraud_detection():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 示例交易数据: (account_id, timestamp, amount)
    transactions_data = [
        (1, 1000, 188.23),
        (2, 1001, 0.50),    # 小额交易
        (2, 1002, 600.00),  # 大额交易 - 警报！
        (3, 1003, 42.00),
        (1, 1004, 0.89),    # 小额交易
        (1, 1005, 300.00),  # 金额不够大 - 无警报
        (4, 1006, 0.10),    # 小额交易
        (4, 1007, 520.00),  # 大额交易 - 警报！
        (3, 1008, 0.75),    # 小额交易
        (3, 1009, 800.00),  # 大额交易 - 警报！
    ]

    transactions = env.from_collection(
        transactions_data,
        type_info=Types.TUPLE([Types.LONG(), Types.LONG(), Types.DOUBLE()])
    )

    alerts = transactions \
        .key_by(lambda t: t[0]) \
        .process(FraudDetector())

    alerts.print()

    env.execute("Fraud Detection")


if __name__ == '__main__':
    fraud_detection()
```

{{< /tab >}}
{{< /tabs >}}

## 运行应用

就是这样，一个功能完整的、有状态的、分布式的流处理应用程序！查询持续从数据源消费交易，检测欺诈模式，并在准备好时发出警报。由于输入是无界的（在 Java 版本中），查询会一直运行直到手动停止。

{{< tabs "running" >}}
{{< tab "Java" >}}

在 IDE 中运行 `FraudDetectionJob` 类，查看打印到控制台的流式结果。你应该看到类似以下的输出：

```
2024-01-01 14:22:06,220 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2024-01-01 14:22:11,383 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2024-01-01 14:22:16,551 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
```

{{< /tab >}}
{{< tab "Python" >}}

在命令行运行程序：

```bash
$ python fraud_detection.py
```

你应该看到类似以下的输出：

```
Alert{id=2}
Alert{id=4}
Alert{id=3}
```

上述命令会构建 PyFlink 程序，并在本地 mini cluster 中运行。如果想将作业提交到远端集群执行，可以参考[作业提交示例]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs)。

{{< /tab >}}
{{< /tabs >}}

## 下一步

恭喜你完成本教程！以下是一些继续学习的路径：

### 增强欺诈检测器

当前实现检测小额后大额的交易模式，但真正的欺诈检测通常包含时间约束。例如，骗子通常不会在测试交易和大额消费之间等待太久。

要了解如何为欺诈检测器添加定时器（例如，只标记在 1 分钟内发生的交易），请参阅 Learn Flink 中的[事件驱动应用]({{< ref "docs/learn-flink/event_driven" >}})部分。

### 深入了解 DataStream

- [DataStream API 概述]({{< ref "docs/dev/datastream/overview" >}})：完整的 DataStream API 参考
- [处理函数]({{< ref "docs/dev/datastream/operators/process_function" >}})：深入了解 KeyedProcessFunction 和其他处理函数
- [状态与容错]({{< ref "docs/dev/datastream/fault-tolerance/state" >}})：理解托管状态和检查点

### 探索其他教程

- [Flink SQL 教程]({{< ref "docs/getting-started/quickstart-sql" >}})：无需编码的交互式 SQL 查询
- [Table API 教程]({{< ref "docs/getting-started/table_api" >}})：使用 Table API 构建流处理管道
- [Flink 运维练习场]({{< ref "docs/getting-started/flink-operations-playground" >}})：学习操作 Flink 集群

### 生产部署

- [部署概述]({{< ref "docs/deployment/overview" >}})：在生产环境部署 Flink
- [连接器]({{< ref "docs/connectors/datastream/overview" >}})：连接 Kafka、数据库、文件系统等
