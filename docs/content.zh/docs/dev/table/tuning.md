---
title: "性能调优"
weight: 11
type: docs
aliases:
  - /zh/dev/table/tuning/streaming_aggregation_optimization.html
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

<a name="performance-tuning"></a>

# 性能调优

SQL 是数据分析中使用最广泛的语言。Flink Table API 和 SQL 使用户能够以更少的时间和精力定义高效的流分析应用程序。此外，Flink Table API 和 SQL 是高效优化过的，它集成了许多查询优化和算子优化。但并不是所有的优化都是默认开启的，因此对于某些工作负载，可以通过打开某些选项来提高性能。

在这一页，我们将介绍一些实用的优化选项以及流式聚合和普通连接的内部原理，它们在某些情况下能带来很大的提升。

{{< hint info >}}
目前 [分组聚合]({{< ref "docs/sql/reference/queries/group-agg" >}}) 和 [窗口表值函数聚合]({{< ref "docs/sql/reference/queries/window-agg" >}}) （会话窗口表值函数聚合除外）都支持本页提到的流式聚合优化。
{{< /hint >}}

<a name="minibatch-aggregation"></a>

## MiniBatch 聚合

默认情况下，无界聚合算子是逐条处理输入的记录，即：（1）从状态中读取累加器，（2）累加/撤回记录至累加器，（3）将累加器写回状态，（4）下一条记录将再次从（1）开始处理。这种处理模式可能会增加 StateBackend 开销（尤其是对于 RocksDB StateBackend ）。此外，生产中非常常见的数据倾斜会使这个问题恶化，并且容易导致 job 发生反压。

MiniBatch 聚合的核心思想是将一组输入的数据缓存在聚合算子内部的缓冲区中。当输入的数据被触发处理时，每个 key 只需一个操作即可访问状态。这样可以大大减少状态开销并获得更好的吞吐量。但是，这可能会增加一些延迟，因为它会缓冲一些记录而不是立即处理它们。这是吞吐量和延迟之间的权衡。

下图说明了 mini-batch 聚合如何减少状态操作。

{{<img src="/fig/table-streaming/minibatch_agg.png" width="50%" height="50%" >}}

默认情况下，对于无界聚合算子来说，mini-batch 优化是被禁用的。开启这项优化，需要设置选项 `table.exec.mini-batch.enabled`、`table.exec.mini-batch.allow-latency` 和 `table.exec.mini-batch.size`。更多详细信息请参见[配置]({{< ref "docs/dev/table/config" >}}#execution-options)页面。

{{< hint info >}}
MiniBatch 优化对于 [Window TVF 聚合]({{< ref "docs/sql/reference/queries/window-agg" >}})始终启用，不受上述配置影响。
Window TVF 聚合将记录缓冲在[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm">}}#managed-memory)中，而非 JVM 堆内存，因此不存在 GC 过载或 OOM 的风险。
{{< /hint >}}

下面的例子显示如何启用这些选项。

{{< tabs "2d4673a0-58e4-461a-8ea3-216cbf8893ce" >}}
{{< tab "Java" >}}
```java
// instantiate table environment
TableEnvironment tEnv = ...;

// access flink configuration
TableConfig configuration = tEnv.getConfig();
// set low-level key-value options
configuration.set("table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
configuration.set("table.exec.mini-batch.allow-latency", "5 s"); // use 5 seconds to buffer input records
configuration.set("table.exec.mini-batch.size", "5000"); // the maximum number of records can be buffered by each aggregate operator task
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// instantiate table environment
val tEnv: TableEnvironment = ...

// access flink configuration
val configuration = tEnv.getConfig()
// set low-level key-value options
configuration.set("table.exec.mini-batch.enabled", "true") // enable mini-batch optimization
configuration.set("table.exec.mini-batch.allow-latency", "5 s") // use 5 seconds to buffer input records
configuration.set("table.exec.mini-batch.size", "5000") // the maximum number of records can be buffered by each aggregate operator task
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# instantiate table environment
t_env = ...

# access flink configuration
configuration = t_env.get_config()
# set low-level key-value options
configuration.set("table.exec.mini-batch.enabled", "true") # enable mini-batch optimization
configuration.set("table.exec.mini-batch.allow-latency", "5 s") # use 5 seconds to buffer input records
configuration.set("table.exec.mini-batch.size", "5000") # the maximum number of records can be buffered by each aggregate operator task
```
{{< /tab >}}
{{< /tabs >}}

<a name="local-global-aggregation"></a>

## Local-Global 聚合

Local-Global 聚合是为解决数据倾斜问题提出的，通过将一组聚合分为两个阶段，首先在上游进行本地聚合，然后在下游进行全局聚合，类似于 MapReduce 中的 Combine + Reduce 模式。例如，就以下 SQL 而言：

```sql
SELECT color, sum(id)
FROM T
GROUP BY color
```

数据流中的记录可能会倾斜，因此某些聚合算子的实例必须比其他实例处理更多的记录，这会产生热点问题。本地聚合可以将一定数量具有相同 key 的输入数据累加到单个累加器中。全局聚合将仅接收 reduce 后的累加器，而不是大量的原始输入数据。这可以大大减少网络 shuffle 和状态访问的成本。每次本地聚合累积的输入数据量基于 mini-batch 间隔。这意味着 local-global 聚合依赖于启用了 mini-batch 优化。

下图显示了 local-global 聚合如何提高性能。

{{<img src="/fig/table-streaming/local_agg.png" width="70%" height="70%" >}}



下面的例子显示如何启用 local-global 聚合。

{{< tabs "f447887d-1cf6-4a8c-ba4a-509affe3b441" >}}
{{< tab "Java" >}}
```java
// instantiate table environment
TableEnvironment tEnv = ...;

// access flink configuration
TableConfig configuration = tEnv.getConfig();
// set low-level key-value options
configuration.set("table.exec.mini-batch.enabled", "true"); // local-global aggregation depends on mini-batch is enabled
configuration.set("table.exec.mini-batch.allow-latency", "5 s");
configuration.set("table.exec.mini-batch.size", "5000");
configuration.set("table.optimizer.agg-phase-strategy", "TWO_PHASE"); // enable two-phase, i.e. local-global aggregation
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// instantiate table environment
val tEnv: TableEnvironment = ...

// access flink configuration
val configuration = tEnv.getConfig()
// set low-level key-value options
configuration.set("table.exec.mini-batch.enabled", "true") // local-global aggregation depends on mini-batch is enabled
configuration.set("table.exec.mini-batch.allow-latency", "5 s")
configuration.set("table.exec.mini-batch.size", "5000")
configuration.set("table.optimizer.agg-phase-strategy", "TWO_PHASE") // enable two-phase, i.e. local-global aggregation
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# instantiate table environment
t_env = ...

# access flink configuration
configuration = t_env.get_config()
# set low-level key-value options
configuration.set("table.exec.mini-batch.enabled", "true") # local-global aggregation depends on mini-batch is enabled
configuration.set("table.exec.mini-batch.allow-latency", "5 s")
configuration.set("table.exec.mini-batch.size", "5000")
configuration.set("table.optimizer.agg-phase-strategy", "TWO_PHASE") # enable two-phase, i.e. local-global aggregation
```
{{< /tab >}}
{{< /tabs >}}

<a name="split-distinct-aggregation"></a>

## 拆分 distinct 聚合

Local-Global 优化可有效消除常规聚合的数据倾斜，例如 SUM、COUNT、MAX、MIN、AVG。但是在处理 distinct 聚合时，其性能并不令人满意。

例如，如果我们要分析今天有多少唯一用户登录。我们可能有以下查询：

```sql
SELECT day, COUNT(DISTINCT user_id)
FROM T
GROUP BY day
```

如果 distinct key （即 user_id）的值分布稀疏，则 COUNT DISTINCT 不适合减少数据。即使启用了 local-global 优化也没有太大帮助。因为累加器仍然包含几乎所有原始记录，并且全局聚合将成为瓶颈（大多数繁重的累加器由一个任务处理，即同一天）。

这个优化的想法是将不同的聚合（例如 `COUNT(DISTINCT col)`）分为两个级别。第一次聚合由 group key 和额外的 bucket key 进行 shuffle。bucket key 是使用 `HASH_CODE(distinct_key) % BUCKET_NUM` 计算的。`BUCKET_NUM` 默认为1024，可以通过 `table.optimizer.distinct-agg.split.bucket-num` 选项进行配置。第二次聚合是由原始 group key 进行 shuffle，并使用 `SUM` 聚合来自不同 buckets 的 COUNT DISTINCT 值。由于相同的 distinct key 将仅在同一 bucket 中计算，因此转换是等效的。bucket key 充当附加 group key 的角色，以分担 group key 中热点的负担。bucket key 使 job 具有可伸缩性来解决不同聚合中的数据倾斜/热点。

拆分 distinct 聚合后，以上查询将被自动改写为以下查询：

```sql
SELECT day, SUM(cnt)
FROM (
    SELECT day, COUNT(DISTINCT user_id) as cnt
    FROM T
    GROUP BY day, MOD(HASH_CODE(user_id), 1024)
)
GROUP BY day
```


下图显示了拆分 distinct 聚合如何提高性能（假设颜色表示 days，字母表示 user_id）。

{{<img src="/fig/table-streaming/distinct_split.png" width="70%" height="70%" >}}


注意：上面是可以从这个优化中受益的最简单的示例。除此之外，Flink 还支持拆分更复杂的聚合查询，例如，多个具有不同 distinct key （例如 `COUNT(DISTINCT a), SUM(DISTINCT b)` ）的 distinct 聚合，可以与其他非 distinct 聚合（例如 `SUM`、`MAX`、`MIN`、`COUNT` ）一起使用。

{{< hint info >}}
当前，拆分优化不支持包含用户定义的 AggregateFunction 聚合。
{{< /hint >}}

下面的例子显示了如何启用拆分 distinct 聚合优化。

{{< tabs "830ca323-0d37-4f31-92df-15267ed7a659" >}}
{{< tab "Java" >}}
```java
// instantiate table environment
TableEnvironment tEnv = ...;

tEnv.getConfig()
  .set("table.optimizer.distinct-agg.split.enabled", "true");  // enable distinct agg split
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// instantiate table environment
val tEnv: TableEnvironment = ...

tEnv.getConfig
  .set("table.optimizer.distinct-agg.split.enabled", "true")  // enable distinct agg split
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# instantiate table environment
t_env = ...

t_env.get_config().set("table.optimizer.distinct-agg.split.enabled", "true") # enable distinct agg split
```
{{< /tab >}}
{{< /tabs >}}

<a name="use-filter-modifier-on-distinct-aggregates"></a>

## 在 distinct 聚合上使用 FILTER 修饰符

在某些情况下，用户可能需要从不同维度计算 UV（独立访客）的数量，例如来自 Android 的 UV、iPhone 的 UV、Web 的 UV 和总 UV。很多人会选择 `CASE WHEN`，例如：

```sql
SELECT
 day,
 COUNT(DISTINCT user_id) AS total_uv,
 COUNT(DISTINCT CASE WHEN flag IN ('android', 'iphone') THEN user_id ELSE NULL END) AS app_uv,
 COUNT(DISTINCT CASE WHEN flag IN ('wap', 'other') THEN user_id ELSE NULL END) AS web_uv
FROM T
GROUP BY day
```

但是，在这种情况下，建议使用 `FILTER` 语法而不是 CASE WHEN。因为 `FILTER` 更符合 SQL 标准，并且能获得更多的性能提升。`FILTER` 是用于聚合函数的修饰符，用于限制聚合中使用的值。将上面的示例替换为 `FILTER` 修饰符，如下所示：

```sql
SELECT
 day,
 COUNT(DISTINCT user_id) AS total_uv,
 COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('android', 'iphone')) AS app_uv,
 COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('wap', 'other')) AS web_uv
FROM T
GROUP BY day
```

Flink SQL 优化器可以识别相同的 distinct key 上的不同过滤器参数。例如，在上面的示例中，三个 COUNT DISTINCT 都在 `user_id` 一列上。Flink 可以只使用一个共享状态实例，而不是三个状态实例，以减少状态访问和状态大小。在某些工作负载下，可以获得显著的性能提升。

<a name="minibatch-regular-joins"></a>

## MiniBatch Regular Joins

默认情况下，regular join 算子是逐条处理输入的记录，即：（1）根据当前输入记录的 join key 关联对方状态中的记录，（2）根据当前记录写入或者撤回状态中的记录，（3）根据当前的输入记录和关联到的记录输出结果。
这种处理模式可能会增加 StateBackend 的开销（尤其是对于 RocksDB StateBackend ）。除此之外，这会导致严重的中间结果放大。尤其在多级级联 join 的场景，会产生很多的中间结果从而导致性能降低。

MiniBatch join 主要解决 regular join 存在的中间结果放大和 StateBackend 开销较大的问题。其核心思想是将一组输入的数据缓存在 join 算子内部的缓冲区中，一旦达到时间阈值或者缓存容量阈值，就触发 join 执行流程。
这有两个主要的优化点：

1) 在缓存中折叠数据，以此减少 join 的次数。
2) 尽最大可能在处理数据时抑制冗余数据下发。

以 left join 为例子，左右流的输入都是 join key 包含 unique key 的情况。假设 `id` 为 join key 和 unique key （数字代表 `id`, 字母代表 `content`）, 具体 SQL 如下:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5S';
SET 'table.exec.mini-batch.size' = '5000';
    
SELECT a.id as a_id, a.a_content, b.id as b_id, b.b_content
FROM a LEFT JOIN b
ON a.id = b.id
```

针对上述场景，mini-batch join 算子的具体处理过程如下图所示。

{{< img src="/fig/table-streaming/minibatch_join.png" width="70%" height="70%" >}}

默认情况下，对于 regular join 算子来说，mini-batch 优化是被禁用的。开启这项优化，需要设置选项 `table.exec.mini-batch.enabled`、`table.exec.mini-batch.allow-latency` 和 `table.exec.mini-batch.size`。更多详细信息请参见[配置]({{< ref "docs/dev/table/config" >}}#execution-options)页面。

<a name="multiple-regular-joins"></a>

## Multiple Regular Joins

{{< label Streaming >}}

在流处理场景中，包含多个非时态 Regular Join 的 Flink 作业，往往因状态过大而频繁出现运行不稳定和性能下降的问题。究其根本，是由于多个 Join 串联后产生的中间状态，往往远超原始输入数据本身的规模。为此，Flink 2.1 引入了全新的 Multi Join 算子，这一优化专为存在记录膨胀和大量中间状态的 Join 流水线而设计，旨在大幅压缩状态规模、提升整体性能。该算子通过同时处理多个输入流来完成跨表 Join，从根本上消除了在多表 Join 链路中存储中间状态的必要性。这种“零中间状态”的处理方式以降低状态规模为核心目标，在某些场景下可显著减少资源消耗、提升运行稳定性。当然，这一方案本质上是一种以算力换存储的权衡取舍，中间状态不再持久化保存，而是在需要时按需重新计算。

在大多数 Join 操作中，相当一部分处理时间都花费在从状态中读取记录上。MultiJoin 算子的效率在很大程度上取决于中间状态的大小以及公共 Join Key 的选择性。在一种常见场景中——即流水线出现记录膨胀（每次 Join 产生的数据量和记录数均多于上一次）时，MultiJoin 算子的优势更为突出。这是因为它使算子所需操作的状态始终保持在较小的规模，从而让算子运行更加稳定。即便某条 Join 链路实际产生的状态比原始记录还要小，MultiJoin 算子整体上仍然占用更少的状态。不过在这种特殊情况下，二元 Join 反而可能表现更好，因为最终几个 Join 所需操作的状态本身已经很小。

<a name="the-multijoin-operator"></a>

### MultiJoin 算子

MultiJoin 算子的主要优势：

1) 状态规模大幅缩减：得益于零中间状态机制，状态占用显著降低。
2) 链式 Join 性能提升：在存在记录膨胀的场景下，整体处理性能得到明显改善。
3) 稳定性增强：状态随处理记录数呈线性增长，而非像二元 Join 那样呈多项式级增长。

此外，使用 MultiJoin 替代二元 Join 的流水线通常具有更快地初始化和故障恢复速度，这得益于更小的状态规模以及更少的算子节点。

<a name="when-to-enable-the-multijoin"></a>

### 何时启用 MultiJoin？

如果作业中存在多个 Join 共享至少一个公共 Join Key 的 Join 操作，并且中间 Join 产生的中间状态比原始输入数据源还要大，建议考虑开启 MultiJoin 算子。

推荐使用场景：
- 公共 Join Key 具有较高选择性（即每个键对应的记录数较少）
- 包含多个链式 Join 且中间状态很大的 SQL 语句
- 公共 Join Key 没有明显的数据倾斜
- Join 操作产生了大量状态（状态规模达 50 GB 及以上）

如果公共 Join Key 的选择性较低（即大量记录共享相同键值），MultiJoin 算子所需的中间状态重新计算将对性能产生严重影响。在此类场景下，建议使用二元 Join，因为二元 Join 会利用所有 Join Key 对数据进行分区，从而避免重算影响性能。

<a name="how-to-enable-the-multijoin"></a>

### 如何启用 MultiJoin？

要对所有符合条件的 Join 全局启用此优化，请设置以下配置：

```sql
SET 'table.optimizer.multi-join.enabled' = 'true';
```

或者，你可以使用 `MULTI_JOIN` hint 为特定表启用 MultiJoin 算子：

```sql
SELECT /*+ MULTI_JOIN(t1, t2, t3) */ * FROM t1 
JOIN t2 ON t1.id = t2.id 
JOIN t3 ON t1.id = t3.id;
```

Hint 方式允许你有针对性地将 MultiJoin 优化应用于特定查询块，而无需全局启用。有关 `MULTI_JOIN` hint 的更多详情，请参阅 [Join Hints]({{< ref "docs/sql/reference/queries/hints" >}}#multi_join)。需要注意的是，配置项的优先级高于 hint。

重要提示：该功能目前处于实验性阶段，后续可能会进行优化或引入破坏性变更。当前仅支持流式 INNER/LEFT Join。由于记录分区的机制，Join 条件之间至少需要一个公共 Key，详见下方示例：

- 支持：A JOIN B ON A.key = B.key JOIN C ON A.key = C.key（按 key 分区）
- 支持：A JOIN B ON A.key = B.key JOIN C ON B.key = C.key（通过传递性 key 分区）
- 不支持：A JOIN B ON A.key1 = B.key1 JOIN C ON B.key2 = C.key2（没有单个 key 可以将 A、B、C 同时分区到单个算子中。这将被拆分为多个 MultiJoin 算子）

<a name="multijoin-operator-example---benchmark"></a>

### MultiJoin 算子示例 - 基准测试

以下是默认二元 Join 与 MultiJoin 算子之间的 10-way 基准测试对比。可以在第一部分观察到中间状态的数量，第二部分观察算子达到 100% 忙碌时处理的记录数，第三部分是 checkpoint。

{{< img src="/fig/table-streaming/multijoin_operator.png" height="100%" >}}

对于上面这个涉及记录放大的 10-way join，可以看到有显著提升。这里有一些大概数据：

- 性能：当两者都处于 100% 忙碌时，处理记录量提升 2 倍到超过 100 倍。
- 状态大小：中间状态大小缩小 3 倍到超过 1000 倍。

MultiJoin 算子的总状态始终更小。在这种情况下，初始性能相同，但随着中间状态增长，二元 Join 的性能逐渐下降，而 Multi Join 保持稳定并表现更出色。

这个 10-way Join 通用基准测试使用以下配置运行：每个 tenant_id 1 条记录（高选择性），10 个 upsert Kafka topic，并行度 10，每个 topic 每秒 1 条记录。我们使用了基于 RocksDB 的未对齐 checkpoint 和增量 checkpoint。每个作业运行在 8GB 进程内存的 TaskManager 中，1GB 堆外内存和 20% 网络内存。JobManager 有 4GB 进程内存。主机包含 M1 处理器芯片，32GB RAM 和 1TB SSD。sink 使用 blackhole connector，因此我们只对 Join 进行基准测试。用于生成基准测试数据的 SQL 结构如下：

```sql
INSERT INTO JoinResultsMJ
SELECT *all fields*
FROM TenantKafka t
         LEFT JOIN SuppliersKafka s ON t.tenant_id = s.tenant_id AND ...
         LEFT JOIN ProductsKafka p ON t.tenant_id = p.tenant_id AND ...
         LEFT JOIN CategoriesKafka c ON t.tenant_id = c.tenant_id AND ...
         LEFT JOIN OrdersKafka o ON t.tenant_id = o.tenant_id AND ...
         LEFT JOIN CustomersKafka cust ON t.tenant_id = cust.tenant_id AND ...
         LEFT JOIN WarehousesKafka w ON t.tenant_id = w.tenant_id AND ...
         LEFT JOIN ShippingKafka sh ON t.tenant_id = sh.tenant_id AND ...
         LEFT JOIN PaymentKafka pay ON t.tenant_id = pay.tenant_id AND ...
         LEFT JOIN InventoryKafka i ON t.tenant_id = i.tenant_id AND ...;
```

<a name="delta-joins"></a>

## Delta Joins

在流作业中，regular join 会维护来自两个输入的所有历史数据，以确保结果的准确性。随着时间的推移，这会导致状态不断增长，从而增加资源的使用，并影响作业的稳定性。

为了应对这些挑战，Flink 引入了 delta join 算子。其核心思想是基于双向 lookup join 来替代 regular join 所维护的大状态，直接重用源表中的数据。与传统的 regular join 相比，delta join 显著减少了状态大小，提高了作业的稳定性，并降低了总体的资源消耗。

该功能默认启用。当满足以下所有条件时， regular join 将自动优化为 delta join。

1. 作业拓扑结构满足优化条件。具体可以查看[支持的功能和限制]({{< ref "docs/dev/table/tuning" >}}#supported-features-and-limitations)。
2. 源表所在的外部存储系统提供了可供 delta join 快速查询的索引信息。目前 [Apache Fluss(Incubating)](https://fluss.apache.org/blog/fluss-open-source/) 已支持在 Flink 中提供表级别的索引信息，其上的表可作为 delta join 的源表。具体可参考 [Fluss 文档](https://fluss.apache.org/docs/engine-flink/delta-joins/#flink-version-support)。

<a name="working-principle"></a>

### 工作原理

在 Flink 中，regular join 将来自两个输入端的所有输入数据存储在状态中，以确保当对侧的数据到达时，能够正确地匹配对应的记录。

相比之下，delta join 利用了外部存储系统的索引功能，并不执行状态查找，而是直接对外部存储发出高效的、基于索引的查询，以获取匹配的记录。该方法消除了 Flink 状态与外部系统之间冗余的数据存储。

{{< img src="/fig/table-streaming/delta_join.png" width="70%" height="70%" >}}

<a name="important-configurations"></a>

### 关键参数

Delta join 优化默认启用。您可以通过设置以下配置手动禁用此功能：

```sql
SET 'table.optimizer.delta-join.strategy' = 'NONE';
```

详细信息请参见[配置]({{< ref "docs/dev/table/config" >}}#optimizer-options)页面。

您还可以配置以下参数来调整优化 delta join 的性能。

- `table.exec.delta-join.cache-enabled`
- `table.exec.delta-join.left.cache-size`
- `table.exec.delta-join.right.cache-size`

详细信息请参见[配置]({{< ref "docs/dev/table/config" >}}#execution-options)页面。

<a name="supported-features-and-limitations"></a>

### 支持的功能和限制

目前 delta join 仍在持续演进中，当前版本已支持的功能如下：

1. 支持 **INSERT-only** 的表作为源表。
2. 支持不带 **DELETE 操作**的 **CDC** 表作为源表。
3. 支持源表和 delta join 间包含 **project** 和 **filter** 算子。
4. Delta join 算子内支持**缓存**。

然而，delta join 也存在几个**限制**，包含以下任何条件的作业无法优化为 delta join。

1. 表的**索引键**必须包含在 join 的**等值条件**中
2. 目前仅支持 **INNER JOIN**。
3. **下游节点**必须能够处理**冗余变更**。例如以 **UPSERT 模式**运行、不带 `upsertMaterialize` 的 sink 节点。
4. 当消费 **CDC 流**时，**join key** 必须是**主键**的一部分。
5. 当消费 **CDC 流**时，所有 **filter** 必须应用于 **upsert key** 上。
6. 所有 project 和 filter 都不能包含**非确定性函数**。

{{< top >}}
