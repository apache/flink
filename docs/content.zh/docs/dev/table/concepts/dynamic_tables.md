---
title: "动态表 (Dynamic Table)"
weight: 2
type: docs
aliases:
  - /zh/dev/table/streaming/dynamic_tables.html
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

# 动态表 (Dynamic Table)

SQL 和 Table API 为实时数据处理提供灵活而强大的功能。本文描述了关系型概念如何优雅的转为流式，从而使 Flink 在无界流上实现相同语义。



数据流中的关系查询
----------------------------------

下表对比了传统的关系代数和流处理在输入数据、执行和输出结果的关系。

| 关系代数/SQL                                                 | 流处理                                                     |
| ------------------------------------------------------------ | ---------------------------------------------------------- |
| 关系型(或表)是有界(多)元组（数据）集合。                     | 流是一个无限元组（数据）序列。                             |
| 对批数据(例如关系数据库中的表)执行的查询可以访问完整的输入数据。 | 流式查询在启动时不能访问所有数据，必须“等待”数据流入。     |
| 批处理查询在产生固定大小的结果后终止。                       | 流查询不断地根据接收到的记录更新其结果，并且始终不会结束。 |

注：元组为关系数据库的基本概念，关系是一张表，表中的每行（每条记录或是说每条数据）就是一个元组。——百度百科

尽管存在这些差异，但是使用关系查询和 SQL 处理流仍是可能的。高级的关系型数据库提供了一个称为*物化视图(Materialized Views)* 的特性。物化视图被定义为 SQL 查询语句，类似常规的虚拟视图。与虚拟视图相比，物化视图缓存了查询的结果，因此在访问视图时不需要对查询进行计算。缓存的一个常见难题是防止缓存提供过期的结果。当基表，也就是定义的查询结果被修改时，物化视图则视为过期。 *即时视图维护(Eager View Maintenance)* 是一种一旦物化视图的基表发生变化就立即更新物化视图的技术。



我们思考以下几点，那么即时视图维护和流上的SQL查询之间的联系就会变得清晰:

- 数据库表的结果数据来源于一连串的 `INSERT`、`UPDATE` 和 `DELETE` DML 语句，这通常称为  *changelog 流* 。
- 物化视图被定义为 SQL 查询语句。为了更新视图，在视图的底层事务里，查询必须不断地处理 changelog 流。
- 物化视图是不断流式 SQL 查询的结果。

带着这些要点，我们在下面介绍 *动态表(Dynamic tables)* 的概念。



动态表 &amp; 连续查询(Continuous Query)
---------------------------------------

*动态表* 是 Flink 的 Table API 和 SQL 支持流数据的核心概念。与表示批处理数据的静态表相比，动态表是随时间而变化。但动态表可以像静态表一样执行查询语句。查询动态表将生成 *Continuous Query（连续查询）* 。连续查询永远不会终止，结果会生成一个动态结果（另一张动态表）。查询通过不断地更新结果表(动态表)来反映输入表（动态表）上的更改。本质上，动态表上的连续查询和物化视图的查询非常类似。

需要注意的是，连续查询的结果在语义上等同于在批处理模式下输入表执行相同查询语句的快照结果。

下图显示了流、动态表和连续查询之间的关系:

{{< img alt="Dynamic tables" src="/fig/table-streaming/stream-query-stream.png" width="80%">}}

1. 将流转换为动态表。
2. 在动态表上进行连续查询处理后，生成一个新的动态表。
3. 生成的动态表被转回流。

**注意：** 首先动态表是一个逻辑概念。在查询执行期间不一定(完全)物化动态表。

下面，我们将使用如下表结构，结合流式的点击事件，来解释中动态表和连续查询的概念:

```sql
CREATE TABLE clicks (
  user  VARCHAR,     -- 用户名
  url   VARCHAR,     -- 用户访问的 URL
  cTime TIMESTAMP(3) -- 访问 URL 的时间
) WITH (...);
```



在流上定义表
----------------------------

为了使用关系查询处理流，必须将其转换成 `Table`。从概念上讲，流的每条记录都被解释为对结果表的 `INSERT` 操作。本质上我们是从一个 `INSERT-only` 的 changelog 流来构建表。

下图显示了点击事件流(左侧)如何转换为表(右侧)。当插入更多的单击流记录时，结果表将不断增长。

{{< img alt="Append mode" src="/fig/table-streaming/append-mode.png" width="60%">}}

**注意：** 在流上定义的表在内部没有物化。



### 连续查询
----------------------

在动态表中通过连续查询的处理，并生成一个新的动态表作为结果。与批处理查询相比，连续查询从不终止，并且结果表随着输入表的变化而更新。在任何时候，连续查询的结果在语义上等同于在批处理模式下输入表执行相同查询语句的结果快照。

如下，我们展示了 `clicks` 表上的两个示例查询，这个表根据流式的点击事件生成。

第一个查询是一个简单的 `GROUP-BY COUNT` 聚合查询。它根据 `user` 字段对 `clicks` 表进行分组，并统计访问的 URL 的数量。下面的图显示了当 `clicks` 表有数据新增时，查询是如何处理的。

{{< img alt="Continuous Non-Windowed Query" src="/fig/table-streaming/query-groupBy-cnt.png" width="90%">}}

当查询开始，`clicks` 表(左侧)是空的。当第一行数据被插入到 `clicks` 表时，查询开始计算结果表。第一条数据 `[Mary,./home]` 插入后，结果表(右侧，顶部)为一行数据 `[Mary, 1]` 。当第二条数据 `[Bob, ./cart]` 插入到 `clicks` 表时，会更新结果表并插入了一行新数据 `[Bob, 1]`。第三条数据 `[Mary, ./prod?id=1]` 将对已生成的行进行更新，`[Mary, 1]` 更新成 `[Mary, 2]`。最后，当第四条数据插入 `clicks` 表时，会在结果表中插入第三行数据 `[Liz, 1]`。



第二条查询与第一条类似， `clicks` 除了根据 `user` 属性分组外，还加入一[小时级滚动窗口]({{< ref "docs/dev/table/sql/overview" >}}#group-windows)条件，然后计算 url 数量(基于时间的计算，例如基于特定[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})的窗口，后面会讨论)。同样，该图显示了不同时间点的输入和输出，来展示动态表的变化特性。

{{< img alt="Continuous Group-Window Query" src="/fig/table-streaming/query-groupBy-window-cnt.png" width="100%">}}

与前面一样，左边显示了输入表 `clicks`。当查询开始，将持续不断在每小时计算出结果并更新结果表。时间戳在 `12:00:00` 和 `12:59:59` 之间，clicks表包含四行带有时间戳(`cTime`)的数据。从这个输入计算出两个结果行(每个 `user` 一个)，并将它们添加到结果表中。对于 `13:00:00` 和 `13:59:59` 之间为下一个窗口，该窗口`clicks` 表中有三行数据，这时计算出的两行数据添加到结果表。随着时间的推移，结果表会随着更多的数据被添加到 `click` 而发生更新。



### 更新和追加查询

虽然这两个示例查询看起来非常相似(都是分组统计)，但它们在一个重要方面不同:
- 第一个查询更新先前输出的结果，即结果表的 changelog 流包含 `INSERT` 和 `UPDATE` 操作。
- 第二个查询只附加到结果表，即结果表的 changelog 流只包含 `INSERT` 操作。

一个查询不管是生成`append-only`表还是更新表会带来一些影响:
- 产生更新变化的查询通常必须维护更多的状态(请参阅以下部分)。
- 将 append-only 表转换为流和将更新表转换为流是不同的(参阅[表到流的转换](#table-to-stream-conversion)章节)。

### 查询限制

许多(但不是全部)语义上有效的查询可以在流上被连续查询处理。有些查询代价太高而无法计算，这可能是由于它们需要维护的状态大小或计算更新代价太高。

- **状态大小：** 连续查询在无界流上计算，通常应该运行数周或数月。因此，连续查询处理的数据总量可能非常大。当查询必须更新先前输出的结果时需要维护所有输出的行，以便能够更新它们。例如，第一个查询示例需要存储每个用户的 URL 计数，以便能够增加该计数并在输入表接收新数据时发送新结果。如果只跟踪已注册的用户，则要维护的计数数量可能不会太高。但是，如果未注册的用户分配了一个惟一的用户名，那么要维护的计数数量将随着时间增长，并可能最终导致查询失败。

```sql
SELECT user, COUNT(url)
FROM clicks
GROUP BY user;
```

- **计算更新：** 有些查询需要重新计算和更新大量已输出的结果行，即使只添加或更新一条输入记录。显然，这样的查询不适合作为连续查询执行。下面的查询就是一个例子，它根据最后一次单击的时间为每个用户计算一个 `RANK`。一旦 `click` 表接收到一个新行，用户的 `lastAction` 就会更新，并必须计算一个新的排名。然而，由于两行不能具有相同的排名，所以所有较低排名的行也需要更新。

```sql
SELECT user, RANK() OVER (ORDER BY lastAction)
FROM (
  SELECT user, MAX(cTime) AS lastAction FROM clicks GROUP BY user
);
```

[查询配置]({{< ref "docs/dev/table/config" >}})章节讨论了控制连续查询执行的参数。一些参数可以用来在维持状态的大小和获得结果的准确性之间做取舍。



表到流的转换
--------------------------

动态表可以像普通数据库表一样通过 `INSERT`、`UPDATE` 和 `DELETE` 来不断修改。它可能是一个只有一行、不断更新的表，也可能是一个 insert-only 的表，没有 `UPDATE` 和 `DELETE` 修改，或者介于两者之间的其他表。

当将动态表转换为流或将其写入外部系统时，这些变化需要进行编码。Flink的 Table API 和 SQL 支持三种方式来编码一个动态表的变化:

* **Append-only 流：** 只有 `INSERT` 操作的动态表将通过输出插入的行转换为流。
* **Retract 流：** retract 流包含两种类型的 message： *add messages* 和 *retract messages* 。动态表通过编码被转换为 retract 流，会将`INSERT` 操作编码为 add message；将 `DELETE` 操作编码为 retract message；将 `UPDATE` 操作分为两部分，要被更新的行(先前)编码为 retract message 和将要更新成的行(新)编码为 add message。下图显示了将动态表转换为 retract 流的过程。

{{< img alt="Dynamic tables" src="/fig/table-streaming/undo-redo-mode.png" width="85%" >}}


* **Upsert 流:** upsert 流包含两种类型的 message： *upsert messages* 和*delete messages*。转换为 upsert 流的动态表需要(可能是组合的)唯一键。带有唯一键的动态表通过编码转为流，会将 `INSERT` 和 `UPDATE` 操作编码为 upsert message，将 `DELETE` 操作编码为 delete message 。消费流的算子需要知道唯一键的属性，以便正确地应用 message。与 retract 流的主要区别在于 `UPDATE` 操作是用单个 message 编码的，因此效率更高。下图显示了将动态表转换为 upsert 流的过程。

{{< img alt="Dynamic tables" src="/fig/table-streaming/redo-mode.png" width="85%" >}}


在[通用概念]({{< ref "docs/dev/table/common" >}}#convert-a-table-into-a-datastream)中讨论了将动态表转换为 `DataStream` 的 API。请注意，在将动态表转换为 `DataStream` 时，只支持 append 流和 retract 流。在 [TableSources 和 TableSinks]({{< ref "docs/dev/table/sourcesSinks">}}#dynamic-table-sink) 章节讨论向外部系统输出动态表的 `TableSink` 接口。

{{< top >}}
