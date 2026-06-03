---
title: "INSERT 语句"
weight: 7
type: docs
aliases:
  - /zh/docs/sql/reference/insert/
  - /zh/dev/table/sql/insert.html
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

# INSERT 语句



INSERT 语句用来向表中添加行。

## 执行 INSERT 语句

{{< tabs "execute" >}}
{{< tab "Java" >}}

单条 INSERT 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 INSERT 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。
多条 INSERT 语句，使用 `TableEnvironment` 中的 `createStatementSet` 创建一个 `StatementSet` 对象，然后使用 `StatementSet` 中的 `addInsertSql()` 方法添加多条 INSERT 语句，最后通过 `StatementSet` 中的 `execute()` 方法来执行。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 INSERT 语句，或者通过 `StatementSet` 执行多条 INSERT 语句。
{{< /tab >}}
{{< tab "Scala" >}}

单条 INSERT 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 INSERT 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。
多条 INSERT 语句，使用 `TableEnvironment` 中的 `createStatementSet` 创建一个 `StatementSet` 对象，然后使用 `StatementSet` 中的 `addInsertSql()` 方法添加多条 INSERT 语句，最后通过 `StatementSet` 中的 `execute()` 方法来执行。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 INSERT 语句，或者通过 `StatementSet` 执行多条 INSERT 语句。

{{< /tab >}}
{{< tab "Python" >}}

单条 INSERT 语句，可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行。`execute_sql()` 方法执行 INSERT 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。
多条 INSERT 语句，使用 `TableEnvironment` 中的 `create_statement_set` 创建一个 `StatementSet` 对象，然后使用 `StatementSet` 中的 `add_insert_sql()` 方法添加多条 INSERT 语句，最后通过 `StatementSet` 中的 `execute()` 方法来执行。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 INSERT 语句，或者通过 `StatementSet` 执行多条 INSERT 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/sql/interfaces/sql-client" >}}) 中执行 INSERT 语句

以下的例子展示了如何在 SQL CLI 中执行一条 INSERT 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "77ed5a01-effa-432c-b089-f922c3964c88" >}}
{{< tab "Java" >}}
```java
TableEnvironment tEnv = TableEnvironment.create(...);

// 注册一个 "Orders" 源表，和 "RubberOrders" 结果表
tEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
tEnv.executeSql("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)");

// 运行一条 INSERT 语句，将源表的数据输出到结果表中
TableResult tableResult1 = tEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
// 通过 TableResult 来获取作业状态
System.out.println(tableResult1.getJobClient().get().getJobStatus());

//----------------------------------------------------------------------------
// 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)");

// 运行多条 INSERT 语句，将原表数据输出到多个结果表中
StatementSet stmtSet = tEnv.createStatementSet();
// `addInsertSql` 方法每次只接收单条 INSERT 语句
stmtSet.addInsertSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
stmtSet.addInsertSql(
  "INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'");
// 执行刚刚添加的所有 INSERT 语句
TableResult tableResult2 = stmtSet.execute();
// 通过 TableResult 来获取作业状态
System.out.println(tableResult1.getJobClient().get().getJobStatus());

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tEnv = TableEnvironment.create(...)

// 注册一个 "Orders" 源表，和 "RubberOrders" 结果表
tEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
tEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

// 运行一个 INSERT 语句，将源表的数据输出到结果表中
val tableResult1 = tEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
// 通过 TableResult 来获取作业状态
println(tableResult1.getJobClient().get().getJobStatus())

//----------------------------------------------------------------------------
// 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)");

// 运行多个 INSERT 语句，将原表数据输出到多个结果表中
val stmtSet = tEnv.createStatementSet()
// `addInsertSql` 方法每次只接收单条 INSERT 语句
stmtSet.addInsertSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
stmtSet.addInsertSql(
  "INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'")
// 执行刚刚添加的所有 INSERT 语句
val tableResult2 = stmtSet.execute()
// 通过 TableResult 来获取作业状态
println(tableResult1.getJobClient().get().getJobStatus())
  
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# 注册一个 "Orders" 源表，和 "RubberOrders" 结果表
table_env.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
table_env.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

# 运行一条 INSERT 语句，将源表的数据输出到结果表中
table_result1 = table_env \
    .executeSql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
# 通过 TableResult 来获取作业状态
print(table_result1.get_job_client().get_job_status())

#----------------------------------------------------------------------------
# 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
table_env.execute_sql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)")

# 运行多条 INSERT 语句，将原表数据输出到多个结果表中
stmt_set = table_env.create_statement_set()
# `add_insert_sql` 方法每次只接收单条 INSERT 语句
stmt_set \
    .add_insert_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
stmt_set \
    .add_insert_sql("INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'")
# 执行刚刚添加的所有 INSERT 语句
table_result2 = stmt_set.execute()
# 通过 TableResult 来获取作业状态
print(table_result2.get_job_client().get_job_status())


```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...);

Flink SQL> SHOW TABLES;
Orders
RubberOrders

Flink SQL> INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
[INFO] Submitting SQL update statement to the cluster...
[INFO] Table update statement has been successfully submitted to the cluster:
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## 将 SELECT 查询数据插入表中

通过 INSERT 语句，可以将查询的结果插入到表中，

### 语法

```sql

[EXECUTE] INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec] select_statement

part_spec:
  (part_col_name1=val1 [, part_col_name2=val2, ...])

```

**OVERWRITE**

`INSERT OVERWRITE` 将会覆盖表中或分区中的任何已存在的数据。否则，新数据会追加到表中或分区中。

**PARTITION**

`PARTITION` 语句应该包含需要插入的静态分区列与值。

**COLUMN LIST**

给定一个表 T(a INT, b INT, c INT)，Flink 支持 INSERT INTO T(c, b) SELECT x, y FROM S。
预期行为是 “x” 被写入 “c” 列，“y” 被写入 “b” 列，而 “a” 被设置为空值（假设 “a” 列可为空）。<br />
连接器开发人员在处理部分列更新时，如果希望避免用空值覆盖非目标列，可以从 {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/DynamicTableSink.java" name="DynamicTableSink$Context.getTargetColumns()" >}}
中获取用户插入语句指定的目标列信息，然后决定如何处理部分更新。

### 示例

```sql
-- 创建一个分区表
CREATE TABLE country_page_view (user STRING, cnt INT, date STRING, country STRING)
PARTITIONED BY (date, country)
WITH (...)

-- 追加行到该静态分区中 (date='2019-8-30', country='China')
INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- Insert语句的开头可以额外增加EXECUTE关键字,带EXECUTE关键字和不带是等价的
EXECUTE INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- 追加行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
INSERT INTO country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;

-- 覆盖行到静态分区 (date='2019-8-30', country='China')
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- 覆盖行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;
```

## 将值插入表中

通过 INSERT 语句，也可以直接将值插入到表中，

### 语法

```sql
[EXECUTE] INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name VALUES values_row [, values_row ...]

values_row:
    : (val1 [, val2, ...])
```

**OVERWRITE**

`INSERT OVERWRITE` 将会覆盖表中的任何已存在的数据。否则，新数据会追加到表中。

### 示例

```sql

CREATE TABLE students (name STRING, age INT, gpa DECIMAL(3, 2)) WITH (...);

EXECUTE INSERT INTO students
  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);

```
## 插入数据到多张表
`STATEMENT SET` 可以实现通过一个语句插入数据到多个表。

### 语法

```sql
EXECUTE STATEMENT SET
BEGIN
insert_statement;
...
insert_statement;
END;

insert_statement:
   <insert_from_select>|<insert_from_values>
```

### 示例

```sql

CREATE TABLE students (name STRING, age INT, gpa DECIMAL(3, 2)) WITH (...);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO students
  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
INSERT INTO students
  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
END;
```

## ON CONFLICT clause

When a query produces a changelog stream with an upsert key that differs from the sink table's primary key, multiple records with different upsert keys may map to the same primary key. The `ON CONFLICT` clause specifies how to resolve these primary key conflicts at the sink.

### When is ON CONFLICT required?

By default, Flink requires an explicit `ON CONFLICT` clause whenever the query's upsert key differs from the sink table's primary key. Without it, the query fails at planning time. This forces you to consider whether your query genuinely has a conflict scenario or whether there is a logic issue (e.g., a missing `GROUP BY`).

This check is controlled by the configuration option `table.exec.sink.require-on-conflict` (default: `true`). Setting it to `false` restores the legacy behavior where no `ON CONFLICT` clause was required, but may lead to non-deterministic results.

Alternatively, if you do not need consistency guarantees for conflicting keys, you can disable the sink upsert materializer entirely by setting `table.exec.sink.upsert-materialize` to `NONE`. This removes the materializer operator from the pipeline, so no buffering, compaction, or conflict resolution is performed. Records are passed directly to the sink as they arrive.

### Syntax

```sql
[EXECUTE] INSERT INTO [catalog_name.][db_name.]table_name
    select_statement
    ON CONFLICT conflict_action

conflict_action:
    DO NOTHING
  | DO ERROR
  | DO DEDUPLICATE
```

### Strategies

#### DO ERROR

Throws an exception at runtime if multiple records with different upsert keys map to the same primary key. Use this when you believe no real conflict exists — for example, the planner could not prove that the upsert key matches the primary key, but you know they are logically equivalent.

Buffered records are compacted on watermark progression before conflict checking, so transient disorder from changelog reordering does not cause false errors.

```sql
INSERT INTO product_orders
SELECT p.name, o.order_id
FROM orders o JOIN products p ON o.product_name = p.name
ON CONFLICT DO ERROR;
```

#### DO NOTHING

Keeps the first record that arrives for a given primary key and silently discards subsequent conflicting records. Use this when it is acceptable to drop duplicate primary key values from different upsert keys.

Like `DO ERROR`, this strategy uses watermark-based compaction before applying conflict resolution.

```sql
INSERT INTO product_orders
SELECT p.name, o.order_id
FROM orders o JOIN products p ON o.product_name = p.name
ON CONFLICT DO NOTHING;
```

#### DO DEDUPLICATE

{{< hint warning >}}
`DO DEDUPLICATE` maintains the full history of changes per primary key in state to support rollback on retraction. This results in significantly higher state usage compared to `DO ERROR` and `DO NOTHING`.
{{< /hint >}}

Maintains the full history of changes per primary key so that retractions can be correctly rolled back. This is the most correct strategy when true multi-source updates to the same primary key occur and correctness cannot be sacrificed.

```sql
INSERT INTO product_orders
SELECT p.name, o.order_id
FROM orders o JOIN products p ON o.product_name = p.name
ON CONFLICT DO DEDUPLICATE;
```

### How conflicts happen

A conflict occurs when the query's upsert key differs from the sink table's primary key. For example, consider a join whose result has an upsert key derived from the join condition, but the target table has a different primary key. Records from different upstream upsert keys can then collide on the same primary key in the sink.

Because retraction (`-U`) and update (`+U`) messages may travel different paths through the pipeline, they can arrive at the sink out of order. `DO ERROR` and `DO NOTHING` use watermark-based compaction to wait for a consistent set of changes before applying conflict resolution, preventing false positives from transient reordering.

### Watermark-based compaction

Changelog messages produced by operators such as joins can arrive at the sink out of order. A retraction (`-U`) for a row may arrive after a new insert (`+I`) for a different row that shares the same primary key, making it look like two active records exist for that key — a false conflict.

Watermark-based compaction solves this by buffering incoming records keyed by their primary key and upsert key. When a watermark advances, all buffered records with timestamps up to that watermark are compacted: matching insert and retraction pairs for the same upsert key cancel each other out (for example, `+I` and `-D`, or `-U` and `+U` pairs).

**Example.** Using the `orders JOIN products` query from above, suppose order 1 changes its product from `Laptop` to `Phone` while order 3 is also for `Laptop`. The join emits these changelog records:

```
+I[Laptop, 1]   -- upsert key: order_id=1
+I[Laptop, 3]   -- upsert key: order_id=3
-U[Laptop, 1]   -- upsert key: order_id=1  (retraction for order 1's old product)
+U[Phone,  1]   -- upsert key: order_id=1  (order 1 now maps to Phone)
```

Without compaction, after the first two `+I` records arrive the operator sees two active records for PK `Laptop` with different upsert keys (`order_id=1` and `order_id=3`) — a false conflict. With compaction, the operator waits for the watermark. The retraction `-U[Laptop, 1]` then cancels the earlier `+I[Laptop, 1]` (same upsert key `order_id=1`), leaving only `+I[Laptop, 3]` for PK `Laptop` — no conflict.

After compaction, if zero or one record remains per primary key, there is no conflict. If multiple records with different upsert keys still remain, a genuine conflict exists and is resolved by the chosen strategy (`DO ERROR` or `DO NOTHING`). `DO DEDUPLICATE` does not use watermark-based compaction; instead, it maintains the full history of changes in state to support correct rollback on retraction.

### Examples

```sql
-- Source and dimension tables
CREATE TABLE orders (
    order_id BIGINT,
    product_name STRING,
    quantity INT,
    PRIMARY KEY(order_id) NOT ENFORCED
) WITH (...);

CREATE TABLE products (
    name STRING,
    PRIMARY KEY(name) NOT ENFORCED
) WITH (...);

-- Sink table
CREATE TABLE product_orders (
    product_name STRING,
    last_order_id BIGINT,
    PRIMARY KEY(product_name) NOT ENFORCED
) WITH (...);

-- This join produces an upsert key that may differ from the sink's PK,
-- so ON CONFLICT is required.
INSERT INTO product_orders
SELECT p.name, o.order_id
FROM orders o JOIN products p ON o.product_name = p.name
ON CONFLICT DO NOTHING;
```

Given the following data in the source tables:

```
orders:                                    products:
+----------+--------------+----------+    +--------+
| order_id | product_name | quantity |    | name   |
+----------+--------------+----------+    +--------+
| 1        | Laptop       | 2        |    | Laptop |
| 2        | Phone        | 1        |    | Phone  |
| 3        | Laptop       | 5        |    +--------+
+----------+--------------+----------+
```

The join produces these changelog records for `product_orders`:

```
+I[Laptop, 1]  -- upsert key: order_id=1
+I[Phone,  2]  -- upsert key: order_id=2
+I[Laptop, 3]  -- upsert key: order_id=3  ← conflicts with order_id=1 on PK 'Laptop'
```

Two records with different upsert keys (`order_id=1` and `order_id=3`) target the same
primary key (`product_name='Laptop'`). This is the conflict each strategy resolves differently:

- **`DO ERROR`** — throws a runtime exception because two distinct upsert keys map to the same primary key.
- **`DO NOTHING`** — keeps the first record and discards the conflict:

  | product_name | last_order_id |
  |:-------------|:--------------|
  | Laptop       | 1             |
  | Phone        | 2             |

- **`DO DEDUPLICATE`** — accepts both; the last arriving value is visible:

  | product_name | last_order_id |
  |:-------------|:--------------|
  | Laptop       | 3             |
  | Phone        | 2             |

**What happens on retraction?** If order 3 is later deleted from the source, the join
emits a retraction `-D[Laptop, 3]`:

- **`DO NOTHING`** — the retraction has no effect because `(Laptop, 3)` was never written.
  The Laptop row remains with `last_order_id=1`.
- **`DO DEDUPLICATE`** — rolls back to the previous value. Laptop falls back to order 1,
  producing `{(Laptop, 1), (Phone, 2)}`. The full history kept in state enables this
  correct rollback.

{{< top >}}
