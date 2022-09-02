---
title: "时态表函数"
weight: 5
type: docs
aliases:
  - /zh/dev/table/streaming/temporal_table_function.html
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

# 时态表函数

时态表函数提供了访问特定时间点对应版本的时态表数据。为了能够访问时态表的数据，必须要指定一个 [时间属性]({{<ref "docs/dev/table/concepts/time_attributes">}})用于确定时态表的版本。Flink 使用 [表值函数]({{<ref "docs/dev/table/functions/udfs" >}}#表值函数) 的 SQL 语法来使用时态表函数。

和时态表不同的是时态表函数不支持 changelog 类型的输入流，只能在 append-only 流之前定义。除此之外，时态表函数不能够完全使用 SQL DDL 定义。

## 定义时态表函数

时态表函数可以使用 [Table API]({{< ref "docs/dev/table/tableApi" >}}) 在 append-only 流之前定义，需要提供一个或者多个键列和一个用于确定版本的时间属性字段。

假定已经有了一个 append-only 类型的时态表 `currency_rates`，后面将使用这张表来注册一个时态表函数。

```sql
SELECT * FROM currency_rates;

update_time   currency   rate
============= =========  ====
09:00:00      Yen        102
09:00:00      Euro       114
09:00:00      USD        1
11:15:00      Euro       119
11:49:00      Pounds     108
```

使用 Table API 的方式，将 `currency` 作为键，`update_time` 作为版本控制的时间属性字段。

{{< tabs "066b6695-5bc3-4d7a-9033-ff6b1d14b3a1" >}}
{{< tab "Java" >}}
```java
TemporalTableFunction rates = tEnv
    .from("currency_rates")
    .createTemporalTableFunction("update_time", "currency");
 
tEnv.registerFunction("rates", rates);                                                        
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
rates = tEnv
    .from("currency_rates")
    .createTemporalTableFunction("update_time", "currency")
 
tEnv.registerFunction("rates", rates)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python Table API.
```
{{< /tab >}}
{{< /tabs >}}

## 时态表函数的 Join 

在定义之后，时态表函数就可以像标准的 [表值函数]({{<ref "docs/dev/table/functions/udfs" >}}#表值函数) 一样使用。
Append-only 表（left input/probe side）能够和一张时态表（right input/build side） Join，也就是说，一张表随着时间变化数据也在变化，但是它能够通过键获取到之前特定时间对应的值，通过这种方法来追踪数据的变化。

Append-only 表 `orders` 记录着消费者不同币种的订单。 

```sql
SELECT * FROM orders;

order_time amount currency
========== ====== =========
10:15        2    Euro
10:30        1    USD
10:32       50    Yen
10:52        3    Euro
11:04        5    USD
```

通过上面的表，将订单的货币都统一成 USD。

{{< tabs "7ec4efc6-41ae-42c1-a261-4a94dd3b44e0" >}}
{{< tab "SQL" >}}
```sql
SELECT
  SUM(amount * rate) AS amount
FROM
  orders,
  LATERAL TABLE (rates(order_time))
WHERE
  rates.currency = orders.currency
```
{{< /tab >}}
{{< tab "Java" >}}
```java
Table result = orders
    .joinLateral($("rates(order_time)"), $("orders.currency = rates.currency"))
    .select($("(o_amount * r_rate).sum as amount"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val result = orders
    .joinLateral($"rates(order_time)", $"orders.currency = rates.currency")
    .select($"(o_amount * r_rate).sum as amount"))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
