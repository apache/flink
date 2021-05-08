---
title: "Legacy Features"
weight: 1002
type: docs
aliases:
  - /dev/table/streaming/legacy.html
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

# Legacy Features

As Flink SQL has matured there are some features that have been replaced with more modern and better functioning substitutes.
These legacy features remain documented here for those users that have not yet or are unable to, upgrade to the more modern variant.

# Temporal Table Function

The temporal table function is the legacy way of defining something akin to a [versioned table]({{< ref "docs/dev/table/concepts/versioned_tables" >}})
that can be used in a temporal table join.
Please define temporal joins using [versioned tables]({{< ref "docs/dev/table/concepts/versioned_tables" >}}) in new queries.

Unlike a versioned table, temporal table functions can only be defined on top of append-only streams 
&mdash; it does not support changelog inputs.
Additionally, a temporal table function cannot be defined in pure SQL DDL. 

#### Defining a Temporal Table Function

Temporal table functions can be defined on top of append-only streams using the [Table API]({{< ref "docs/dev/table/tableApi" >}}).
The table is registered with one or more key columns, and a time attribute used for versioning.

Suppose we have an append-only table of currency rates that we would like to 
register as a temporal table function.

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

Using the Table API, we can register this stream using `currency` for the key and `update_time` as 
the versioning time attribute.

{{< tabs "066b6695-5bc3-4d7a-9033-ff6b1d14b3a1" >}}
{{< tab "Java" >}}
```java
TemporalTableFunction rates = tEnv
    .from("currency_rates").
    .createTemporalTableFunction("update_time", "currency");
 
tEnv.registerFunction("rates", rates);                                                        
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
rates = tEnv
    .from("currency_rates").
    .createTemporalTableFunction("update_time", "currency")
 
tEnv.registerFunction("rates", rates)
```
{{< /tab >}}
{{< /tabs >}}

#### Temporal Table Function Join

Once defined, a temporal table function is used as a standard [table function]({{< ref "/docs/dev/table/functions/udfs" >}}#table-functions).
Append-only tables (left input/probe side) can join with a temporal table (right input/build side),
i.e., a table that changes over time and tracks its changes, to retrieve the value for a key as it was at a particular point in time.

Consider an append-only table `orders` that tracks customers' orders in different currencies.

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

Given these tables, we would like to convert orders to a common currency &mdash; USD.

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
{{< /tabs >}}

{{< top >}}
