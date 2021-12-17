---
title: "函数"
weight: 1
type: docs
aliases:
  - /zh/dev/table/functions/
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

# 函数

Flink 允许用户在 Table API 和 SQL 中使用函数进行数据的转换。



函数类型
------------------

Flink 中的函数有两个划分标准。

一个划分标准是：系统（内置）函数和 Catalog 函数。系统函数没有名称空间，只能通过其名称来进行引用。
Catalog 函数属于 Catalog 和数据库，因此它们拥有 Catalog 和数据库命名空间。
用户可以通过全/部分限定名（`catalog.db.func` 或 `db.func`）或者函数名
来对 Catalog 函数进行引用。

另一个划分标准是：临时函数和持久化函数。
临时函数始终由用户创建，它容易改变并且仅在会话的生命周期内有效。
持久化函数不是由系统提供，就是存储在 Catalog 中，它在会话的整个生命周期内都有效。

这两个划分标准给 Flink 用户提供了 4 种函数：

1. 临时性系统函数
2. 系统函数  
3. 临时性 Catalog 函数
4. Catalog 函数

请注意，系统函数始终优先于 Catalog 函数解析，临时函数始终优先于持久化函数解析，
函数解析优先级如下所述。

函数引用
---------------------

用户在 Flink 中可以通过精确、模糊两种引用方式引用函数。

### 精确函数引用

精确函数引用允许用户跨 Catalog，跨数据库调用 Catalog 函数。
例如：`select mycatalog.mydb.myfunc(x) from mytable` 和 `select mydb.myfunc(x) from mytable`。

仅 Flink 1.10 以上版本支持。 

### 模糊函数引用

在模糊函数引用中，用户只需在 SQL 查询中指定函数名，例如： `select myfunc(x) from mytable`。


函数解析顺序
-------------------------

当函数名相同，函数类型不同时，函数解析顺序才有意义。
例如：当有三个都名为 "myfunc" 的临时性 Catalog 函数，Catalog 函数，和系统函数时，
如果没有命名冲突，三个函数将会被解析为一个函数。

### 精确函数引用

由于系统函数没有命名空间，Flink 中的精确函数引用必须
指向临时性 Catalog 函数或 Catalog 函数。

解析顺序如下：

1. 临时性 catalog 函数
2. Catalog 函数

### 模糊函数引用

解析顺序如下：

1. 临时性系统函数
2. 系统函数
3. 临时性 Catalog 函数, 在会话的当前 Catalog 和当前数据库中
4. Catalog 函数, 在会话的当前 Catalog 和当前数据库中
