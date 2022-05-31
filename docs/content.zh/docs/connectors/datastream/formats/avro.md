---
title:  "Avro"
weight: 4
type: docs
aliases:
- /zh/dev/connectors/formats/avro.html
- /zh/apis/streaming/connectors/formats/avro.html
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

<a name="avro-format"></a>

# Avro format

Flink 内置支持 [Apache Avro](http://avro.apache.org/) 格式。在 Flink 中将更容易地读写基于 Avro schema 的 Avro 数据。
Flink 的序列化框架可以处理基于 Avro schemas 生成的类。为了能够使用 Avro format，需要在自动构建工具（例如 Maven 或 SBT）中添加如下依赖到项目中。

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

如果读取 Avro 文件数据，你必须指定 `AvroInputFormat`。

**示例**：

```java
AvroInputFormat<User> users = new AvroInputFormat<User>(in, User.class);
DataStream<User> usersDS = env.createInput(users);
```

注意，`User` 是一个通过 Avro schema生成的 POJO 类。Flink 还允许选择 POJO 中字符串类型的键。例如：

```java
usersDS.keyBy("name");
```


注意，在 Flink 中可以使用 `GenericData.Record` 类型，但是不推荐使用。因为该类型的记录中包含了完整的 schema，导致数据非常密集，使用起来可能很慢。

Flink 的 POJO 字段选择也适用于从 Avro schema 生成的 POJO 类。但是，只有将字段类型正确写入生成的类时，才能使用。如果字段是 `Object` 类型，则不能将该字段用作 join 键或 grouping 键。
在 Avro 中如 `{"name": "type_double_test", "type": "double"},` 这样指定字段是可行的，但是如 (`{"name": "type_double_test", "type": ["double"]},`) 这样指定包含一个字段的复合类型就会生成 `Object` 类型的字段。注意，如 (`{"name": "type_double_test", "type": ["null", "double"]},`) 这样指定 nullable 类型字段也是可能产生 `Object` 类型的!
