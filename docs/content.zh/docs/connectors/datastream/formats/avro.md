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
usersDS.keyBy("name")
```


注意，在 Flink 中可能会使用 `GenericData.Record` 类型，但是不推荐使用。由于该类型的记录中包含了完整的 schema，导致数据非常密集，使用起来可能很慢。

Flink 中在 Avro schema 生成的 POJO 类上也可以进行 POJO 字段选择。因此，只有在生成的类中正确写入字段类型时，才可以使用字段选择。如果一个字段类型是 `Object` 类型，你不可以选择该字段进行 join 或者 group by 操作。
在 Avro 中类似 `{"name": "type_double_test", "type": "double"},` 这样指定字段是可行的，但是类似 (`{"name": "type_double_test", "type": ["double"]},`) 这样指定字段，字段类型就会生成为 `Object` 类型。注意，类似 (`{"name": "type_double_test", "type": ["null", "double"]},`) 这样指定 nullable 类型字段，也是有可能的!
