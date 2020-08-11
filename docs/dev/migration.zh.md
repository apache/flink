---
title: "API 迁移指南"
nav-parent_id: dev
nav-pos: 100
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

* This will be replaced by the TOC
{:toc}

有关从 Flink 1.3 之前的旧版本进行迁移的信息，请参阅[旧的迁移指南](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/migration.html)。

<a name="migrating-from-flink-13-to-flink-17"></a>

## 从 Flink 1.3+ 迁移到 Flink 1.7

<a name="api-changes-for-serializer-snapshots"></a>

### Serializer snapshots 的 API 变更

这主要与为其状态实现自定义 `TypeSerializer`s 的用户相关。

现在已弃用旧的 `TypeSerializerConfigSnapshot` 抽象，并在将来完全删除，以使用新的 `TypeSerializerSnapshot`。有关如何迁移的详细信息和指南，请参阅[从 Flink 1.7 之前弃用的 serializer snapshot APIs 迁移]({% link dev/stream/state/custom_serialization.zh.md %}#migrating-from-deprecated-serializer-snapshot-apis-before-flink-17).

{% top %}
