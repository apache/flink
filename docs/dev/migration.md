---
title: "API Migration Guides"
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

See the [older migration
guide](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/migration.html)
for information about migrating from older versions than Flink 1.3.

## Migrating from Flink 1.3+ to Flink 1.7

### API changes for serializer snapshots

This would be relevant mostly for users implementing custom `TypeSerializer`s for their state.

The old `TypeSerializerConfigSnapshot` abstraction is now deprecated, and will be fully removed in the future
in favor of the new `TypeSerializerSnapshot`. For details and guides on how to migrate, please see
[Migrating from deprecated serializer snapshot APIs before Flink 1.7]({% link dev/stream/state/custom_serialization.md %}#migrating-from-deprecated-serializer-snapshot-apis-before-flink-17).

{% top %}
