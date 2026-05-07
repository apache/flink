---
title: "Release Notes - Flink 2.4"
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

# Release notes - Flink 2.4

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 2.3 and Flink 2.4. Please read these notes carefully if you are
planning to upgrade your Flink version to 2.4.

### Table & SQL

#### Strict UTF-8 validation in CAST(BYTES AS STRING)

`CAST(value AS STRING)` (and the equivalent `VARCHAR`/`CHAR(n)` targets) now validates that the input bytes are well-formed UTF-8 when the source type is `BINARY`, `VARBINARY`, or `BYTES`. Invalid byte sequences fail the job with a `TableRuntimeException` instead of being silently substituted with the Unicode replacement character `U+FFFD` as before.

Migration options:

- Use `MAKE_VALID_UTF8(bytes)` to keep the lenient decode (replaces invalid sequences with `U+FFFD`).
- Use `TRY_CAST(bytes AS STRING)` to return `NULL` on invalid input.
- Filter at ingest with `WHERE IS_VALID_UTF8(bytes)` to route well-formed records, or `WHERE NOT IS_VALID_UTF8(bytes)` for a dead-letter sink.
- Restore the prior behavior across the job by setting `table.exec.legacy-bytes-to-string-cast` to `true`.
