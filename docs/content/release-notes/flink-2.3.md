---
title: "Release Notes - Flink 2.3"
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

# Release notes - Flink 2.3

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 2.2 and Flink 2.3. Please read these notes carefully if you are
planning to upgrade your Flink version to 2.3.


### Core

#### Set security.ssl.algorithms default value to modern cipher suite

### [FLINK-39022](https://issues.apache.org/jira/browse/FLINK-39022)

A JDK update (affecting JDK 11.0.30+, 17.0.18+, 21.0.10+, and 24+) disabled `TLS_RSA_*` cipher suites.
This was done to support forward-secrecy (RFC 9325) and comply with the IETF Draft on *Deprecating Obsolete Key Exchange Methods in TLS*.

To support these and future JDK versions, the default value for the Flink configuration option `security.ssl.algorithms` has been changed to a modern, widely available cipher suite:

`TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`

This default provides strong security and wide compatibility. You can customize the cipher suites using the `security.ssl.algorithms` configuration option if your environment has different requirements. 
If these cipher suites are not supported on your setup, you will see that Flink processes will not be able to connect to each other.

### SQL & Table API

#### Drop redundant watermark assigners during stream optimization

##### [FLINK-14621](https://issues.apache.org/jira/browse/FLINK-14621)

The streaming optimizer now removes `WatermarkAssigner` operators whose watermarks are not consumed by any
downstream operator (for example, plain projections, filters, or processing-time-only pipelines that never
reach a windowed aggregate, an event-time join, an event-time temporal sort, or a `CURRENT_WATERMARK(...)`
SQL call). Pipelines that do consume watermarks &mdash; window aggregates, event-time joins, rowtime sinks,
`CURRENT_WATERMARK` &mdash; are unaffected.

The optimization is enabled by default and can be turned off via the new
`table.optimizer.redundant-watermark-assigner-remove.enabled` configuration option.

Because the rewrite changes the operator topology, jobs that restore from a savepoint **without** a
compiled plan ([FLIP-190](https://cwiki.apache.org/confluence/display/FLINK/FLIP-190%3A+Support+Version+Upgrades+for+Table+API+%26+SQL+Programs))
may need `--allowNonRestoredState` on the first restart after upgrading to 2.3, or can disable the rule
with the option above. Compiled-plan jobs are not affected because the plan is frozen at compile time.
