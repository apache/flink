---
title: "Release Notes - Flink 1.7"
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

These release notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Flink 1.6 and Flink 1.7. Please read these notes carefully if you are planning to upgrade your Flink version to 1.7.

<!-- Should be removed once FLINK-10911 is fixed -->
### Scala shell does not work with Scala 2.12

Flink's Scala shell does not work with Scala 2.12.
Therefore, the module `flink-scala-shell` is not being released for Scala 2.12.

See [FLINK-10911](https://issues.apache.org/jira/browse/FLINK-10911) for more details.  

{% top %}
