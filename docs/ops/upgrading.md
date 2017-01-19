---
title: "Upgrading Jobs and Flink Versions"
nav-parent_id: setup
nav-pos: 15
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

* ToC
{:toc}

## Upgrading Flink Streaming Applications

  - Savepoint, stop/cancel, start from savepoint
  - Atomic Savepoint and Stop (link to JIRA issue)

  - Limitations: Breaking chaining behavior (link to Savepoint section)
  - Encourage using `uid(...)` explicitly for every operator

## Upgrading the Flink Framework Version

  - Either "in place" : Savepoint -> stop/cancel -> shutdown cluster -> start new version -> start job 
  - Another cluster variant : Savepoint -> resume in other cluster -> "flip switch" -> shutdown old cluster

## Compatibility Table

Savepoints are compatible across Flink versions as indicated by the table below:
                             
| Created with \ Resumed With | 1.1.x | 1.2.x |
| ---------------------------:|:-----:|:-----:|
| 1.1.x                       |   X   |   X   |
| 1.2.x                       |       |   X   |



## Special Considerations for Upgrades from Flink 1.1.x to Flink 1.2.x

  - The parallelism of the Savepoint in Flink 1.1.x becomes the maximum parallelism in Flink 1.2.x.
  - Increasing the parallelism for upgraded jobs is not possible out of the box.


