---
title: Getting Started
icon: <i class="fa fa-rocket title appetizer" aria-hidden="true"></i>
bold: true
bookCollapseSection: true
weight: 1
aliases:
  - /docs/getting-started/
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

# Getting Started with Apache Flink

Welcome to Apache Flink! Choose a path based on your goals:

## Choose Your Path

| Tutorial | Best For | Prerequisites |
|----------|----------|---------------|
| [First Steps]({{< ref "docs/getting-started/local_installation" >}}) | **Everyone** - Start here to install Flink | Java 11/17/21 or Docker |
| [Flink SQL Tutorial]({{< ref "docs/getting-started/quickstart-sql" >}}) | Data analysts, SQL users | Flink cluster running |
| [Table API Tutorial]({{< ref "docs/getting-started/table_api" >}}) | Java/Scala developers building streaming pipelines | Docker, Maven |
| [Flink Operations Playground]({{< ref "docs/getting-started/flink-operations-playground" >}}) | DevOps, platform engineers | Docker |

## Quick Overview

- **[First Steps]({{< ref "docs/getting-started/local_installation" >}})**: Install Flink locally or via Docker and verify it's running. This is the starting point for everyone.

- **[Flink SQL Tutorial]({{< ref "docs/getting-started/quickstart-sql" >}})**: Learn Flink through interactive SQL queries. No coding required - just use the SQL Client to query streaming data.

- **[Table API Tutorial]({{< ref "docs/getting-started/table_api" >}})**: Build a complete streaming pipeline that reads from Kafka, processes data, writes to MySQL, and visualizes results in Grafana.

- **[Flink Operations Playground]({{< ref "docs/getting-started/flink-operations-playground" >}})**: Learn to operate Flink in production - failure recovery, scaling, upgrades, and monitoring.

## After Getting Started

Once you've completed a tutorial, explore:

- [Concepts]({{< ref "docs/concepts/overview" >}}): Understand Flink's core concepts
- [Learn Flink]({{< ref "docs/learn-flink/overview" >}}): Deep-dive into Flink's programming model
- [Flink SQL]({{< ref "docs/sql/overview" >}}): Complete SQL reference and tools
