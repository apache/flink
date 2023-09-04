---
title: "Quickstart for Flink OLAP"
weight: 91
type: docs
aliases:
- /dev/table/olapQuickstart.html
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

# Introduction

Flink OLAP has already added to [Apache Flink Roadmap](https://flink.apache.org/roadmap/). It means Flink can not only support streaming and batch computing, but also support OLAP(On-Line Analytical Processing). This page will show how to quickly set up a Flink OLAP service, and will introduce some best practices.

## Architecture

The Flink OLAP service consists of three parts: Client, Flink SQL Gateway, Flink Session Cluster.

* **Client**: Could be any client that can interact with Flink SQL Gateway, such as  SQL client, Flink JDBC driver and so on.
* **Flink SQL Gateway**: The SQL Gateway provides an easy way to submit the Flink Job, look up the metadata, and analyze table stats.
* **Flink Session Cluster**: We choose session clusters to run OLAP queries, mainly to avoid the overhead of cluster startup.

## Advantage

* **Massively Parallel Processing**
  * Flink OLAP runs naturally as an MPP(Massively Parallel Processing) system, which supports low-latency ad-hoc queries
* **Reuse Connectors**
  * Flink OLAP can reuse rich connectors in Flink ecosystem.
* **Unified Engine**
  * Unified computing engine for Streaming/Batch/OLAP.

# Deploying in Local Mode

## Downloading Flink

The same as [Local Installation]({{< ref "docs/try-flink/local_installation" >}}). Flink runs on all UNIX-like environments, i.e. Linux, Mac OS X, and Cygwin (for Windows). We need to have at least Java 11 installed, Java 17 is more recommended in OLAP scenario. To check the Java version installed, type in your terminal:

```
java -version
```

Next, [Download](https://flink.apache.org/downloads/) the latest binary release of Flink, then extract the archive:

```
tar -xzf flink-*.tgz
```

## Starting a local cluster

To start a local cluster, run the bash script that comes with Flink:

```
./bin/start-cluster.sh
```

You should be able to navigate to the web UI at localhost:8081 to view the Flink dashboard and see that the cluster is up and running.

## Start a SQL Client CLI

You can start the CLI with an embedded gateway by calling:

```
./bin/sql-client.sh
```

## Running Queries

You could simply execute queries in CLI and retrieve the results.

```
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '100000'
);

SELECT buyer, SUM(price) AS total_cost
FROM Orders
GROUP BY  buyer
ORDER BY  total_cost LIMIT 3;
```

And then you could find job detail information in web UI at localhost:8081.

# Deploying in Production

This section guides you through setting up a production ready Flink OLAP service.

## Cluster Deployment

In production, we recommend to use Flink Session Cluster, Flink SQL Gateway and Flink JDBC Driver to build an OLAP service.

### Session Cluster

For Flink Session Cluster, we recommend to deploy Flink on native Kubernetes using session mode. Kubernetes is a popular container-orchestration system for automating computer application deployment, scaling, and management. By deploying on native Kubernetes, Flink Session Cluster is able to dynamically allocate and de-allocate TaskManagers. For more information, please refer to [Native Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes">}}).

### SQL Gateway

For Flink SQL Gateway, we recommend deploying it as a stateless microservice and register this on the service discovery component.  For more information, please refer to the [SQL Gateway Overview]({{< ref "docs/dev/table/sql-gateway/overview">}}).

### Flink JDBC Driver

When submitting queries to SQL Gateway, we recommend using Flink JDBC Driver since it provides low-level connection management. When used in production, we need to pay attention to reuse the JDBC connection to avoid frequently creating/closing sessions in the Gateway. For more information, please refer to the [Flink JDBC Driver]({{{<ref "docs/dev/table/jdbcDriver">}}}).

## Datasource Configurations

### Catalogs

In OLAP scenario, we recommend using FileCatalogStore in the catalog configuration introduced in [FLIP-295](https://cwiki.apache.org/confluence/display/FLINK/FLIP-295%3A+Support+lazy+initialization+of+catalogs+and+persistence+of+catalog+configurations). As a long running service, Flink OLAP cluster's catalog information will not change frequently and can be re-used cross sessions. For more information, please refer to the [Catalog Store]({{< ref "docs/dev/table/catalogs#catalog-store">}}).

### Connectors

Both Session Cluster and SQL Gateway rely on connectors to analyze table stats and read data from the configured data source. To add connectors, please refer to the [Connectors and Formats]({{< ref "docs/connectors/table/overview">}}).

## Cluster Configurations

In OLAP scenario, we picked out a few configurations that can help improve user usability and query performance.

### SQL&Table Options

| Parameters                           | Default | Recommended |
|:-------------------------------------|:--------|:------------|
| table.optimizer.join-reorder-enabled | false   | true        |
| pipeline.object-reuse                | false   | true        |

### Runtime Options

| Parameters                   | Default                | Recommended                                                                                                                               |
|:-----------------------------|:-----------------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| execution.runtime-mode       | STREAMING              | BATCH                                                                                                                                     |
| execution.batch-shuffle-mode | ALL_EXCHANGES_BLOCKING | ALL_EXCHANGES_PIPELINED                                                                                                                   |
| env.java.opts.all            | {default value}        | {default value} -XX:PerMethodRecompilationCutoff=10000 -XX:PerBytecodeRecompilationCutoff=10000-XX:ReservedCodeCacheSize=512M -XX:+UseZGC |
| JDK Version                  | 11                     | 17                                                                                                                                        |

We strongly recommend using JDK17 with ZGC in OLAP scenario in order to provide zero gc stw and solve the issue described in [FLINK-32746](https://issues.apache.org/jira/browse/FLINK-32746).

### Scheduling Options

| Parameters                                               | Default           | Recommended       |
|:---------------------------------------------------------|:------------------|:------------------|
| jobmanager.scheduler                                     | Default           | Default           |
| jobmanager.execution.failover-strategy                   | region            | full              |
| restart-strategy.type                                    | (none)            | disable           |
| jobstore.type                                            | File              | Memory            |
| jobstore.max-capacity                                    | Integer.MAX_VALUE | 500               |

We would like to highlight the usage of `PipelinedRegionSchedulingStrategy`. Since many OLAP queries will have blocking edges in their jobGraph.

### Network Options

| Parameters                          | Default    | Recommended    |
|:------------------------------------|:-----------|:---------------|
| rest.server.numThreads              | 4          | 32             |
| web.refresh-interval                | 3000       | 300000         |
| pekko.framesize                     | 10485760b  | 104857600b     |

### ResourceManager Options

| Parameters                           | Default   | Recommended    |
|:-------------------------------------|:----------|:---------------|
| kubernetes.jobmanager.replicas       | 1         | 2              |
| kubernetes.jobmanager.cpu.amount     | 1.0       | 16.0           |
| jobmanager.memory.process.size       | (none)    | 65536m         |
| jobmanager.memory.jvm-overhead.max   | 1g        | 6144m          |
| kubernetes.taskmanager.cpu.amount    | (none)    | 16             |
| taskmanager.numberOfTaskSlots        | 1         | 32             |
| taskmanager.memory.process.size      | (none)    | 65536m         |
| taskmanager.memory.managed.size      | (none)    | 65536m         |

We prefer to use large taskManager pods in OLAP since this can put more computation in local and reduce network/deserialization/serialization overhead. Meanwhile, since JobManager is a single point of calculation in OLAP scenario, we also prefer large pod.

# Future Work
There is a big margin for improvement in Flink OLAP, both in usability and query performance, and we trace all of them in underlying tickets.
- https://issues.apache.org/jira/browse/FLINK-25318
- https://issues.apache.org/jira/browse/FLINK-32898

Furthermore, we are adding relevant OLAP benchmarks to the Flink repository such as [flink-benchmarks](https://github.com/apache/flink-benchmarks).