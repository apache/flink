---
title: "OLAP Quickstart"
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

# OLAP Quickstart

OLAP (OnLine Analysis Processing) is a key technology in the field of data analysis, it is generally used to perform complex queries on large data sets with latencies in seconds. Now Flink can not only support streaming and batch computing, but also supports users to deploy it as an OLAP computing service. This page will show you how to quickly set up a local Flink OLAP service, and will also introduce some best practices helping you deploy Flink OLAP service in production.

## Architecture Introduction
This chapter will introduce you to the overall architecture of Flink OLAP service and the advantages of using it.

### Architecture

Flink OLAP service consists of three parts: Client, Flink SQL Gateway and Flink Session Cluster.

* **Client**: Could be any client that can interact with [Flink SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}), such as [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}), [Flink JDBC Driver]({{< ref "docs/dev/table/jdbcDriver" >}}) and so on.
* **Flink SQL Gateway**: The SQL Gateway provides an easy way to parse the sql query, look up the metadata, analyze table stats, optimize the plan and submit JobGraphs to cluster.
* **Flink Session Cluster**: OLAP queries run on [session cluster]({{< ref "/docs/deployment/resource-providers/native_kubernetes#starting-a-flink-session-on-kubernetes" >}}), mainly to avoid the overhead of cluster startup.

{{< img src="/fig/olap-architecture.svg" alt="Illustration of Flink OLAP Architecture" width="85%" >}}

### Advantage

* **Massively Parallel Processing**
  * Flink OLAP runs naturally as a massively parallel processing system, which enables planners to easily adjust the job parallelism to fulfill queries' latency requirement under different data sizes.
* **Elastic Resource Management**
  * Flink's resource management supports min/max scaling, which means the session cluster can allocate the resource according to workload dynamically.
* **Reuse Connectors**
  * Flink OLAP can reuse the rich [Connectors]({{< ref "docs/connectors/table/overview" >}}) in Flink ecosystem.
* **Unified Engine**
  * Unified computing engine for Streaming/Batch/OLAP.

## Deploying in Local Mode

In this chapter, you will learn how to build Flink OLAP services locally.

### Downloading Flink

The same as [Local Installation]({{< ref "docs/try-flink/local_installation" >}}). Flink runs on all UNIX-like environments, i.e. Linux, Mac OS X, and Cygwin (for Windows). User need to have at __Java 11__ installed. To check the Java version installed, user can type in the terminal:

```
java -version
```

Next, [Download](https://flink.apache.org/downloads/) the latest binary release of Flink, then extract the archive:

```
tar -xzf flink-*.tgz
```

### Starting a local cluster

To start a local cluster, run the bash script that comes with Flink:

```
./bin/start-cluster.sh
```

You should be able to navigate to the web UI at http://localhost:8081 to view the Flink dashboard and see that the cluster is up and running.

### Start a SQL Client CLI

You can start the CLI with an embedded gateway by calling:

```
./bin/sql-client.sh
```

### Running Queries

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

And then you could find job detail information in web UI at http://localhost:8081.

## Deploying in Production

This section guides you through setting up a production ready Flink OLAP service.

### Client

#### Flink JDBC Driver

You should use Flink JDBC Driver when submitting queries to SQL Gateway since it provides low-level connection management. When used in production, you should pay attention to reuse the JDBC connection to avoid frequently creating/closing sessions in the Gateway and then reduce the E2E query latency. For detailed information, please refer to the [Flink JDBC Driver]({{ <ref "docs/dev/table/jdbcDriver"> }}).

### Cluster Deployment

In production, you should use Flink Session Cluster, Flink SQL Gateway to build an OLAP service.

#### Session Cluster

For Flink Session Cluster, you can deploy it on Native Kubernetes using session mode. Kubernetes is a popular container-orchestration system for automating computer application deployment, scaling, and management. By deploying on Native Kubernetes, Flink Session Cluster is able to dynamically allocate and de-allocate TaskManagers. For more information, please refer to [Native Kubernetes]({{ < ref "docs/deployment/resource-providers/native_kubernetes"> }}). Furthermore, you can config the option [slotmanager.number-of-slots.min]({{< ref "docs/deployment/config#slotmanager-number-of-slots-min" >}}) in session cluster. This will help you significantly reduce the cold start time of your query. For detailed information, please refer to [FLIP-362](https://cwiki.apache.org/confluence/display/FLINK/FLIP-362%3A+Support+minimum+resource+limitation).

#### SQL Gateway

For Flink SQL Gateway, you should deploy it as a stateless microservice and register the instance on service discovery component. Through this way, client can balance the query between instances easily. For more information, please refer to [SQL Gateway Overview]({{< ref "docs/dev/table/sql-gateway/overview">}}).

### Datasource Configurations

#### Catalogs

In OLAP scenario, you should configure `FileCatalogStore` provided by [Catalogs]({{< ref "docs/dev/table/catalogs">}}) as the catalog used by cluster. As a long-running service, Flink OLAP cluster's catalog information will not change frequently and should be re-used cross sessions to reduce the cold-start cost. For more information, please refer to the [Catalog Store]({{< ref "docs/dev/table/catalogs#catalog-store">}}).

#### Connectors

Both Session Cluster and SQL Gateway rely on connectors to analyze table stats and read data from the configured data source. To add connectors, please refer to the [Connectors]({{< ref "docs/connectors/table/overview">}}).

### Recommended Cluster Configurations

In OLAP scenario, appropriate configurations that can greatly help users improve the overall usability and query performance. Here are some recommended production configurations:

#### SQL&Table Options

| Parameters                                                                                                     | Default | Recommended |
|:---------------------------------------------------------------------------------------------------------------|:--------|:------------|
| [table.optimizer.join-reorder-enabled]({{<ref "docs/dev/table/config#table-optimizer-join-reorder-enabled">}}) | false | true |
| [pipeline.object-reuse]({{< ref "docs/deployment/config#pipeline-object-reuse" >}})                            | false | true |

#### Runtime Options

| Parameters                                                                                        | Default                | Recommended                                                                                                                               |
|:--------------------------------------------------------------------------------------------------|:-----------------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| [execution.runtime-mode]({{< ref "docs/deployment/config#execution-runtime-mode" >}})             | STREAMING              | BATCH                                                                                                                                     |
| [execution.batch-shuffle-mode]({{< ref "docs/deployment/config#execution-batch-shuffle-mode" >}}) | ALL_EXCHANGES_BLOCKING | ALL_EXCHANGES_PIPELINED                                                                                                                   |
| [env.java.opts.all]({{< ref "docs/deployment/config#env-java-opts-all" >}})                       | {default value}        | {default value} -XX:PerMethodRecompilationCutoff=10000 -XX:PerBytecodeRecompilationCutoff=10000-XX:ReservedCodeCacheSize=512M -XX:+UseZGC |
| JDK Version                                                                                       | 11                     | 17                                                                                                                                        |

Using JDK17 within ZGC can greatly help optimize the metaspace garbage collection issue, detailed information can be found in [FLINK-32746](https://issues.apache.org/jira/browse/FLINK-32746). Meanwhile, ZGC can provide close to zero application pause time when collecting garbage objects in memory. Additionally, OLAP queries need to be executed in `BATCH` mode because both `Pipelined` and `Blocking` edges may appear in the execution plan of an OLAP query. Batch scheduler allows queries to be scheduled in stages, which could avoid scheduling deadlocks in this scenario.

#### Scheduling Options

| Parameters                                               | Default           | Recommended       |
|:------------------------------------------------------------------------------------------------------------------------|:------------------|:--------|
| [jobmanager.scheduler]({{< ref "docs/deployment/config#jobmanager-scheduler" >}})                                       | Default           | Default |
| [jobmanager.execution.failover-strategy]({{< ref "docs/deployment/config#jobmanager-execution-failover-strategy-1" >}}) | region            | full    |
| [restart-strategy.type]({{< ref "docs/deployment/config#restart-strategy-type" >}})                                     | (none)            | disable |
| [jobstore.type]({{< ref "docs/deployment/config#jobstore-type" >}})                                                     | File              | Memory  |
| [jobstore.max-capacity]({{< ref "docs/deployment/config#jobstore-max-capacity" >}})                                     | Integer.MAX_VALUE | 500     |


#### Network Options

| Parameters                                                                            | Default    | Recommended    |
|:--------------------------------------------------------------------------------------|:-----------|:---------------|
| [rest.server.numThreads]({{< ref "docs/deployment/config#rest-server-numthreads" >}}) | 4         | 32         |
| [web.refresh-interval]({{< ref "docs/deployment/config#web-refresh-interval" >}})     | 3000      | 300000     |
| [pekko.framesize]({{< ref "docs/deployment/config#pekko-framesize" >}})               | 10485760b | 104857600b |

#### ResourceManager Options

| Parameters                                                         | Default | Recommended                             |
|:-------------------------------------------------------------------|:--------|:----------------------------------------|
| [kubernetes.jobmanager.replicas]({{< ref "docs/deployment/config#kubernetes-jobmanager-replicas" >}})         | 1      | 2                                       |
| [kubernetes.jobmanager.cpu.amount]({{< ref "docs/deployment/config#kubernetes-jobmanager-cpu-amount" >}})     | 1.0    | 16.0                                    |
| [jobmanager.memory.process.size]({{< ref "docs/deployment/config#jobmanager-memory-process-size" >}})         | (none) | 32g                                     |
| [jobmanager.memory.jvm-overhead.max]({{< ref "docs/deployment/config#jobmanager-memory-jvm-overhead-max" >}}) | 1g     | 3g                                      |
| [kubernetes.taskmanager.cpu.amount]({{< ref "docs/deployment/config#kubernetes-taskmanager-cpu-amount" >}})   | (none) | 16                                      |
| [taskmanager.numberOfTaskSlots]({{< ref "docs/deployment/config#taskmanager-numberoftaskslots" >}})           | 1      | 32                                      |
| [taskmanager.memory.process.size]({{< ref "docs/deployment/config#taskmanager-memory-process-size" >}})       | (none) | 65536m                                  |
| [taskmanager.memory.managed.size]({{< ref "docs/deployment/config#taskmanager-memory-managed-size" >}})       | (none) | 16384m                                  |
| [slotmanager.number-of-slots.min]({{< ref "docs/deployment/config#slotmanager-number-of-slots-min" >}})       | 0      | {taskManagerNumber * numberOfTaskSlots} |

You can configure `slotmanager.number-of-slots.min` to a proper value as the reserved resource pool serving OLAP queries. TaskManager should configure with a large resource specification in OLAP scenario since this can put more computations in local and reduce network/deserialization/serialization overhead. Meanwhile, as a single point of calculation in OLAP, JobManager also prefer large resource specification.

## Future Work
Flink OLAP is now part of [Apache Flink Roadmap](https://flink.apache.org/what-is-flink/roadmap/), which means the community will keep putting efforts to improve Flink OLAP, both in usability and query performance. Relevant work are traced in underlying tickets:
- https://issues.apache.org/jira/browse/FLINK-25318
- https://issues.apache.org/jira/browse/FLINK-32898