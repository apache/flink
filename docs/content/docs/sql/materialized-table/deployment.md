---
title: Deployment
weight: 3
type: docs
aliases:
- /dev/table/materialized-table/deployment.html
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

Creating and operating materialized tables involves multiple components' collaborative work. This document will systematically explain the complete deployment solution for Materialized Tables, covering architectural overview, environment preparation, deployment procedures, and operational practices.

# Architecture Introduction

- **Client**: Could be any client that can interact with [Flink SQL Gateway]({{< ref "docs/sql/interfaces/sql-gateway/overview" >}}), such as [SQL Client]({{< ref "docs/sql/interfaces/sql-client" >}}), [Flink JDBC Driver]({{< ref "docs/sql/interfaces/jdbc-driver" >}}) and so on.
- **Flink SQL Gateway**: Supports creating, altering, and dropping Materialized table. It also serves as an embedded workflow scheduler to periodically refresh full mode Materialized Table.
- **Flink Cluster**: The pipeline for refreshing Materialized Table will run on the Flink cluster.
- **Catalog**: Manages the creation, retrieval, modification, and deletion of the metadata of Materialized Table.
- **Catalog Store**: Supports catalog property persistence to automatically initialize catalogs for retrieving metadata in Materialized Table related operations.

{{< img src="/fig/materialized-table-architecture.svg" alt="Illustration of Flink Materialized Table Architecture" width="85%" >}}

# Deployment Preparation

## Flink Cluster Setup

Materialized Table refresh jobs currently support execution in these cluster environments:
* [Standalone clusters]({{<ref "docs/deployment/resource-providers/standalone/overview">}})
* [YARN clusters]({{<ref "docs/deployment/resource-providers/yarn" >}})
* [Kubernetes clusters]({{<ref "docs/deployment/resource-providers/native_kubernetes" >}})

## Flink SQL Gateway Deployment

Materialized Tables in full refresh mode must be created and operated through SQL Gateway, which schedules their refresh workflows. SQL Gateway requires specific configurations for metadata persistence and job scheduling. Materialized Tables in continuous refresh mode can also be created from any `TableEnvironment` (see [Using Table API](#using-table-api)).

### Configure Catalog Store

Add catalog store configurations in `config.yaml` to persist catalog properties. 

```yaml
table:
  catalog-store:
    kind: file
    file:
      path: {path_to_catalog_store} # Replace with the actual path
```
Refer to [Catalog Store]({{<ref "docs/sql/catalogs">}}#catalog-store) for details.

### Configure Workflow Scheduler Plugin

Add workflow scheduler configurations in `config.yaml` for periodic refresh job scheduling. Currently, only the `embedded` scheduler is supported:

```yaml
workflow-scheduler:
  type: embedded
```

### Start SQL Gateway

Start the SQL Gateway using:

```shell
./sql-gateway.sh start
```

<span class="label label-danger">Note</span>
The Catalog must support creating materialized tables, which is currently only supported by [Paimon Catalog](https://paimon.apache.org/docs/master/concepts/table-types/#materialized-table).

# Operation Guide

## Connecting to SQL Gateway

Example using SQL Client:

```shell
./sql-client.sh gateway --endpoint {gateway_endpoint}:{gateway_port}
```

## Creating Materialized Tables

### Refresh Jobs Running on Standalone Cluster

```sql
Flink SQL> SET 'execution.target' = 'remote';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```

### Refresh Jobs Running in Session Mode

For session modes, pre-create session cluster as documented in  [yarn-session]({{< ref "docs/deployment/resource-providers/yarn" >}}#starting-a-flink-session-on-yarn) or [kubernetes-session]({{<ref "docs/deployment/resource-providers/native_kubernetes" >}}#starting-a-flink-session-on-kubernetes)

**Kubernetes session mode:**

```sql
Flink SQL> SET 'execution.target' = 'kubernetes-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-session-1';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Set `execution.target` to `kubernetes-session` and specify a valid `kubernetes.cluster-id` corresponding to an existing Kubernetes session cluster.

**YARN session mode:**

```sql
Flink SQL> SET 'execution.target' = 'yarn-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'yarn.application.id' = 'application-xxxx';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Set `execution.target` to `yarn-session` and specify a valid `yarn.application.id` corresponding to an existing YARN session cluster.

### Refresh Jobs Running in Application Mode

**Kubernetes application mode:**

```sql
Flink SQL> SET 'execution.target' = 'kubernetes-application';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-application-1';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Set `execution.target` to `kubernetes-application`. The `kubernetes.cluster-id` is optional; if not set, it will be automatically generated.

**YARN application mode:**

```sql
Flink SQL> SET 'execution.target' = 'yarn-application';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Only set `execution.target` to yarn-application. The `yarn.application.id` doesn’t need to be set; it will be automatically generated during submission.

## Maintenance Operations

Cluster information (e.g., `execution.target` or `kubernetes.cluster-id`) is already persisted in the catalog and does not need to be set when suspending or dropping a Materialized Table. Resuming a Materialized Table, or altering an activated one, submits a new refresh job to the session's `execution.target` and cluster id, so set them as when creating the Materialized Table.

### Suspend Refresh Job

```sql
-- Suspend the MATERIALIZED TABLE refresh job
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table SUSPEND;
[INFO] Execute statement succeeded.
```

### Resume Refresh Job

```sql
-- Resume the MATERIALIZED TABLE refresh job
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table RESUME;
[INFO] Execute statement succeeded.
```

### Modify Query Definition

```sql
-- Modify the MATERIALIZED TABLE query definition
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table
> AS SELECT
> ... ;
[INFO] Execute statement succeeded.
```

# Using Table API

Materialized tables in continuous refresh mode can also be managed from any `TableEnvironment`. Full refresh mode still requires SQL Gateway, which schedules the refresh workflows.

Configure the session cluster on the program's `TableConfig`, or in the configuration the program runs with:

| Session cluster | Options |
|---|---|
| Standalone | `execution.target: remote`, `rest.address`, `rest.port` |
| YARN session | `execution.target: yarn-session`, `yarn.application.id` |
| Kubernetes session | `execution.target: kubernetes-session`, `kubernetes.cluster-id` |

```java
TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
tEnv.getConfig().set("execution.target", "remote");
tEnv.getConfig().set("rest.address", "flink-jobmanager");
tEnv.getConfig().set("execution.checkpointing.savepoint-dir", "s3://my-bucket/savepoints");

tEnv.executeSql("CREATE MATERIALIZED TABLE my_materialized_table ...");
```

* A statement that submits a refresh job fails before anything changes if the target can't host one: an unset, `local` or `embedded` target, an application target, or a session target without its cluster id. Application mode requires SQL Gateway.
* Suspending a materialized table, and altering an activated one, stop the refresh job with a savepoint and require `execution.checkpointing.savepoint-dir`.
* Any program that uses the same catalog, and SQL Gateway, can operate the materialized table. The refresh job is reached through the execution target and cluster id recorded in the catalog. For `remote`, configure the same `rest.address` as the program that submitted it.
* Resuming a materialized table, and altering an activated one, restart the refresh job on the program's own target, which must match the recorded one. A refresh job that SQL Gateway deployed in its own application cluster can be suspended, refreshed and dropped from a program, but must be resumed or altered through SQL Gateway.
