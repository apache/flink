---
title: Quickstart
weight: 3
type: docs
aliases:
- /dev/table/materialized-table/quickstart.html
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

# Quickstart Guide

This guide will help you quickly understand and get started with materialized tables. It includes setting up the environment and creating, altering, and dropping materialized tables in CONTINUOUS and FULL mode.

# Architecture Introduction

- **Client**: Could be any client that can interact with [Flink SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}), such as [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}), [Flink JDBC Driver]({{< ref "docs/dev/table/jdbcDriver" >}}) and so on.
- **Flink SQL Gateway**: Supports creating, altering, and dropping materialized tables. It also serves as an embedded workflow scheduler to periodically refresh full mode materialized tables.
- **Flink Cluster**: The pipeline for refreshing materialized tables will run on the Flink cluster.
- **Catalog**: Manages the creation, retrieval, modification, and deletion of the metadata of materialized tables.
- **Catalog Store**: Supports catalog property persistence to automatically initialize catalogs for retrieving metadata in materialized table related operations.

{{< img src="/fig/materialized-table-architecture.svg" alt="Illustration of Flink Materialized Table Architecture" width="85%" >}}

# Environment Setup

## Directory Preparation

**Replace the example paths below with real paths on your machine.**

- Create directories for Catalog Store and test-filesystem Catalog:

```
# Directory for File Catalog Store to save catalog information
mkdir -p {catalog_store_path}

# Directory for test-filesystem Catalog to save table metadata and table data
mkdir -p {catalog_path}

# Directory for the default database of test-filesystem Catalog
mkdir -p {catalog_path}/mydb
```

- Create directories for Checkpoints and Savepoints:

```
mkdir -p {checkpoints_path}

mkdir -p {savepoints_path}
```

## Resource Preparation

The method here is similar to the steps recorded in [local installation]({{< ref "docs/try-flink/local_installation" >}}). Flink can run on any UNIX-like operating system, such as Linux, Mac OS X, and Cygwin (for Windows).

[Download](https://flink.apache.org/downloads/) the latest Flink binary package and extract it:

```
tar -xzf flink-*.tgz
```

[Download](https://https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-filesystem-test-utils/) the test-filesystem connector and place it in the lib directory:

```
cp flink-table-filesystem-test-utils-{VERSION}.jar flink-*/lib/
```

## Configuration Preparation

Edit the `config.yaml` file and add the following configurations:

```yaml
execution:
  checkpoints:
    dir: file://{checkpoints_path}

# Configure file catalog store
table:
  catalog-store:
    kind: file
    file:
      path: {catalog_store_path}

# Configure embedded scheduler
workflow-scheduler:
  type: embedded

# Configure SQL gateway address and port
sql-gateway:
  endpoint:
    rest:
      address: 127.0.0.1
      port: 8083
```

## Start Flink Cluster

Run the following script to start the cluster locally:

```
./bin/start-cluster.sh
```

## Start SQL Gateway

Run the following script to start the SQL Gateway locally:

```
./bin/sql-gateway.sh start
```

## Start SQL Client

Run the following script to start the SQL Client locally and connect to the SQL Gateway:

```
./bin/sql-client.sh gateway --endpoint http://127.0.0.1:8083
```

## Create Catalog and Source Table

- Create the test-filesystem catalog:

```sql
CREATE CATALOG mt_cat WITH (
  'type' = 'test-filesystem',
  'path' = '{catalog_path}',
  'default-database' = 'mydb'
);

USE CATALOG mt_cat;
```

- Create the Source table:

```sql
-- 1. Create Source table and specify the data format is json
CREATE TABLE json_source (
  order_id BIGINT,
  user_id BIGINT,
  user_name STRING,
  order_created_at STRING,
  payment_amount_cents BIGINT
) WITH (
  'format' = 'json',
  'source.monitor-interval' = '10s'
);

-- 2. Insert some test data
INSERT INTO json_source VALUES 
  (1001, 1, 'user1', '2024-06-19', 10),
  (1002, 2, 'user2', '2024-06-19', 20),
  (1003, 3, 'user3', '2024-06-19', 30),
  (1004, 4, 'user4', '2024-06-19', 40),
  (1005, 1, 'user1', '2024-06-20', 10),
  (1006, 2, 'user2', '2024-06-20', 20),
  (1007, 3, 'user3', '2024-06-20', 30),
  (1008, 4, 'user4', '2024-06-20', 40);
```

# Create Continuous Mode Materialized Table

## Create Materialized Table

Create a materialized table in CONTINUOUS mode with a data freshness of 30 seconds. You can find the Flink streaming job for continuous refresh the materialized table is running on the page http://localhost:8081. And it's checkpoint interval is 30 seconds.

```sql
CREATE MATERIALIZED TABLE continuous_users_shops
PARTITIONED BY (ds)
WITH (
  'format' = 'debezium-json',
  'sink.rolling-policy.rollover-interval' = '10s',
  'sink.rolling-policy.check-interval' = '10s'
)
FRESHNESS = INTERVAL '30' SECOND
AS SELECT
  user_id,
  ds,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS PV
FROM (
  SELECT user_id, order_created_at AS ds, payment_amount_cents
    FROM json_source
  ) AS tmp
GROUP BY user_id, ds;
```

## Suspend Materialized Table

Suspend the refresh pipeline of the materialized table. Your will find that the Flink streaming job for continuous refresh the materialized table transitions to FINISHED state on http://localhost:8081. Before executing the suspend operation, you need to set the savepoint path. 

```sql
-- Set savepoint path before suspending
SET 'execution.checkpointing.savepoint-dir' = 'file://{savepoints_path}';

ALTER MATERIALIZED TABLE continuous_users_shops SUSPEND;
```

## Query Materialized Table

Query the materialized table data and confirm that data has already been written.

```sql
SELECT * FROM continuous_users_shops;
```

## Resume Materialized Table

Resume the refresh pipeline of the materialized table. You will find that a new Flink streaming job for continuous refresh the materialized table is started and restored state from the specified savepoint path on http://localhost:8081 page.

```sql
ALTER MATERIALIZED TABLE continuous_users_shops RESUME;
```

## Drop Materialized Table

Drop the materialized table, and you will find that the Flink streaming job for continuous refresh the materialized table transitions to the CANCELED state on http://localhost:8081 page.

```sql
DROP MATERIALIZED TABLE continuous_users_shops;
```

# Create Full Mode Materialized Table

## Create Materialized Table

Create a materialized table in FULL mode with a data freshness of 1 minute. (Here we set freshness to 1 minute just for convenience of testing) You will find that the Flink Batch job for periodic refreshing the materialized table is scheduled every 1 minute on the http://localhost:8081.

```sql
CREATE MATERIALIZED TABLE full_users_shops
PARTITIONED BY (ds)
WITH (
  'format' = 'json',
  'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
)
FRESHNESS = INTERVAL '1' MINUTE
REFRESH_MODE = FULL
AS SELECT
  user_id,
  ds,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS PV
FROM (
  SELECT user_id, order_created_at AS ds, payment_amount_cents
  FROM json_source
) AS tmp
GROUP BY user_id, ds;
```

## Query Materialized Table

Insert some data into today's partition. Wait at least 1 minute and query the materialized table results to find that only today's partition data is refreshed.

```sql
INSERT INTO json_source VALUES 
  (1001, 1, 'user1', CAST(CURRENT_DATE AS STRING), 10),
  (1002, 2, 'user2', CAST(CURRENT_DATE AS STRING), 20),
  (1003, 3, 'user3', CAST(CURRENT_DATE AS STRING), 30),
  (1004, 4, 'user4', CAST(CURRENT_DATE AS STRING), 40);
```


```sql
SELECT * FROM full_users_shops;
```

## Manually Refresh Historical Partition

Manually refresh the partition `ds='2024-06-20'` and verify the data in the materialized table. You can find the Flink batch job for the current refresh operation on the http://localhost:8081 page.

```sql
-- Manually refresh historical partition
ALTER MATERIALIZED TABLE full_users_shops REFRESH PARTITION(ds='2024-06-20');

-- Query materialized table data
SELECT * FROM full_users_shops;
```

## Suspend and Resume Materialized Table

By suspending and resuming operations, you can control the refresh jobs corresponding to the materialized table. After suspending, the Flink batch job for periodic refreshing the materialized table will not be scheduled. After resuming, the Flink batch job for periodic refreshing the materialized table will be rescheduled again. You can find the Flink job scheduling status on the http://localhost:8081 page.

```sql
-- Suspend background refresh pipeline
ALTER MATERIALIZED TABLE full_users_shops SUSPEND;

-- Resume background refresh pipeline
ALTER MATERIALIZED TABLE full_users_shops RESUME;
```

## Drop Materialized Table

After dropping the materialized table, the Flink batch job for periodic refreshing the materialized table will not be scheduled again. You can confirm this on the http://localhost:8081 page.

```sql
DROP MATERIALIZED TABLE full_users_shops;
```
