---
title: 'First steps'
weight: 1
type: docs
aliases:
  - /try-flink/local_installation.html
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

# First Steps

Welcome to Apache Flink! This guide will help you get a Flink cluster up and running so you can start exploring Flink's capabilities.

## Prerequisites

Choose one of the following installation methods:

- **Docker**: No Java installation needed, includes SQL Client
- **Local Installation**: Requires Java 11, 17, or 21
- **PyFlink**: Requires Java and Python 3.9+

## Option A: Docker Installation

The fastest way to get started with Flink. No Java installation required.

### Step 1: Get the docker-compose.yml file

[Download docker-compose.yml](/downloads/docker-compose.yml) or create a file named `docker-compose.yml` with the following content:

```yaml
services:
  jobmanager:
    image: flink:latest
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: flink:latest
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2

  sql-client:
    image: flink:latest
    depends_on:
      - jobmanager
    command: bin/sql-client.sh
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager
```

### Step 2: Start the Cluster

```bash
$ docker compose up -d
```

### Step 3: Verify the Cluster is Running

Open the Flink Web UI at [http://localhost:8081](http://localhost:8081) to verify the cluster is running.

### Using the SQL Client

To start an interactive SQL session:

```bash
$ docker compose run sql-client
```

To exit the SQL Client, type `exit;` and press Enter.

### Stopping the Cluster

```bash
$ docker compose down
```

For more Docker options (scaling, Application Mode), see the [Docker deployment guide]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}).

## Option B: Local Installation

If you prefer to run Flink directly on your machine without Docker.

### Step 1: Download Flink

Flink runs on all UNIX-like environments, including Linux, Mac OS X, and Cygwin (for Windows).

First, verify your Java version:

```bash
$ java -version
```

You need Java 11, 17, or 21 installed. Then, [download the latest binary release]({{< downloads >}}) and extract the archive:

```bash
$ tar -xzf flink-*.tgz
$ cd flink-*
```

### Step 2: Start the Cluster

Start a local Flink cluster:

```bash
$ ./bin/start-cluster.sh
```

### Step 3: Verify the Cluster is Running

Open the Flink Web UI at [http://localhost:8081](http://localhost:8081) to verify the cluster is running. You should see the Flink dashboard showing one TaskManager with available task slots.

### Using the SQL Client

To start an interactive SQL session:

```bash
$ ./bin/sql-client.sh
```

To exit the SQL Client, type `exit;` and press Enter.

### Stopping the Cluster

When you're done, stop the cluster with:

```bash
$ ./bin/stop-cluster.sh
```

## Option C: PyFlink Installation

For Python development with the Table API or DataStream API. No Flink cluster is required—PyFlink runs in local mode during development.

### Step 1: Verify Prerequisites

PyFlink requires Java and Python:

```bash
$ java -version
# Java 11, 17, or 21

$ python --version
# Python 3.9, 3.10, 3.11, or 3.12
```

### Step 2: Install PyFlink

{{< stable >}}
```bash
$ python -m pip install apache-flink=={{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ python -m pip install apache-flink
```
{{< /unstable >}}

{{< hint info >}}
**Tip:** We recommend installing PyFlink in a [virtual environment](https://docs.python.org/3/library/venv.html) to keep your project dependencies isolated.
{{< /hint >}}

### Step 3: Verify Installation

```bash
$ python -c "import pyflink; print(pyflink.__version__)"
```

You're now ready to follow the [Table API Tutorial]({{< ref "docs/getting-started/table_api" >}}) or [DataStream API Tutorial]({{< ref "docs/getting-started/datastream" >}}) using the Python tabs.

## Next Steps

Choose a tutorial to start learning:

| Tutorial | Description | Setup Required |
|----------|-------------|----------------|
| [Flink SQL Tutorial]({{< ref "docs/getting-started/quickstart-sql" >}}) | Query data interactively using SQL. No coding required. | Option A or B (cluster) |
| [Table API Tutorial]({{< ref "docs/getting-started/table_api" >}}) | Build streaming pipelines with Java or Python. | Maven (Java) or Option C (Python) |
| [DataStream API Tutorial]({{< ref "docs/getting-started/datastream" >}}) | Build stateful streaming applications with Java or Python. | Maven (Java) or Option C (Python) |
| [Flink Operations Playground]({{< ref "docs/getting-started/flink-operations-playground" >}}) | Learn to operate Flink: scaling, failure recovery, and upgrades. | Docker |

To learn more about Flink's core concepts, visit the [Concepts]({{< ref "docs/concepts/overview" >}}) section.
