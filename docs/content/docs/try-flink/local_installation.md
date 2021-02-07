---
title: 'Local Installation'
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

# Local Installation
 
{{< unstable >}}
{{< hint info >}}
  NOTE: The Apache Flink community only publishes official builds for
  released versions of Apache Flink.

  Since you are currently looking at the latest `SNAPSHOT`
  version of the documentation, all version references below will not work.
  Please switch the documentation to the latest released version via the release picker which you
  find on the left side below the menu.
{{< /hint >}}
{{< /unstable >}}

Follow these few steps to download the latest stable versions and get started.

## Step 1: Download

To be able to run Flink, the only requirement is to have a working __Java 8 or 11__ installation.
You can check the correct installation of Java by issuing the following command:

```bash
java -version
```

[Download](https://flink.apache.org/downloads.html) the {{< version >}} release and un-tar it. 

```bash
$ tar -xzf flink-{{< version >}}-bin-scala{{< scala_version >}}.tgz
$ cd flink-{{< version >}}-bin-scala{{< scala_version >}}
```

## Step 2: Start a Cluster

Flink ships with a single bash script to start a local cluster.

```bash
$ ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host.
Starting taskexecutor daemon on host.
```

## Step 3: Submit a Job

Releases of Flink come with a number of example Jobs.
You can quickly deploy one of these applications to the running cluster. 

```bash
$ ./bin/flink run examples/streaming/WordCount.jar
$ tail log/flink-*-taskexecutor-*.out
  (to,1)
  (be,1)
  (or,1)
  (not,1)
  (to,2)
  (be,2)
```

Additionally, you can check Flink's [Web UI](http://localhost:8081) to monitor the status of the Cluster and running Job.

## Step 4: Stop the Cluster

When you are finished you can quickly stop the cluster and all running components.

```bash
$ ./bin/stop-cluster.sh
```
