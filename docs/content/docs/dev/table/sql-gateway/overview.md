---
title: Overview
weight: 1
type: docs
aliases:
- /dev/table/sql-gateway.html
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

Introduction
----------------

The SQL Gateway is a service that enables multiple clients from the remote to execute SQL in concurrency. It provides 
an easy way to submit the Flink Job, look up the metadata, and analyze the data online.

The SQL Gateway is composed of pluggable endpoints and the `SqlGatewayService`. The `SqlGatewayService` is a processor that is 
reused by the endpoints to handle the requests. The endpoint is an entry point that allows users to connect. Depending on the 
type of the endpoints, users can use different utils to connect.

{{< img width="80%" src="/fig/sql-gateway-architecture.png" alt="SQL Gateway Architecture" >}}

Getting Started
---------------

This section describes how to setup and run your first Flink SQL program from the command-line.

The SQL Gateway is bundled in the regular Flink distribution and thus runnable out-of-the-box. It requires only a running Flink cluster where table programs can be executed. For more information about setting up a Flink cluster see the [Cluster & Deployment]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) part. If you simply want to try out the SQL Client, you can also start a local cluster with one worker using the following command:

```bash
$ ./bin/start-cluster.sh
```
### Starting the SQL Gateway

The SQL Gateway scripts are also located in the binary directory of Flink. Users can start by calling:

```bash
$ ./bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost
```

The command starts the SQL Gateway with REST Endpoint that listens on the address localhost:8083. You can use the curl command to check
whether the REST Endpoint is available.

```bash
$ curl http://localhost:8083/v1/info
{"productName":"Apache Flink","version":"{{< version >}}"}
```

### Running SQL Queries

For validating your setup and cluster connection, you can work with following steps.

**Step 1: Open a session**

```bash
$ curl --request POST http://localhost:8083/v1/sessions
{"sessionHandle":"..."}
```

The `sessionHandle` in the return results is used by the SQL Gateway to uniquely identify every active user. 

**Step 2: Execute a query**

```bash
$ curl --request POST http://localhost:8083/v1/sessions/${sessionHandle}/statements/ --data '{"statement": "SELECT 1"}'
{"operationHandle":"..."}
```

The `operationHandle` in the return results is used by the SQL Gateway to uniquely identify the submitted SQL.


**Step 3: Fetch results**

With the `sessionHandle` and `operationHandle` above, you can fetch the corresponding results.

```bash
$ curl --request GET http://localhost:8083/v1/sessions/${sessionHandle}/operations/${operationHandle}/result/0
{
  "results": {
    "columns": [
      {
        "name": "EXPR$0",
        "logicalType": {
          "type": "INTEGER",
          "nullable": false
        }
      }
    ],
    "data": [
      {
        "kind": "INSERT",
        "fields": [
          1
        ]
      }
    ]
  },
  "resultType": "PAYLOAD",
  "nextResultUri": "..."
}
```

The `nextResultUri` in the results is used to fetch the next batch results if it is not `null`.

```bash
$ curl --request GET ${nextResultUri}
```

Configuration
----------------

### SQL Gateway startup options

Currently, the SQL Gateway script has the following optional commands. They are discussed in details in the subsequent paragraphs.

```bash
$ ./bin/sql-gateway.sh --help

Usage: sql-gateway.sh [start|start-foreground|stop|stop-all] [args]
  commands:
    start               - Run a SQL Gateway as a daemon
    start-foreground    - Run a SQL Gateway as a console application
    stop                - Stop the SQL Gateway daemon
    stop-all            - Stop all the SQL Gateway daemons
    -h | --help         - Show this help message
```

For "start" or "start-foreground" command,  you are able to configure the SQL Gateway in the CLI.

```bash
$ ./bin/sql-gateway.sh start --help

Start the Flink SQL Gateway as a daemon to submit Flink SQL.

  Syntax: start [OPTIONS]
     -D <property=value>   Use value for given property
     -h,--help             Show the help message with descriptions of all
                           options.
```

### SQL Gateway Configuration

You can configure the SQL Gateway when starting the SQL Gateway below, or any valid [Flink configuration]({{< ref "docs/dev/table/config" >}}) entry:

```bash
$ ./sql-gateway -Dkey=value
```

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>sql-gateway.session.check-interval</h5></td>
            <td style="word-wrap: break-word;">1 min</td>
            <td>Duration</td>
            <td>The check interval for idle session timeout, which can be disabled by setting to zero.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.session.idle-timeout</h5></td>
            <td style="word-wrap: break-word;">10 min</td>
            <td>Duration</td>
            <td>Timeout interval for closing the session when the session hasn't been accessed during the interval. If setting to zero, the session will not be closed.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.session.max-num</h5></td>
            <td style="word-wrap: break-word;">1000000</td>
            <td>Integer</td>
            <td>The maximum number of the active session for sql gateway service.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.worker.keepalive-time</h5></td>
            <td style="word-wrap: break-word;">5 min</td>
            <td>Duration</td>
            <td>Keepalive time for an idle worker thread. When the number of workers exceeds min workers, excessive threads are killed after this time interval.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.worker.threads.max</h5></td>
            <td style="word-wrap: break-word;">500</td>
            <td>Integer</td>
            <td>The maximum number of worker threads for sql gateway service.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.worker.threads.min</h5></td>
            <td style="word-wrap: break-word;">5</td>
            <td>Integer</td>
            <td>The minimum number of worker threads for sql gateway service.</td>
        </tr>
    </tbody>
</table>

Supported Endpoints
----------------

Flink natively supports [REST Endpoint]({{< ref "docs/dev/table/sql-gateway/rest" >}}) and [HiveServer2 Endpoint]({{< ref "docs/dev/table/hive-compatibility/hiveserver2" >}}).
The SQL Gateway is bundled with the REST Endpoint by default. With the flexible architecture, users are able to start the SQL Gateway with the specified endpoints by calling 

```bash
$ ./bin/sql-gateway.sh start -Dsql-gateway.endpoint.type=hiveserver2
```

or add the following config in the `conf/flink-conf.yaml`:

```yaml
sql-gateway.endpoint.type: hiveserver2
```

{{< hint info >}}
Notice: The CLI command has higher priority if flink-conf.yaml also contains the option `sql-gateway.endpoint.type`.
{{< /hint >}}

For the specific endpoint, please refer to the corresponding page.

{{< top >}}
