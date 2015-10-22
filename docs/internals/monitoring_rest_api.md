---
title:  "Monitoring REST API"
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

Flink has a monitoring API that can be used to query status and statistics of running jobs, as well as recent completed jobs.
This monitoring API is used by Flink's own dashboard, but is designed to be used also by custom monitoring tools.

The monitoring API is a REST-ful API that accepts HTTP GET requests and responds with JSON data.

* This will be replaced by the TOC
{:toc}



## Overview

The monitoring API is backed by a web server that runs as part of the *JobManager*. By default, this server listens at post `8081`, which can be configured in `flink-conf.yaml` via `jobmanager.web.port`. Note that the monitoring API web server and the web dashboard web server are currently the same and thus run together at the same port. They respond to different HTTP URLs, though.

In the case of multiple JobManagers (for high availability), each JobManager will run its own instance of the monitoring API, which offers information about completed and running job while that JobManager was elected the cluster leader.

**NOTE:** Currently, the monitoring API is started together with the new web dashboard. To enable it, one need to add the following entry to the `flink-conf.yaml` in order to activate the new dashboard instread of the old dashboard: `jobmanager.new-web-frontend: true`


## Developing

The REST API backend is in the `flink-runtime-web` project. The core class is `org.apache.flink.runtime.webmonitor.WebRuntimeMonitor`, which sets up the server and the request routing.

We use *Netty* and the *Netty Router* library to handle REST requests and translate URLs. This choice was made because this combination has lightweight dependencies, and the performance of Netty HTTP is very good.

To add new requests, one needs to add a new *request handler* class. A good example to look at is the `org.apache.flink.runtime.webmonitor.handlers.JobExceptionsHandler`. After creating the handler, the handler needs to be registered with the request router in `org.apache.flink.runtime.webmonitor.WebRuntimeMonitor`.


## Available Requests

Below is a list of available requests, with a sample JSON response. All requests are of the sample form `http://hostname:8081/jobs`, below we list only the *path* part of the URLs.

Values in angle brackets are variables, for example `http://hostname:8081/jobs/<jobid>/exceptions` will have to requested for example as `http://hostname:8081/jobs/7684be6004e4e955c2a558a9bc463f65/exceptions`.

  - `/config`
  - `/overview`
  - `/jobs`
  - `/joboverview/running`
  - `/joboverview/completed`
  - `/jobs/<jobid>`
  - `/jobs/<jobid>/vertices`
  - `/jobs/<jobid>/config`
  - `/jobs/<jobid>/exceptions`
  - `/jobs/<jobid>/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasktimes`
  - `/jobs/<jobid>/vertices/<vertexid>/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>/accumulators`
  - `/jobs/<jobid>/plan`


### General

**`/config`**

Some information about the monitoring API and the server setup.

Sample Result:

~~~
{
  "refresh-interval": 3000,
  "timezone-offset": 3600000,
  "timezone-name": "Central European Time",
  "flink-version": "0.10-SNAPSHOT",
  "flink-revision": "8124545 @ 16.09.2015 @ 15:38:42 CEST"
}
~~~

**`/overview`**

Simple summary of the Flink cluster status.

Sample Result:

~~~
{
  "taskmanagers": 17,
  "slots-total": 68,
  "slots-available": 68,
  "jobs-running": 0,
  "jobs-finished": 3,
  "jobs-cancelled": 1,
  "jobs-failed": 0
}
~~~

### Overview of Jobs

**`/jobs`**

IDs of the jobs, grouped by status *running*, *finished*, *failed*, *canceled*.

Sample Result:

~~~
{
  "jobs-running": [],
  "jobs-finished": ["7684be6004e4e955c2a558a9bc463f65","49306f94d0920216b636e8dd503a6409"],
  "jobs-cancelled":[],
  "jobs-failed":[]
}
~~~

**`/joboverview`**

Jobs, groupes by status, each with a small summary of its status.

Sample Result:

~~~
{
  "running":[],
  "finished":[
    {
      "jid": "7684be6004e4e955c2a558a9bc463f65",
      "name": "Flink Java Job at Wed Sep 16 18:08:21 CEST 2015",
      "state": "FINISHED",
      "start-time": 1442419702857,
      "end-time": 1442419975312,
      "duration":272455,
      "last-modification": 1442419975312,
      "tasks": {
         "total": 6,
         "pending": 0,
         "running": 0,
         "finished": 6,
         "canceling": 0,
         "canceled": 0,
         "failed": 0
      }
    },
    {
      "jid": "49306f94d0920216b636e8dd503a6409",
      "name": "Flink Java Job at Wed Sep 16 18:16:39 CEST 2015",
      ...
    }]
}
~~~

**`/joboverview/running`**

Jobs, groupes by status, each with a small summary of its status. The same as `/joboverview`, but containing only currently running jobs.

**`/joboverview/completed`**

Jobs, groupes by status, each with a small summary of its status. The same as `/joboverview`, but containing only completed (finished, canceled, or failed) jobs.


### Details of a Running or Completed Job

**`/jobs/<jobid>`**

Summary of one job, listing dataflow plan, status, timestamps of state transitions, aggregate information for each vertex (operator).

Sample Result:

~~~
{
  "jid": "ab78dcdbb1db025539e30217ec54ee16",
  "name": "WordCount Example",
  "state":"FINISHED",
  "start-time":1442421277536,
  "end-time":1442421299791,
  "duration":22255,
  "now":1442421991768,
  "timestamps": {
    "CREATED": 1442421277536, "RUNNING": 1442421277609, "FAILING": 0, "FAILED": 0, "CANCELLING": 0, "CANCELED": 0, "FINISHED": 1442421299791, "RESTARTING": 0
  },
  "vertices": [ {
    "id": "19b5b24062c48a06e4eac65422ac3317",
    "name": "CHAIN DataSource (at getTextDataSet(WordCount.java:142) ...",
    "parallelism": 2,
    "status": "FINISHED",
    "start-time": 1442421277609,
    "end-time": 1442421299469,
    "duration": 21860,
    "tasks": {
      "CREATED": 0, "SCHEDULED": 0, "DEPLOYING": 0, "RUNNING": 0, "FINISHED": 2, "CANCELING": 0, "CANCELED": 0, "FAILED": 0
    },
    "metrics": {
      "read-bytes": 0, "write-bytes": 37098, "read-records": 0, "write-records": 3312
    }
  }, {
    "id": "f00c89b349b5c998cfd9fe2a06e50fd0",
    "name":"Reduce (SUM(1), at main(WordCount.java:67)",
    "parallelism": 2,
    ....
  }, {
    "id": "0a36cbc29102d7bc993d0a9bf23afa12",
    "name": "DataSink (CsvOutputFormat (path: /tmp/abzs, delimiter:  ))",
    ...
  } ],
  "status-counts": {
    "CREATED": 0, "SCHEDULED": 0, "DEPLOYING": 0, "RUNNING": 0, "FINISHED": 3, "CANCELING": 0, "CANCELED": 0, "FAILED": 0
  },
  "plan": {
    // see plan details below
  }
}
~~~

**`/jobs/<jobid>/vertices`**

Currently the same as `/jobs/<jobid>`


**`/jobs/<jobid>/config`**

The user-defined execution config used by the job.

Sample Result:

~~~
{
  "jid": "ab78dcdbb1db025539e30217ec54ee16",
  "name": "WordCount Example",
  "execution-config": {
    "execution-mode": "PIPELINED",
    "max-execution-retries": -1,
    "job-parallelism": -1,
    "object-reuse-mode": false
  }
}
~~~

**`/jobs/<jobid>/exceptions`**

The non-recoverable exceptions that have been observed by the job.
The `truncated` flag defines whether more exceptions occurred, but are not listed, because the response would otherwise get too big.

Sample Result:

~~~
{
  "root-exception": "java.io.IOException: File already exists:/tmp/abzs/2\n\tat org.apache.flink.core.fs.local.LocalFileSystem. ...",
  "all-exceptions": [ {
    "exception": "java.io.IOException: File already exists:/tmp/abzs/1\n\tat org.apache.flink...",
    "task": "DataSink (CsvOutputFormat (path: /tmp/abzs, delimiter:  )) (1/2)",
    "location": "localhost:49220"
  }, {
    "exception": "java.io.IOException: File already exists:/tmp/abzs/2\n\tat org.apache.flink...",
    "task": "DataSink (CsvOutputFormat (path: /tmp/abzs, delimiter:  )) (2/2)",
    "location": "localhost:49220"
  } ],
  "truncated":false
}
~~~

**`/jobs/<jobid>/accumulators`**

The aggregated user accumulators plus job accumulators.

Sample Result:

~~~
{
  "job-accumulators":[],
  "user-task-accumulators": [ {
    "name": "avglen",
    "type": "DoubleCounter",
    "value": "DoubleCounter 61.5162972"
  },
  { 
    "name": "genwords",
    "type": "LongCounter",
    "value": "LongCounter 37500000"
  } ]
}
~~~

**`/jobs/<jobid>/vertices/<vertexid>`**

Information about one specific vertex, with a summary for each of its subtasks.

Sample Result:

~~~
{
  "id": "dceafe2df1f57a1206fcb907cb38ad97",
  "name": "CHAIN DataSource -> Map -> FlatMap -> Combine(SUM(1))",
  "parallelism": 2,
  "now": 1442424002154,
  "subtasks": [ { 
    "subtask":0,
    "status": "FINISHED",
    "attempt": 0,
    "host": "localhost",
    "start-time": 1442421093762,
    "end-time": 1442421386680,
    "duration": 292918,
    "metrics": {
      "read-bytes": 0, "write-bytes": 12684375, "read-records": 0, "write-records": 1153125
    }
  }, {
    "subtask": 1,
    "status": "FINISHED",
    "attempt": 0,
    "host": "localhost",
    "start-time": 1442421093774,
    "end-time": 1442421386267,
    "duration": 292493,
    "metrics": {
      "read-bytes": 0, "write-bytes": 12684375, "read-records": 0, "write-records": 1153125
    }
  } ]
}
~~~

**`/jobs/<jobid>/vertices/<vertexid>/subtasktimes`**

This request returns the timestamps for the state transitions of all subtasks of a given vertex.
These can be used, for example, to create time-line comparisons between subtasks.

Sample Result:

~~~
{
  "id": "dceafe2df1f57a1206fcb907cb38ad97",
  "name": "CHAIN DataSource -> Map -> Combine(SUM(1))",
  "now":1442423745088,
  "subtasks": [ {
    "subtask": 0,
    "host": "localhost",
    "duration": 292924,
    "timestamps": {
      "CREATED": 1442421093741, "SCHEDULED": 1442421093756, "DEPLOYING": 1442421093762, "RUNNING": 1442421094026, "FINISHED": 1442421386680, "CANCELING": 0, "CANCELED": 0, "FAILED": 0
    }
  }, {
    "subtask": 1,
    "host": "localhost",
    "duration": 292494,
    "timestamps": {
      "CREATED": 1442421093741, "SCHEDULED": 1442421093773, "DEPLOYING": 1442421093774, "RUNNING": 1442421094013, "FINISHED": 1442421386267, "CANCELING": 0, "CANCELED": 0, "FAILED": 0
    }
  } ]
}
~~~

**`/jobs/<jobid>/vertices/<vertexid>/accumulators`**

The aggregated user-defined accumulators, for a specific vertex.

Sample Result:

~~~
{
  "id": "dceafe2df1f57a1206fcb907cb38ad97",
  "user-accumulators": [ {
    "name": "avglen", "type": "DoubleCounter", "value": "DoubleCounter 123.03259440000001"
  }, {
    "name": "genwords", "type": "LongCounter", "value": "LongCounter 75000000"
  } ]
}
~~~

**`/jobs/<jobid>/vertices/<vertexid>/subtasks/accumulators`**

Gets all user-defined accumulators for all subtasks of a given vertex. These are the individual accumulators that are returned in aggregated form by the
request `/jobs/<jobid>/vertices/<vertexid>/accumulators`.

Sample Result:

~~~
{
  "id": "dceafe2df1f57a1206fcb907cb38ad97",
  "parallelism": 2,
  "subtasks": [ {
    "subtask": 0,
    "attempt": 0,
    "host": "localhost",
    "user-accumulators": [ {
      "name": "genwords", "type": "LongCounter", "value": "LongCounter 62500000"
    }, {
      "name": "genletters", "type": "LongCounter", "value": "LongCounter 1281589525"
    } ]
  }, {
    "subtask": 1,
    "attempt": 0,
    "host": "localhost",
    "user-accumulators": [ {
      "name": "genwords", "type": "LongCounter", "value": "LongCounter 12500000"
    }, {
      "name": "genletters", "type": "LongCounter", "value": "LongCounter 256317905"
    } ]
  } ]
}
~~~

**`/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>`**

Summary of the current or latest execution attempt of a specific subtask. See below for a sample.


**`/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>`**

Summary of a specific execution attempt of a specific subtask. Multiple execution attempts happen in case of failure/recovery.

Sample Result:

~~~
{
  "subtask": 0,
  "status": "FINISHED",
  "attempt": 0,
  "host": "localhost",
  "start-time": 1442421093762,
  "end-time": 1442421386680,
  "duration": 292918,
  "metrics": {
    "read-bytes": 0, "write-bytes": 12684375, "read-records": 0, "write-records": 1153125
  }
}
~~~

**`/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>/accumulators`**

The accumulators collected for one specific subtask during one specific execution attempt (multiple attempts happen in case of failure/recovery).

Sample Result:

~~~
{
  "subtask": 0,
  "attempt": 0,
  "id": "b22f94d91bf41ddb",
  "user-accumulators": [ {
    "name": "genwords", "type":"LongCounter", "value":"LongCounter 62500000"
  }, {
    "name": "genletters", "type": "LongCounter", "value": "LongCounter 1281589525"
  }, {
  "name": "avglen", "type": "DoubleCounter", "value": "DoubleCounter 102.527162"
  } ]
}
~~~

**`/jobs/<jobid>/plan`**

The dataflow plan of a job. The plan is also included in the job summary (`/jobs/<jobid>`).

Sample Result:

~~~
{
  "jid":"ab78dcdbb1db025539e30217ec54ee16",
  "name":"WordCount Example",
  "nodes": [ {
    "id": "f00c89b349b5c998cfd9fe2a06e50fd0",
    "parallelism": 2,
    "operator": "GroupReduce",
    "operator_strategy": "Sorted Group Reduce",
    "description": "Reduce (SUM(1), at main(WordCount.java:67)",
    "inputs": [ {
      "num": 0,
      "id":"19b5b24062c48a06e4eac65422ac3317",
      "ship_strategy": "Hash Partition on [0]",
      "local_strategy":"Sort (combining) on [0:ASC]",
      "exchange":"pipelined"
    } ],
    "optimizer_properties": {
      "global_properties": [ 
        { "name":"Partitioning", "value":"HASH_PARTITIONED" },
        { "name":"Partitioned on", "value":"[0]" },
        { "name":"Partitioning Order", "value":"(none)" },
        { "name":"Uniqueness", "value":"not unique" }
      ],
      "local_properties": [
        { "name":"Order", "value":"[0:ASC]" },
        { "name":"Grouped on", "value":"[0]" },
        { "name":"Uniqueness", "value":"not unique" }
      ],
      "estimates": [
        { "name":"Est. Output Size", "value":"(unknown)" },
        { "name":"Est. Cardinality", "value":"(unknown)" }
      ],
      "costs": [ 
        { "name":"Network", "value":"(unknown)" },
        { "name":"Disk I/O", "value":"(unknown)" },
        { "name":"CPU", "value":"(unknown)" },
        { "name":"Cumulative Network", "value":"(unknown)" },
        { "name":"Cumulative Disk I/O", "value":"(unknown)" },
        { "name":"Cumulative CPU","value":"(unknown)" }
      ],
      "compiler_hints": [
        { "name":"Output Size (bytes)", "value":"(none)" },
        { "name":"Output Cardinality", "value":"(none)" },
        { "name":"Avg. Output Record Size (bytes)", "value":"(none)" },
        { "name":"Filter Factor", "value":"(none)" }
      ]
    }
  },
  {
    "id": "19b5b24062c48a06e4eac65422ac3317",
    "parallelism": 2,
    "operator": "Data Source -> FlatMap -> GroupCombine",
    "operator_strategy":" (none) -> FlatMap -> Sorted Combine",
    "description":"DataSource (at getTextDataSet(WordCount.java:142) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at main(WordCount.java:67)) -> Combine(SUM(1), at main(WordCount.java:67)",
    "optimizer_properties": {
      ...
    }
  },
  {
    "id": "0a36cbc29102d7bc993d0a9bf23afa12",
    "parallelism": 2,
    "operator": "Data Sink",
    "operator_strategy": "(none)",
    "description": "DataSink (CsvOutputFormat (path: /tmp/abzs, delimiter:  ))",
    "inputs":[ {
      "num": 0,
      "id": "f00c89b349b5c998cfd9fe2a06e50fd0",
      "ship_strategy": "Forward",
      "exchange": "pipelined"
    } ],
    "optimizer_properties": {
      ...
    }
  } ]
}
~~~


