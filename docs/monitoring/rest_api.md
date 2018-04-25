---
title: "Monitoring REST API"
nav-parent_id: monitoring
nav-pos: 10
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


## Developing

The REST API backend is in the `flink-runtime-web` project. The core class is `org.apache.flink.runtime.webmonitor.WebRuntimeMonitor`, which sets up the server and the request routing.

We use *Netty* and the *Netty Router* library to handle REST requests and translate URLs. This choice was made because this combination has lightweight dependencies, and the performance of Netty HTTP is very good.

To add new requests, one needs to add a new *request handler* class. A good example to look at is the `org.apache.flink.runtime.webmonitor.handlers.JobExceptionsHandler`. After creating the handler, the handler needs to be registered with the request router in `org.apache.flink.runtime.webmonitor.WebRuntimeMonitor`.


## Available Requests

Below is a list of available requests, with a sample JSON response. All requests are of the sample form `http://hostname:8081/jobs`, below we list only the *path* part of the URLs.

Values in angle brackets are variables, for example `http://hostname:8081/jobs/<jobid>/exceptions` will have to requested for example as `http://hostname:8081/jobs/7684be6004e4e955c2a558a9bc463f65/exceptions`.

  - `/config`
  - `/overview`
  - `/jobs/overview`
  - `/jobs/<jobid>`
  - `/jobs/<jobid>/vertices`
  - `/jobs/<jobid>/config`
  - `/jobs/<jobid>/exceptions`
  - `/jobs/<jobid>/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasktimes`
  - `/jobs/<jobid>/vertices/<vertexid>/taskmanagers`
  - `/jobs/<jobid>/vertices/<vertexid>/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>/accumulators`
  - `/jobs/<jobid>/plan`
  - `/jars/upload`
  - `/jars`
  - `/jars/:jarid`
  - `/jars/:jarid/plan`
  - `/jars/:jarid/run`


### General

**`/config`**

Some information about the monitoring API and the server setup.

Sample Result:

{% highlight json %}
{
  "refresh-interval": 3000,
  "timezone-offset": 3600000,
  "timezone-name": "Central European Time",
  "flink-version": "{{ site.version }}",
  "flink-revision": "8124545 @ 16.09.2015 @ 15:38:42 CEST"
}
{% endhighlight %}

**`/overview`**

Simple summary of the Flink cluster status.

Sample Result:

{% highlight json %}
{
  "taskmanagers": 17,
  "slots-total": 68,
  "slots-available": 68,
  "jobs-running": 0,
  "jobs-finished": 3,
  "jobs-cancelled": 1,
  "jobs-failed": 0
}
{% endhighlight %}

### Overview of Jobs

**`/jobs/overview`**

Jobs, grouped by status, each with a small summary of its status.

Sample Result:

{% highlight json %}
{
  "jobs":[
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
{% endhighlight %}

### Details of a Running or Completed Job

**`/jobs/<jobid>`**

Summary of one job, listing dataflow plan, status, timestamps of state transitions, aggregate information for each vertex (operator).

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/vertices`**

Currently the same as `/jobs/<jobid>`


**`/jobs/<jobid>/config`**

The user-defined execution config used by the job.

Sample Result:

{% highlight json %}
{
  "jid": "ab78dcdbb1db025539e30217ec54ee16",
  "name": "WordCount Example",
  "execution-config": {
    "execution-mode": "PIPELINED",
    "restart-strategy": "Restart deactivated",
    "job-parallelism": -1,
    "object-reuse-mode": false
  }
}
{% endhighlight %}

**`/jobs/<jobid>/exceptions`**

The non-recoverable exceptions that have been observed by the job.
The `truncated` flag defines whether more exceptions occurred, but are not listed, because the response would otherwise get too big.

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/accumulators`**

The aggregated user accumulators plus job accumulators.

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/vertices/<vertexid>`**

Information about one specific vertex, with a summary for each of its subtasks.

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/vertices/<vertexid>/subtasktimes`**

This request returns the timestamps for the state transitions of all subtasks of a given vertex.
These can be used, for example, to create time-line comparisons between subtasks.

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/vertices/<vertexid>/taskmanagers`**

TaskManager statistics for one specific vertex. This is an aggregation of subtask statistics returned by `/jobs/<jobid>/vertices/<vertexid>`.

Sample Result:

{% highlight json %}
{
  "id": "fe20bcc29b87cdc76589ca42114c2499",
  "name": "Reduce (SUM(1), at main(WordCount.java:72)",
  "now": 1454348282653,
  "taskmanagers": [ {
    "host": "ip-10-0-43-227:35413",
    "status": "FINISHED",
    "start-time": 1454347870991,
    "end-time": 1454347872111,
    "duration": 1120,
    "metrics": {
      "read-bytes": 32503056, "write-bytes": 9637041, "read-records": 2906087, "write-records": 849467
    },
    "status-counts": {
      "CREATED": 0, "SCHEDULED": 0, "DEPLOYING": 0, "RUNNING": 0, "FINISHED": 18, "CANCELING": 0, "CANCELED": 0, "FAILED": 0
    }
  },{
    "host": "ip-10-0-43-227:41486",
    "status": "FINISHED",
    "start-time": 1454347871001,
    "end-time": 1454347872395,
    "duration": 1394,
    "metrics": {
      "read-bytes": 32389499, "write-bytes": 9608829, "read-records": 2895999, "write-records": 846948
    },
    "status-counts": {
      "CREATED": 0, "SCHEDULED": 0, "DEPLOYING": 0, "RUNNING": 0, "FINISHED": 18, "CANCELING": 0, "CANCELED": 0, "FAILED": 0
    }
  } ]
}
{% endhighlight %}

**`/jobs/<jobid>/vertices/<vertexid>/accumulators`**

The aggregated user-defined accumulators, for a specific vertex.

Sample Result:

{% highlight json %}
{
  "id": "dceafe2df1f57a1206fcb907cb38ad97",
  "user-accumulators": [ {
    "name": "avglen", "type": "DoubleCounter", "value": "DoubleCounter 123.03259440000001"
  }, {
    "name": "genwords", "type": "LongCounter", "value": "LongCounter 75000000"
  } ]
}
{% endhighlight %}

**`/jobs/<jobid>/vertices/<vertexid>/subtasks/accumulators`**

Gets all user-defined accumulators for all subtasks of a given vertex. These are the individual accumulators that are returned in aggregated form by the
request `/jobs/<jobid>/vertices/<vertexid>/accumulators`.

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>`**

Summary of the current or latest execution attempt of a specific subtask. See below for a sample.


**`/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>`**

Summary of a specific execution attempt of a specific subtask. Multiple execution attempts happen in case of failure/recovery.

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>/accumulators`**

The accumulators collected for one specific subtask during one specific execution attempt (multiple attempts happen in case of failure/recovery).

Sample Result:

{% highlight json %}
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
{% endhighlight %}

**`/jobs/<jobid>/plan`**

The dataflow plan of a job. The plan is also included in the job summary (`/jobs/<jobid>`).

Sample Result:

{% highlight json %}
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
{% endhighlight %}

### Job Cancellation

#### Cancel Job

`DELETE` request to **`/jobs/:jobid/cancel`**.

Triggers job cancellation, result on success is `{}`.

#### Cancel Job with Savepoint

Triggers a savepoint and cancels the job after the savepoint succeeds.

`GET` request to **`/jobs/:jobid/cancel-with-savepoint/`** triggers a savepoint to the default savepoint directory and cancels the job.

`GET` request to **`/jobs/:jobid/cancel-with-savepoint/target-directory/:targetDirectory`** triggers a savepoint to the given target directory and cancels the job.

Since savepoints can take some time to complete this operation happens asynchronously. The result to this request is the location of the in-progress cancellation.

Sample Trigger Result:

{% highlight json %}
{
  "status": "accepted",
  "request-id": 1,
  "location": "/jobs/:jobid/cancel-with-savepoint/in-progress/1"
}
{% endhighlight %}

##### Monitoring Progress

The progress of the cancellation has to be monitored by the user at

{% highlight json %}
/jobs/:jobid/cancel-with-savepoint/in-progress/:requestId
{% endhighlight %}

The request ID is returned by the trigger result.

###### In-Progress

{% highlight json %}
{
  "status": "in-progress",
  "request-id": 1
}
{% endhighlight %}

###### Success

{% highlight json %}
{
  "status": "success",
  "request-id": 1,
  "savepoint-path": "<savepointPath>"
}
{% endhighlight %}

The `savepointPath` points to the external path of the savepoint, which can be used to resume the savepoint.

###### Failed

{% highlight json %}
{
  "status": "failed",
  "request-id": 1,
  "cause": "<error message>"
}
{% endhighlight %}

### Submitting Programs

It is possible to upload, run, and list Flink programs via the REST APIs and web frontend.

#### Upload a new JAR file

Send a `POST` request to `/jars/upload` with your jar file sent as multi-part data under the `jarfile` file.
Also make sure that the multi-part data includes the `Content-Type` of the file itself, some http libraries do not add the header by default.

The multi-part payload should start like

{% highlight plain %}
------BoundaryXXXX
Content-Disposition: form-data; name="jarfile"; filename="YourFileName.jar"
Content-Type: application/x-java-archive
{% endhighlight %}

#### Run a Program (POST)

Send a `POST` request to `/jars/:jarid/run`. The `jarid` parameter is the file name of the program JAR in the configured web frontend upload directory (configuration key `jobmanager.web.upload.dir`).

You can specify the following query parameters (all optional):

- **Program arguments**: `program-args=arg1 arg2 arg3`
- **Main class to execute**: `entry-class=EntryClassName.class`
- **Default parallelism**: `parallelism=4`
- **Savepoint path to restore from**: `savepointPath=hdfs://path/to/savepoint`
- **Allow non restored state**:  `allowNonRestoredState=true`

If the call succeeds, you will get a response with the ID of the submitted job.

**Example:** Run program with a savepoint

Request:

{% highlight bash %}
POST: /jars/MyProgram.jar/run?savepointPath=/my-savepoints/savepoint-1bae02a80464&allowNonRestoredState=true
{% endhighlight %}

Response:

{% highlight json %}
{"jobid": "869a9868d49c679e7355700e0857af85"}
{% endhighlight %}

### Dispatcher

{% include generated/rest_dispatcher.html %}

{% top %}
