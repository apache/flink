---
title: "History Server"
nav-parent_id: advanced
nav-pos: 2
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

Flink has a history server that can be used to query the statistics of completed jobs after the corresponding Flink cluster has been shut down.

Furthermore, it exposes a REST API that accepts HTTP requests and responds with JSON data.

* This will be replaced by the TOC
{:toc}

## Overview

The HistoryServer allows you to query the status and statistics of completed jobs that have been archived by a JobManager.

After you have configured the HistoryServer *and* JobManager, you start and stop the HistoryServer via its corresponding startup script:

{% highlight shell %}
# Start or stop the HistoryServer
bin/historyserver.sh (start|start-foreground|stop)
{% endhighlight %}

By default, this server binds to `localhost` and listens at port `8082`.

Currently, you can only run it as a standalone process.

## Configuration

The configuration keys `jobmanager.archive.fs.dir` and `historyserver.archive.fs.refresh-interval` need to be adjusted for archiving and displaying archived jobs.

**JobManager**

The archiving of completed jobs happens on the JobManager, which uploads the archived job information to a file system directory. You can configure the directory to archive completed jobs in `flink-conf.yaml` by setting a directory via `jobmanager.archive.fs.dir`.

{% highlight yaml %}
# Directory to upload completed job information
jobmanager.archive.fs.dir: hdfs:///completed-jobs
{% endhighlight %}

**HistoryServer**

The HistoryServer can be configured to monitor a comma-separated list of directories in via `historyserver.archive.fs.dir`. The configured directories are regularly polled for new archives; the polling interval can be configured via `historyserver.archive.fs.refresh-interval`.

{% highlight yaml %}
# Monitor the following directories for completed jobs
historyserver.archive.fs.dir: hdfs:///completed-jobs

# Refresh every 10 seconds
historyserver.archive.fs.refresh-interval: 10000
{% endhighlight %}

The contained archives are downloaded and cached in the local filesystem. The local directory for this is configured via `historyserver.web.tmpdir`.

Check out the configuration page for a [complete list of configuration options]({% link deployment/config.md %}#history-server).

## Available Requests

Below is a list of available requests, with a sample JSON response. All requests are of the sample form `http://hostname:8082/jobs`, below we list only the *path* part of the URLs.

Values in angle brackets are variables, for example `http://hostname:port/jobs/<jobid>/exceptions` will have to requested for example as `http://hostname:port/jobs/7684be6004e4e955c2a558a9bc463f65/exceptions`.

  - `/config`
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

{% top %}
