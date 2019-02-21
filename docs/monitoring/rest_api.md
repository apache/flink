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

The monitoring API is a REST-ful API that accepts HTTP requests and responds with JSON data.

* This will be replaced by the TOC
{:toc}


## Overview

The monitoring API is backed by a web server that runs as part of the *Dispatcher*. By default, this server listens at post `8081`, which can be configured in `flink-conf.yaml` via `rest.port`. Note that the monitoring API web server and the web dashboard web server are currently the same and thus run together at the same port. They respond to different HTTP URLs, though.

In the case of multiple Dispatchers (for high availability), each Dispatcher will run its own instance of the monitoring API, which offers information about completed and running job while that Dispatcher was elected the cluster leader.


## Developing

The REST API backend is in the `flink-runtime` project. The core class is `org.apache.flink.runtime.webmonitor.WebMonitorEndpoint`, which sets up the server and the request routing.

We use *Netty* and the *Netty Router* library to handle REST requests and translate URLs. This choice was made because this combination has lightweight dependencies, and the performance of Netty HTTP is very good.

To add new requests, one needs to
* add a new `MessageHeaders` class which serves as an interface for the new request,
* add a new `AbstractRestHandler` class which handles the request according to the added `MessageHeaders` class,
* add the handler to `org.apache.flink.runtime.webmonitor.WebMonitorEndpoint#initializeHandlers()`.

A good example is the `org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler` that uses the `org.apache.flink.runtime.rest.messages.JobExceptionsHeaders`.


## API

The REST API is versioned, with specific versions being queryable by prefixing the url with the version prefix. Prefixes are always of the form `v[version_number]`.
For example, to access version 1 of `/foo/bar` one would query `/v1/foo/bar`.

If no version is specified Flink will default to the *oldest* version supporting the request.

Querying unsupported/non-existing versions will return a 404 error.

<div class="codetabs" markdown="1">

<div data-lang="v1" markdown="1">
#### Dispatcher

{% include generated/rest_v1_dispatcher.html %}
</div>

</div>

