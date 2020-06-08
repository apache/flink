---
title: "Apache Flink Documentation"
nav-pos: 0
nav-title: '<i class="fa fa-home title" aria-hidden="true"></i> Home'
nav-parent_id: root
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

This documentation is for Apache Flink version {{ site.version_title }}. These pages were built at: {% build_time %}.

<div class="row">
<div class="col-sm-6" markdown="1">

### Try Flink

Apache Flink is an open source platform with expressive APIs for defining stream and batch data processing jobs, and a robust and scalable engine for executing those jobs.

To quickly get a general understanding of how Flink works and what it offers for your use case, work your way through one of these tutorials:

* [Fraud Detection with the DataStream API]({% link try-flink/datastream_api.md %})
* [Real Time Reporting with the Table API]({% link try-flink/table_api.md %})
* [Intro to the Python Table API]({% link try-flink/python_table_api.md %})
* [Flink Operations Playground]({% link try-flink/flink-operations-playground.md %})

### Learn Flink

* Our [Self-paced Training Course]({% link learn-flink/index.md %}) includes a set of lessons and hands-on exercises that provide a step-by-step introduction to Flink.

* The [Concepts]({% link concepts/index.md %}) section covers what you need to know about Flink before exploring the reference documentation.

</div>
<div class="col-sm-6" markdown="1">

### Explore Flink

The reference documentation covers all the details. Some starting points:

<div class="row">
<div class="col-sm-6" markdown="1">

* [DataStream API]({% link dev/datastream_api.md %})
* [DataSet API]({% link dev/batch/index.md %})
* [Table API &amp; SQL]({% link dev/table/index.md %})

</div>
<div class="col-sm-6" markdown="1">

* [Configuration]({% link ops/config.md %})
* [Rest API]({% link monitoring/rest_api.md %})
* [CLI]({% link ops/cli.md %})

</div>
</div>

### Deploy Flink

Before putting your Flink job into production, read the [Production Readiness Checklist]({% link ops/production_ready.md %}). For an overview of possible deployment targets, see [Clusters and Deployments]({% link ops/deployment/index.md %}). 

### Upgrade Flink

Release notes cover important changes between Flink versions. Please read them carefully if you plan to upgrade your Flink setup to a more recent version.

See the release notes for [Flink 1.10]({% link release-notes/flink-1.10.md %}), [Flink 1.9]({% link release-notes/flink-1.9.md %}), [Flink 1.8]({% link release-notes/flink-1.8.md %}), or [Flink 1.7]({% link release-notes/flink-1.7.md %}).

</div>
</div>

