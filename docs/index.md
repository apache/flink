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

<p style="margin: 30px 60px 0 60px;text-align: center" markdown="1">
Apache Flink is a framework and distributed processing engine for stateful computations over _unbounded and bounded_ data streams. Flink has been designed to run in _all common cluster environments_, perform computations at _in-memory speed_ and at _any scale_.
</p>

<div class="row">
<div class="col-sm-6" markdown="1">

### Try Flink

If you’re interested in playing around with Flink, try one of our tutorials:

* [Local Installation]({% link try-flink/local_installation.md %})
* [Fraud Detection with the DataStream API]({% link try-flink/datastream_api.md %})
* [Real Time Reporting with the Table API]({% link try-flink/table_api.md %})
* [Intro to the Python Table API]({% link try-flink/python_api.md %})
* [Flink Operations Playground]({% link try-flink/flink-operations-playground.md %})

### Learn Flink

* To dive in deeper, the [Hands-on Training]({% link learn-flink/index.md %}) includes a set of lessons and exercises that provide a step-by-step introduction to Flink.

* The [Concepts]({% link concepts/index.md %}) section explains what you need to know about Flink before exploring the reference documentation.

### Get Help with Flink

If you get stuck, check out our [community support resources](https://flink.apache.org/community.html). In particular, Apache Flink’s user mailing list is consistently ranked as one of the most active of any Apache project, and is a great way to get help quickly.

</div>
<div class="col-sm-6" markdown="1">

### Explore Flink

The reference documentation covers all the details. Some starting points:

<div class="row">
<div class="col-sm-6" markdown="1">

* [DataStream API]({% link dev/datastream_api.md %})
* [Table API &amp; SQL]({% link dev/table/index.md %})
* [Stateful Functions]({% if site.is_stable %} {{ site.statefundocs_stable_baseurl }} {% else %} {{ site.statefundocs_baseurl }} {% endif %})

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

Release notes cover important changes between Flink versions. Please read them carefully if you plan to upgrade your Flink setup.

See the release notes for [Flink 1.11]({% link release-notes/flink-1.11.md %}), [Flink 1.10]({% link release-notes/flink-1.10.md %}), [Flink 1.9]({% link release-notes/flink-1.9.md %}), [Flink 1.8]({% link release-notes/flink-1.8.md %}), or [Flink 1.7]({% link release-notes/flink-1.7.md %}).

</div>
</div>

<div style="margin: 40px 0 0 0; position: relative; top: 20px;">
<p>
This documentation is for Apache Flink version {{ site.version_title }}. These pages were built at: {% build_time %}.
</p>
</div>
