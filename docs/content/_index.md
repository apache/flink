---
title: Apache Flink Documentation 
type: docs
bookToc: false
aliases:
  - /examples/index.html
  - /getting-started/examples/index.html
  - /dev/execution_plans.html
  - /docs/dev/execution/execution_plans/
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

# Apache Flink Documentation

{{< center >}}
**Apache Flink** is a framework and distributed processing engine for stateful computations over *unbounded* and *bounded* data streams. Flink has been designed to run in *all common cluster environments* perform computations at *in-memory* speed and at *any scale*.
{{< /center >}}

{{< columns >}}

### Try Flink

If you’re interested in playing around with Flink, try one of our tutorials:

* [Fraud Detection with the DataStream API]({{< ref "docs/try-flink/datastream" >}})
* [Real Time Reporting with the Table API]({{< ref "docs/try-flink/table_api" >}})
* [Intro to PyFlink]({{< ref "docs/dev/python/overview" >}})
* [Flink Operations Playground]({{< ref "docs/try-flink/flink-operations-playground" >}})

### Learn Flink

* To dive in deeper, the [Hands-on Training]({{< ref "docs/learn-flink/overview" >}}) includes a set of lessons and exercises that provide a step-by-step introduction to Flink.

* The [Concepts]({{< ref "docs/concepts/overview" >}}) section explains what you need to know about Flink before exploring the reference documentation.

### Get Help with Flink

If you get stuck, check out our [community support resources](https://flink.apache.org/community.html). In particular, Apache Flink’s user mailing list is consistently ranked as one of the most active of any Apache project, and is a great way to get help quickly.

<--->

### Explore Flink

The reference documentation covers all the details. Some starting points:

{{< columns >}}
* [DataStream API]({{< ref "docs/dev/datastream/overview" >}})
* [Table API & SQL]({{< ref "docs/dev/table/overview" >}})
* [Stateful Functions](https://nightlies.apache.org/flink/flink-statefun-docs-stable/)

<--->

* [Configuration]({{< ref "docs/deployment/config" >}})
* [Rest API]({{< ref "docs/ops/rest_api" >}})
* [CLI]({{< ref "docs/deployment/cli" >}})
{{< /columns >}}

### Deploy Flink

Before putting your Flink job into production, read the [Production Readiness Checklist]({{< ref "docs/ops/production_ready" >}}).
For an overview of possible deployment targets, see [Clusters and Deployments]({{< ref "docs/deployment/overview" >}}).

### Upgrade Flink

Release notes cover important changes between Flink versions. Please read them carefully if you plan to upgrade your Flink setup.

<!--
For some reason Hugo will only allow linking to the 
release notes if there is a leading '/' and file extension.
-->
See the release notes for [Flink 1.14]({{< ref "/release-notes/flink-1.14.md" >}}), [Flink 1.13]({{< ref "/release-notes/flink-1.13.md" >}}), [Flink 1.12]({{< ref "/release-notes/flink-1.12.md" >}}), [Flink 1.11]({{< ref "/release-notes/flink-1.11.md" >}}), [Flink 1.10]({{< ref "/release-notes/flink-1.10.md" >}}), [Flink 1.9]({{< ref "/release-notes/flink-1.9.md" >}}), [Flink 1.8]({{< ref "/release-notes/flink-1.8.md" >}}), or [Flink 1.7]({{< ref "/release-notes/flink-1.7.md" >}}).

{{< /columns >}}

{{< build_time >}}
