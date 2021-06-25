---
title: '概览'
weight: 1
type: docs
aliases:
  - /zh/deployment/
  - /zh/apis/cluster_execution.html
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

# Deployment

Flink is a versatile framework, supporting many different deployment scenarios in a mix and match fashion.

Below, we briefly explain the building blocks of a Flink cluster, their purpose and available implementations.
If you just want to start Flink locally, we recommend setting up a [Standalone Cluster]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}).

## Overview and Reference Architecture

The figure below shows the building blocks of every Flink cluster. There is always somewhere a client running. It takes the code of the Flink applications, transforms it into a JobGraph and submits it to the JobManager.

The JobManager distributes the work onto the TaskManagers, where the actual operators (such as sources, transformations and sinks) are running.

When deploying Flink, there are often multiple options available for each building block. We have listed them in the table below the figure.


<!-- Image source: https://docs.google.com/drawings/d/1s_ZlXXvADqxWfTMNRVwQeg7HZ3hN1Xb7goxDPjTEPrI/edit?usp=sharing -->
{{< img class="img-fluid" width="80%" src="/fig/deployment_overview.svg" alt="Figure for Overview and Reference Architecture" >}}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Component</th>
      <th class="text-left" style="width: 50%">Purpose</th>
      <th class="text-left">Implementations</th>
    </tr>
   </thead>
   <tbody>
        <tr>
            <td>Flink Client</td>
            <td>
              Compiles batch or streaming applications into a dataflow graph, which it then submits to the JobManager.
            </td>
            <td>
                <ul>
                    <li><a href="{{< ref "docs/deployment/cli" >}}">Command Line Interface</a></li>
                    <li><a href="{{< ref "docs/ops/rest_api" >}}">REST Endpoint</a></li>
                    <li><a href="{{< ref "docs/dev/table/sqlClient" >}}">SQL Client</a></li>
                    <li><a href="{{< ref "docs/deployment/repls/python_shell" >}}">Python REPL</a></li>
                    <li><a href="{{< ref "docs/deployment/repls/scala_shell" >}}">Scala REPL</a></li>
                </ul>
            </td>
        </tr>
        <tr>
            <td>JobManager</td>
            <td>
                JobManager is the name of the central work coordination component of Flink. It has implementations for different resource providers, which differ on high-availability, resource allocation behavior and supported job submission modes. <br />
                JobManager <a href="#deployment-modes">modes for job submissions</a>:
                <ul>
                    <li><b>Application Mode</b>: runs the cluster exclusively for one application. The job's main method (or client) gets executed on the JobManager. Calling `execute`/`executeAsync` multiple times in an application is supported.</li>
                    <li><b>Per-Job Mode</b>: runs the cluster exclusively for one job. The job's main method (or client) runs only prior to the cluster creation.</li>
                    <li><b>Session Mode</b>: one JobManager instance manages multiple jobs sharing the same cluster of TaskManagers</li>
                </ul>
            </td>
            <td>
                <ul id="jmimpls">
                    <li><a href="{{< ref "docs/deployment/resource-providers/standalone/" >}}">Standalone</a> (this is the barebone mode that requires just JVMs to be launched. Deployment with <a href="{{< ref "docs/deployment/resource-providers/standalone/docker" >}}">Docker, Docker Swarm / Compose</a>, <a href="{{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}">non-native Kubernetes</a> and other models is possible through manual setup in this mode)
                    </li>
                    <li><a href="{{< ref "docs/deployment/resource-providers/native_kubernetes" >}}">Kubernetes</a></li>
                    <li><a href="{{< ref "docs/deployment/resource-providers/yarn" >}}">YARN</a></li>
                    <li><a href="{{< ref "docs/deployment/resource-providers/mesos" >}}">Mesos</a></li>
                </ul>
            </td>
        </tr>
        <tr>
            <td>TaskManager</td>
            <td>
                TaskManagers are the services actually performing the work of a Flink job.
            </td>
            <td>
            </td>
        </tr>
        <tr>
            <td colspan="3" class="text-center">
                <b>External Components</b> (all optional)
            </td>
        </tr>
        <tr>
            <td>High Availability Service Provider</td>
            <td>
                Flink's JobManager can be run in high availability mode which allows Flink to recover from JobManager faults. In order to failover faster, multiple standby JobManagers can be started to act as backups.
            </td>
            <td>
                <ul>
                    <li><a href="{{< ref "docs/deployment/ha/zookeeper_ha" >}}">Zookeeper</a></li>
                    <li><a href="{{< ref "docs/deployment/ha/kubernetes_ha" >}}">Kubernetes HA</a></li>
                </ul>
            </td>
        </tr>
        <tr>
            <td>File Storage and Persistency</td>
            <td>
                For checkpointing (recovery mechanism for streaming jobs) Flink relies on external file storage systems
            </td>
            <td>See <a href="{{< ref "docs/deployment/filesystems/overview" >}}">FileSystems</a> page.</td>
        </tr>
        <tr>
            <td>Resource Provider</td>
            <td>
                Flink can be deployed through different Resource Provider Frameworks, such as Kubernetes, YARN or Mesos.
            </td>
            <td>See <a href="#jmimpls">JobManager</a> implementations above.</td>
        </tr>
        <tr>
            <td>Metrics Storage</td>
            <td>
                Flink components report internal metrics and Flink jobs can report additional, job specific metrics as well.
            </td>
            <td>See <a href="{{< ref "docs/deployment/metric_reporters" >}}">Metrics Reporter</a> page.</td>
        </tr>
        <tr>
            <td>Application-level data sources and sinks</td>
            <td>
                While application-level data sources and sinks are not technically part of the deployment of Flink cluster components, they should be considered when planning a new Flink production deployment. Colocating frequently used data with Flink can have significant performance benefits
            </td>
            <td>
                For example:
                <ul>
                    <li>Apache Kafka</li>
                    <li>Amazon S3</li>
                    <li>ElasticSearch</li>
                    <li>Apache Cassandra</li>
                </ul>
                See <a href="{{< ref "docs/connectors/datastream/overview" >}}">Connectors</a> page.
            </td>
        </tr>
    </tbody>
</table>



## Deployment Modes

Flink can execute applications in one of three ways:
- in Application Mode,
- in a Per-Job Mode,
- in Session Mode.

 The above modes differ in:
 - the cluster lifecycle and resource isolation guarantees
 - whether the application's `main()` method is executed on the client or on the cluster.


<!-- Image source: https://docs.google.com/drawings/d/1EfloufuOp1A7YDwZmBEsHKRLIrrbtRkoWRPcfZI5RYQ/edit?usp=sharing -->
{{< img class="img-fluid" width="80%" style="margin: 15px" src="/fig/deployment_modes.svg" alt="Figure for Deployment Modes" >}}

#### Application Mode
    
In all the other modes, the application's `main()` method is executed on the client side. This process 
includes downloading the application's dependencies locally, executing the `main()` to extract a representation
of the application that Flink's runtime can understand (i.e. the `JobGraph`) and ship the dependencies and
the `JobGraph(s)` to the cluster. This makes the Client a heavy resource consumer as it may need substantial
network bandwidth to download dependencies and ship binaries to the cluster, and CPU cycles to execute the
`main()`. This problem can be more pronounced when the Client is shared across users.

Building on this observation, the *Application Mode* creates a cluster per submitted application, but this time,
the `main()` method of the application is executed on the JobManager. Creating a cluster per application can be 
seen as creating a session cluster shared only among the jobs of a particular application, and torn down when
the application finishes. With this architecture, the *Application Mode* provides the same resource isolation
and load balancing guarantees as the *Per-Job* mode, but at the granularity of a whole application. Executing 
the `main()` on the JobManager allows for saving the CPU cycles required, but also save the bandwidth required
for downloading the dependencies locally. Furthermore, it allows for more even spread of the network load for
downloading the dependencies of the applications in the cluster, as there is one JobManager per application.

{{< hint info >}}
In the Application Mode, the `main()` is executed on the cluster and not on the client, 
as in the other modes. This may have implications for your code as, for example, any paths you register in 
your environment using the `registerCachedFile()` must be accessible by the JobManager of your application.
{{< /hint >}}

Compared to the *Per-Job* mode, the *Application Mode* allows the submission of applications consisting of
multiple jobs. The order of job execution is not affected by the deployment mode but by the call used
to launch the job. Using `execute()`, which is blocking, establishes an order and it will lead to the 
execution of the "next"  job being postponed until "this" job finishes. Using `executeAsync()`, which is 
non-blocking, will lead to the "next" job starting before "this" job finishes.

{{< hint warning >}}
The Application Mode allows for multi-`execute()` applications but 
High-Availability is not supported in these cases. High-Availability in Application Mode is only
supported for single-`execute()` applications.

Additionally, when any of multiple running jobs in Application Mode (submitted for example using 
`executeAsync()`) gets cancelled, all jobs will be stopped and the JobManager will shut down. 
Regular job completions (by the sources shutting down) are supported.
{{< /hint >}}

#### Per-Job Mode

Aiming at providing better resource isolation guarantees, the *Per-Job* mode uses the available resource provider
framework (e.g. YARN, Kubernetes) to spin up a cluster for each submitted job. This cluster is available to 
that job only. When the job finishes, the cluster is torn down and any lingering resources (files, etc) are
cleared up. This provides better resource isolation, as a misbehaving job can only bring down its own 
TaskManagers. In addition, it spreads the load of book-keeping across multiple JobManagers, as there is 
one per job. For these reasons, the *Per-Job* resource allocation model is the preferred mode by many 
production reasons.

#### Session Mode

*Session mode* assumes an already running cluster and uses the resources of that cluster to execute any 
submitted application. Applications executed in the same (session) cluster use, and consequently compete
for, the same resources. This has the advantage that you do not pay the resource overhead of spinning up
a full cluster for every submitted job. But, if one of the jobs misbehaves or brings down a TaskManager,
then all jobs running on that TaskManager will be affected by the failure. This, apart from a negative
impact on the job that caused the failure, implies a potential massive recovery process with all the 
restarting jobs accessing the filesystem concurrently and making it unavailable to other services. 
Additionally, having a single cluster running multiple jobs implies more load for the JobManager, who 
is responsible for the book-keeping of all the jobs in the cluster.


#### Summary

In *Session Mode*, the cluster lifecycle is independent of that of any job running on the cluster
and the resources are shared across all jobs. The *Per-Job* mode pays the price of spinning up a cluster
for every submitted job, but this comes with better isolation guarantees as the resources are not shared 
across jobs. In this case, the lifecycle of the cluster is bound to that of the job. Finally, the 
*Application Mode* creates a session cluster per application and executes the application's `main()` 
method on the cluster.



## Vendor Solutions

A number of vendors offer managed or fully hosted Flink solutions.
None of these vendors are officially supported or endorsed by the Apache Flink PMC.
Please refer to vendor maintained documentation on how to use these products. 

<!--
Please keep this list in alphabetical order
-->

#### AliCloud Realtime Compute

[Website](https://www.alibabacloud.com/products/realtime-compute)

Supported Environments:
{{< label AliCloud >}}

#### Amazon EMR

[Website](https://aws.amazon.com/emr/)

Supported Environments:
{{< label AWS >}}

#### Amazon Kinesis Data Analytics for Apache Flink

[Website](https://docs.aws.amazon.com/kinesisanalytics/latest/java/what-is.html)

Supported Environments:
{{< label AWS >}}

#### Cloudera DataFlow

[Website](https://www.cloudera.com/products/cdf.html)

Supported Environment:
{{< label AWS >}}
{{< label Azure >}}
{{< label Google Cloud >}}
{{< label On-Premise >}}

#### Eventador

[Website](https://eventador.io)

Supported Environment:
{{< label AWS >}}

#### Huawei Cloud Stream Service

[Website](https://www.huaweicloud.com/en-us/product/cs.html)

Supported Environment:
{{< label Huawei Cloud >}}

#### Ververica Platform

[Website](https://www.ververica.com/platform-overview)

Supported Environments:
{{< label AliCloud >}}
{{< label AWS >}}
{{< label Azure >}}
{{< label Google Cloud >}}
{{< label On-Premise >}}

{{< top >}}
