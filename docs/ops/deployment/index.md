---
title: "Clusters & Deployment"
nav-id: deployment
nav-parent_id: ops
nav-pos: 1
nav-show_overview: true
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

When deciding how and where to run Flink, there's a wide range of options available.

* This will be replaced by the TOC
{:toc}

## Deployment Modes

Flink can execute applications in one of three ways:
 - in Session Mode, 
 - in a Per-Job Mode, or
 - in Application Mode.

 The above modes differ in:
 - the cluster lifecycle and resource isolation guarantees
 - whether the application's `main()` method is executed on the client or on the cluster.

#### Session Mode

*Session mode* assumes an already running cluster and uses the resources of that cluster to execute any 
submitted application. Applications executed in the same (session) cluster use, and consequently compete
for, the same resources. This has the advantage that you do not pay the resource overhead of spinning up
a full cluster for every submitted job. But, if one of the jobs misbehaves or brings down a Task Manager,
then all jobs running on that Task Manager will be affected by the failure. This, apart from a negative
impact on the job that caused the failure, implies a potential massive recovery process with all the 
restarting jobs accessing the filesystem concurrently and making it unavailable to other services. 
Additionally, having a single cluster running multiple jobs implies more load for the JobManager, who 
is responsible for the book-keeping of all the jobs in the cluster.

#### Per-Job Mode

Aiming at providing better resource isolation guarantees, the *Per-Job* mode uses the available cluster manager
framework (e.g. YARN, Kubernetes) to spin up a cluster for each submitted job. This cluster is available to 
that job only. When the job finishes, the cluster is torn down and any lingering resources (files, etc) are
cleared up. This provides better resource isolation, as a misbehaving job can only bring down its own 
Task Managers. In addition, it spreads the load of book-keeping across multiple JobManagers, as there is 
one per job. For these reasons, the *Per-Job* resource allocation model is the preferred mode by many 
production reasons.

#### Application Mode
    
In all the above modes, the application's `main()` method is executed on the client side. This process 
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
for downloading the dependencies locally. Furthermore, it allows for more even spread of the network load of
downloading the dependencies of the applications in the cluster, as there is one JobManager per application.

<div class="alert alert-info" markdown="span">
  <strong>Note:</strong> In the Application Mode, the `main()` is executed on the cluster and not on the client, 
  as in the other modes. This may have implications for your code as, for example, any paths you register in 
  your environment using the `registerCachedFile()` must be accessible by the JobManager of your application.
</div>

Compared to the *Per-Job* mode, the *Application Mode* allows the submission of applications consisting of
multiple jobs. The order of job execution is not affected by the deployment mode but by the call used
to launch the job. Using `execute()`, which is blocking, establishes an order and it will lead to the 
execution of the "next"  job being postponed until "this" job finishes. Using `executeAsync()`, which is 
non-blocking, will lead to the "next" job starting before "this" job finishes.

<div class="alert alert-info" markdown="span">
  <strong>Attention:</strong> The Application Mode allows for multi-`execute()` applications but 
  High-Availability is not supported in these cases. High-Availability in Application Mode is only
  supported for single-`execute()` applications.
</div>

#### Summary

In *Session Mode*, the cluster lifecycle is independent of that of any job running on the cluster
and the resources are shared across all jobs. The *Per-Job* mode pays the price of spinning up a cluster
for every submitted job, but this comes with better isolation guarantees as the resources are not shared 
across jobs. In this case, the lifecycle of the cluster is bound to that of the job. Finally, the 
*Application Mode* creates a session cluster per application and executes the application's `main()` 
method on the cluster.

## Deployment Targets

Apache Flink ships with first class support for a number of common deployment targets.

<div class="row">
  <div class="col-sm-4">
    <div class="panel panel-default">
      <div class="panel-heading">
        <b>Local</b>
      </div>
      <div class="panel-body">
        Run Flink locally for basic testing and experimentation
        <br><a href="{% link ops/deployment/local.md %}">Learn more</a>
      </div>
    </div>
  </div>
  <div class="col-sm-4">
    <div class="panel panel-default">
      <div class="panel-heading">
        <b>Standalone</b>
      </div>
      <div class="panel-body">
        A simple solution for running Flink on bare metal or VM's 
        <br><a href="{% link ops/deployment/cluster_setup.md %}">Learn more</a>
      </div>
    </div>
  </div>
  <div class="col-sm-4">
    <div class="panel panel-default">
      <div class="panel-heading">
        <b>Yarn</b>
      </div>
      <div class="panel-body">
        Deploy Flink on-top of Apache Hadoop's resource manager 
        <br><a href="{% link ops/deployment/yarn_setup.md %}">Learn more</a>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col-sm-4">
    <div class="panel panel-default">
      <div class="panel-heading">
        <b>Mesos</b>
      </div>
      <div class="panel-body">
        A generic resource manager for running distriubted systems
        <br><a href="{% link ops/deployment/mesos.md %}">Learn more</a>
      </div>
    </div>
  </div>
  <div class="col-sm-4">
    <div class="panel panel-default">
      <div class="panel-heading">
        <b>Docker</b>
      </div>
      <div class="panel-body">
        A popular solution for running Flink within a containerized environment
        <br><a href="{% link ops/deployment/docker.md %}">Learn more</a>
      </div>
    </div>
  </div>
  <div class="col-sm-4">
    <div class="panel panel-default">
      <div class="panel-heading">
        <b>Kubernetes</b>
      </div>
      <div class="panel-body">
        An automated system for deploying containerized applications
        <br><a href="{% link ops/deployment/kubernetes.md %}">Learn more</a>
      </div>
    </div>
  </div>
</div>

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
<span class="label label-primary">AliCloud</span>

#### Amazon EMR

[Website](https://aws.amazon.com/emr/)

Supported Environments:
<span class="label label-primary">AWS</span>

#### Amazon Kinesis Data Analytics For Java 

[Website](https://docs.aws.amazon.com/kinesisanalytics/latest/java/what-is.html)

Supported Environments:
<span class="label label-primary">AWS</span>

#### Cloudera

[Website](https://www.cloudera.com/)

Supported Environment:
<span class="label label-primary">AWS</span>
<span class="label label-primary">Azure</span>
<span class="label label-primary">Google Cloud</span>
<span class="label label-primary">On-Premise</span>

#### Eventador

[Website](https://eventador.io)

Supported Environment:
<span class="label label-primary">AWS</span>

#### Huawei Cloud Stream Service

[Website](https://www.huaweicloud.com/en-us/product/cs.html)

Supported Environment:
<span class="label label-primary">Huawei Cloud</span>

#### Ververica Platform

[Website](https://www.ververica.com/platform-overview)

Supported Environments:
<span class="label label-primary">AliCloud</span>
<span class="label label-primary">AWS</span>
<span class="label label-primary">Azure</span>
<span class="label label-primary">Google Cloud</span>
<span class="label label-primary">On-Premise</span>

## Deployment Best Practices

### How to provide dependencies in the classpath

Flink provides several approaches for providing dependencies (such as `*.jar` files or static data) to Flink or user-provided
applications. These approaches differ based on the deployment mode and target, but also have commonalities, which are described here.

To provide a dependency, there are the following options:
- files in the **`lib/` folder** are added to the classpath used to start Flink. It is suitable for libraries such as Hadoop or file systems not available as plugins. Beware that classes added here can potentially interfere with Flink, for example if you are adding a different version of a library already provided by Flink.

- **`plugins/<name>/`** are loaded at runtime by Flink through separate classloaders to avoid conflicts with classes loaded and used by Flink. Only jar files which are prepared as [plugins]({% link ops/plugins.md %}) can be added here.

### Download Maven dependencies locally

If you need to extend the Flink with a Maven dependency (and its transitive dependencies),
you can use an [Apache Maven](https://maven.apache.org) *pom.xml* file to download all required files into a local folder:

*pom.xml*:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.apache.flink</groupId>
  <artifactId>docker-dependencies</artifactId>
  <version>1.0-SNAPSHOT</version>

  <dependencies>
        <!-- Put your dependency here, for example a Hadoop GCS connector -->
  </dependencies>

  <build>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-dependency-plugin</artifactId>
          <version>3.1.2</version>
          <executions>
            <execution>
              <id>copy-dependencies</id>
              <phase>package</phase>
              <goals><goal>copy-dependencies</goal></goals>
              <configuration><outputDirectory>jars</outputDirectory></configuration>
            </execution>
          </executions>
        </plugin>
      </plugins>
  </build>
</project>
```

Running `mvn package` in the same directory will create a `jars/` folder containing all the jar files, 
which you can add to the desired folder, Docker image etc.
