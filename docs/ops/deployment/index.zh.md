---
title: "集群与部署"
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
        <br><a href="{{ site.baseurl }}/ops/deployment/local.html">Learn more</a>
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
        <br><a href="{{ site.baseurl }}/ops/deployment/cluster_setup.html">Learn more</a>
      </div>
    </div>
  </div>
  <div class="col-sm-4">
    <div class="panel panel-default">
      <div class="panel-heading">
        <b>Yarn</b>
      </div>
      <div class="panel-body">
        Deploy Flink on-top Apache Hadoop's resource manager 
        <br><a href="{{ site.baseurl }}/ops/deployment/yarn_setup.html">Learn more</a>
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
        <br><a href="{{ site.baseurl }}/ops/deployment/mesos.html">Learn more</a>
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
        <br><a href="{{ site.baseurl }}/ops/deployment/docker.html">Learn more</a>
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
        <br><a href="{{ site.baseurl }}/ops/deployment/kubernetes.html">Learn more</a>
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
