---
title: "JobManager High Availability (HA)"
nav-title: High Availability (HA)
nav-parent_id: deployment
nav-id: ha
nav-show_overview: true
nav-pos: 6
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

JobManager High Availability (HA) hardens a Flink cluster against JobManager failures.
This feature ensures that a Flink cluster will always continue executing your submitted jobs.

* Toc
{:toc}

## JobManager High Availability

The JobManager coordinates every Flink deployment. 
It is responsible for both *scheduling* and *resource management*.

By default, there is a single JobManager instance per Flink cluster. 
This creates a *single point of failure* (SPOF): if the JobManager crashes, no new programs can be submitted and running programs fail.

With JobManager High Availability, you can recover from JobManager failures and thereby eliminate the *SPOF*. 
You can configure high availability for every cluster deployment.
See the [list of available high availability services](#high-availability-services) for more information.

### How to make a cluster highly available

The general idea of JobManager High Availability is that there is a *single leading JobManager* at any time and *multiple standby JobManagers* to take over leadership in case the leader fails. 
This guarantees that there is *no single point of failure* and programs can make progress as soon as a standby JobManager has taken leadership. 

As an example, consider the following setup with three JobManager instances:

<img src="{% link /fig/jobmanager_ha_overview.png %}" class="center" />

Flink's [high availability services](#high-availability-services) encapsulate the required services to make everything work:
* **Leader election**: Selecting a single leader out of a pool of `n` candidates
* **Service discovery**: Retrieving the address of the current leader
* **State persistence**: Persisting state which is required for the successor to resume the job execution (JobGraphs, user code jars, completed checkpoints)

## High Availability Services

Flink ships with two high availability service implementations:

* [ZooKeeper]({% link deployment/ha/zookeeper_ha.zh.md %}): 
ZooKeeper HA services can be used with every Flink cluster deployment. 
They require a running ZooKeeper quorum.  

* [Kubernetes]({% link deployment/ha/kubernetes_ha.zh.md %}):
Kubernetes HA services only work when running on Kubernetes.

## High Availability data lifecycle

In order to recover submitted jobs, Flink persists metadata and the job artifacts.
The HA data will be kept until the respective job either succeeds, is cancelled or fails terminally.
Once this happens, all the HA data, including the metadata stored in the HA services, will be deleted.  

{% top %}
