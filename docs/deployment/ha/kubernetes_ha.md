---
title: "Kubernetes HA Services"
nav-title: Kubernetes HA Services
nav-parent_id: ha
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

Flink's Kubernetes HA services use [Kubernetes](https://kubernetes.io/) for high availability services.

* Toc
{:toc}

Kubernetes high availability services can only be used when deploying to Kubernetes.
Consequently, they can be configured when using [standalone Flink on Kubernetes]({% link deployment/resource-providers/standalone/kubernetes.md %}) or the [native Kubernetes integration]({% link deployment/resource-providers/native_kubernetes.md %})

## Configuration

In order to start an HA-cluster you have to configure the following configuration keys:

- [high-availability]({% link deployment/config.md %}#high-availability-1) (required): 
The `high-availability` option has to be set to `KubernetesHaServicesFactory`.

  <pre>high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory</pre>

- [high-availability.storageDir]({% link deployment/config.md %}#high-availability-storagedir) (required): 
JobManager metadata is persisted in the file system `high-availability.storageDir` and only a pointer to this state is stored in Kubernetes.

  <pre>high-availability.storageDir: s3:///flink/recovery</pre>

  The `storageDir` stores all metadata needed to recover a JobManager failure.
  
- [kubernetes.cluster-id]({% link deployment/config.md %}#kubernetes-cluster-id) (required):
In order to identify the Flink cluster, you have to specify a `kubernetes.cluster-id`.

  <pre>kubernetes.cluster-id: cluster1337</pre>

### Example configuration

Configure high availability mode in `conf/flink-conf.yaml`:

{% highlight bash %}
kubernetes.cluster-id: <cluster-id>
high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
high-availability.storageDir: hdfs:///flink/recovery
{% endhighlight %}

{% top %}

## High availability data clean up

To keep HA data while restarting the Flink cluster, simply delete the deployment (via `kubectl delete deploy <cluster-id>`). 
All the Flink cluster related resources will be deleted (e.g. JobManager Deployment, TaskManager pods, services, Flink conf ConfigMap). 
HA related ConfigMaps will be retained because they do not set the owner reference. 
When restarting the cluster, all previously running jobs will be recovered and restarted from the latest successful checkpoint.

{% top %} 
