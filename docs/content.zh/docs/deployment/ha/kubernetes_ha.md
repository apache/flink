---
title: Kubernetes HA Services
weight: 3
type: docs
aliases:
  - /zh/deployment/ha/kubernetes_ha.html
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

# Kubernetes HA Services

Flink's Kubernetes HA services use [Kubernetes](https://kubernetes.io/) for high availability services.

Kubernetes high availability services can only be used when deploying to Kubernetes.
Consequently, they can be configured when using [standalone Flink on Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}) or the [native Kubernetes integration]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})

## Prerequisites

In order to use Flink's Kubernetes HA services you must fulfill the following prerequisites:

- Kubernetes >= 1.9.
- Service account with permissions to create, edit, delete ConfigMaps.
  Take a look at how to configure a service account for [Flink's native Kubernetes integration]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}#rbac) and [standalone Flink on Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}#kubernetes-high-availability-services) for more information.


## Configuration

In order to start an HA-cluster you have to configure the following configuration keys:

- [high-availability]({{< ref "docs/deployment/config" >}}#high-availability-1) (required): 
The `high-availability` option has to be set to `KubernetesHaServicesFactory`.

```yaml
high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
```

- [high-availability.storageDir]({{< ref "docs/deployment/config" >}}#high-availability-storagedir) (required): 
JobManager metadata is persisted in the file system `high-availability.storageDir` and only a pointer to this state is stored in Kubernetes.

```yaml
high-availability.storageDir: s3:///flink/recovery
```

The `storageDir` stores all metadata needed to recover a JobManager failure.
  
- [kubernetes.cluster-id]({{< ref "docs/deployment/config" >}}#kubernetes-cluster-id) (required):
In order to identify the Flink cluster, you have to specify a `kubernetes.cluster-id`.

```yaml
kubernetes.cluster-id: cluster1337
```

### Example configuration

Configure high availability mode in `conf/flink-conf.yaml`:

```yaml
kubernetes.cluster-id: <cluster-id>
high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
high-availability.storageDir: hdfs:///flink/recovery
```

{{< top >}}

## High availability data clean up

To keep HA data while restarting the Flink cluster, simply delete the deployment (via `kubectl delete deployment <cluster-id>`). 
All the Flink cluster related resources will be deleted (e.g. JobManager Deployment, TaskManager pods, services, Flink conf ConfigMap). 
HA related ConfigMaps will be retained because they do not set the owner reference. 
When restarting the cluster, all previously running jobs will be recovered and restarted from the latest successful checkpoint.

{{< top >}} 
