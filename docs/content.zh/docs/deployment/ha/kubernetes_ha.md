---
title: Kubernetes 高可用服务
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

# Kubernetes 高可用服务

Flink 的 Kubernetes 高可用模式使用 [Kubernetes](https://kubernetes.io/) 提供高可用服务。

Kubernetes 高可用服务只能在部署到 Kubernetes 时使用。因此，当使用 [在 Kubernetes 上单节点部署 Flink]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}) 或 [Flink 原生 Kubernetes 集成]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}) 两种模式时，可以对它们进行配置。

## 准备

为了使用 Flink 的 Kubernetes 高可用服务，你必须满足以下先决条件:

- Kubernetes >= 1.9.
- 具有创建、编辑、删除 ConfigMaps 权限的服务帐户。想了解更多信息，请查看如何在 [Flink 原生 Kubernetes 集成]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}#rbac) 和 [在 Kubernetes 上单节点部署 Flink]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}#kubernetes-high-availability-services) 两种模式中配置服务帐户。


## 配置

为了启用高可用集群（HA-cluster），你必须设置以下配置项:

- [high-availability]({{< ref "docs/deployment/config" >}}#high-availability-1) (必要的):
`high-availability` 选项必须设置为 `KubernetesHaServicesFactory`.

```yaml
high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
```

- [high-availability.storageDir]({{< ref "docs/deployment/config" >}}#high-availability-storagedir) (必要的):
JobManager 元数据持久化到文件系统 `high-availability.storageDir` 配置的路径中，并且在 Kubernetes 中只能有一个目录指向此位置。

```yaml
high-availability.storageDir: s3://flink/recovery
```

  `storageDir` 存储要从 JobManager 失败恢复时所需的所有元数据。

- [kubernetes.cluster-id]({{< ref "docs/deployment/config" >}}#kubernetes-cluster-id) (必要的):
为了识别 Flink 集群，你必须指定 `kubernetes.cluster-id`。

```yaml
kubernetes.cluster-id: cluster1337
```

### 配置示例

在 `conf/flink-conf.yaml` 中配置高可用模式:

```yaml
kubernetes.cluster-id: <cluster-id>
high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
high-availability.storageDir: hdfs:///flink/recovery
```

{{< top >}}

## 高可用数据清理

要在重新启动 Flink 集群时保留高可用数据，只需删除部署（通过 `kubectl delete deployment <cluster-id>`）。所有与 Flink 集群相关的资源将被删除（例如：JobManager Deployment、TaskManager pods、services、Flink conf ConfigMap）。高可用相关的 ConfigMaps 将被保留，因为它们没有设置所有者引用。当重新启动集群时，所有以前运行的作业将从最近成功的检查点恢复并重新启动。

{{< top >}}
