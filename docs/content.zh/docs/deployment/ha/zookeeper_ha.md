---
title: ZooKeeper 高可用服务
weight: 2
type: docs
aliases:
  - /zh/deployment/ha/zookeeper_ha.html
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

# ZooKeeper 高可用服务

Flink 的 ZooKeeper 高可用模式使用 [ZooKeeper](http://zookeeper.apache.org) 提供高可用服务。

Flink 利用 **[ZooKeeper](http://zookeeper.apache.org)** 在所有运行的 JobManager 实例之间进行 *分布式协调*。ZooKeeper 是一个独立于 Flink 的服务，它通过 leader 选举和轻量级的一致性状态存储来提供高可靠的分布式协调。查看 [ZooKeeper入门指南](http://zookeeper.apache.org/doc/current/zookeeperStarted.html)，了解更多关于 ZooKeeper 的信息。Flink 包含 [启动一个简单的ZooKeeper](#bootstrap-zookeeper) 的安装脚本。

## 配置

为了启用高可用集群（HA-cluster），你必须设置以下配置项:

- [high-availability]({{< ref "docs/deployment/config" >}}#high-availability-1) (必要的):
  `high-availability` 配置项必须设置为 `zookeeper`。

  <pre>high-availability: zookeeper</pre>

- [high-availability.storageDir]({{< ref "docs/deployment/config" >}}#high-availability-storagedir) (必要的):
  JobManager 元数据持久化到文件系统 `high-availability.storageDir` 配置的路径中，并且在 ZooKeeper 中只能有一个目录指向此位置。

  <pre>high-availability.storageDir: hdfs:///flink/recovery</pre>

  `storageDir` 存储要从 JobManager 失败恢复时所需的所有元数据。

- [high-availability.zookeeper.quorum]({{< ref "docs/deployment/config" >}}#high-availability-zookeeper-quorum) (必要的):
  *ZooKeeper quorum* 是一个提供分布式协调服务的复制组。

  <pre>high-availability.zookeeper.quorum: address1:2181[,...],addressX:2181</pre>

  每个 `addressX:port` 指的是一个 ZooKeeper 服务器，它可以被 Flink 在给定的地址和端口上访问。

- [high-availability.zookeeper.path.root]({{< ref "docs/deployment/config" >}}#high-availability-zookeeper-path-root) (推荐的):
  *ZooKeeper 根节点*，集群的所有节点都放在该节点下。

  <pre>high-availability.zookeeper.path.root: /flink</pre>

- [high-availability.cluster-id]({{< ref "docs/deployment/config" >}}#high-availability-cluster-id) (推荐的):
  *ZooKeeper cluster-id 节点*，在该节点下放置集群所需的协调数据。

  <pre>high-availability.cluster-id: /default_ns # important: customize per cluster</pre>

  **重要**:
  在 YARN、原生 Kubernetes 或其他集群管理器上运行时，不应该手动设置此值。在这些情况下，将自动生成一个集群 ID。如果在未使用集群管理器的机器上运行多个 Flink 高可用集群，则必须为每个集群手动配置单独的集群 ID（cluster-ids）。

### 配置示例

在 `conf/flink-conf.yaml` 中配置高可用模式和 ZooKeeper 复制组（quorum）:

```bash
high-availability: zookeeper
high-availability.zookeeper.quorum: localhost:2181
high-availability.zookeeper.path.root: /flink
high-availability.cluster-id: /cluster_one # 重要: 每个集群自定义
high-availability.storageDir: hdfs:///flink/recovery
```

{{< top >}}

## ZooKeeper 安全配置

如果 ZooKeeper 使用 Kerberos 以安全模式运行，必要时可以在 `flink-conf.yaml` 中覆盖以下配置:

```bash
# 默认配置为 "zookeeper". 如果 ZooKeeper quorum 配置了不同的服务名称，
# 那么可以替换到这里。

zookeeper.sasl.service-name: zookeeper 

# 默认配置为 "Client". 该值必须为 "security.kerberos.login.contexts" 项中配置的某一个值。
zookeeper.sasl.login-context-name: Client  
```

有关用于 Kerberos 安全性的 Flink 配置的更多信息，请参阅 [Flink 配置页面的安全性部分]({{< ref "docs/deployment/config" >}}#security)。你还可以找到关于 [Flink 如何在内部设置基于 kerberos 的安全性]({{< ref "docs/deployment/security/security-kerberos" >}}) 的详细信息。

{{< top >}}

## Advanced Configuration

### Tolerating Suspended ZooKeeper Connections

Per default, Flink's ZooKeeper client treats suspended ZooKeeper connections as an error.
This means that Flink will invalidate all leaderships of its components and thereby triggering a failover if a connection is suspended.

This behaviour might be too disruptive in some cases (e.g., unstable network environment).
If you are willing to take a more aggressive approach, then you can tolerate suspended ZooKeeper connections and only treat lost connections as an error via [high-availability.zookeeper.client.tolerate-suspended-connections]({{< ref "docs/deployment/config" >}}#high-availability-zookeeper-client-tolerate-suspended-connection).
Enabling this feature will make Flink more resilient against temporary connection problems but also increase the risk of running into ZooKeeper timing problems.

For more information take a look at [Curator's error handling](https://curator.apache.org/errors.html).

## ZooKeeper 版本

Flink 附带了 3.4 和 3.5 的单独的 ZooKeeper 客户端，其中 3.4 位于发行版的 `lib` 目录中，为默认使用版本，而 3.5 位于 opt 目录中。

3.5 客户端允许你通过 SSL 保护 ZooKeeper 连接，但 _可能_ 不适用于 3.4 版本的 ZooKeeper 安装。

你可以通过在 `lib` 目录中放置任意一个 jar 来控制 Flink 使用哪个版本。

{{< top >}}

<a name="bootstrap-zookeeper" />

## 启动 ZooKeeper

如果你没有安装 ZooKeeper，可以使用 Flink 附带的帮助脚本。

在 `conf/zoo.cfg` 文件中有 ZooKeeper 的配置模板。你可以在 `server.X` 配置项中配置主机来运行 ZooKeeper。其中 X 是每个服务器的唯一 ID:

```bash
server.X=addressX:peerPort:leaderPort
[...]
server.Y=addressY:peerPort:leaderPort
```

脚本 `bin/start-zookeeper-quorum.sh` 将在每个配置的主机上启动一个 ZooKeeper 服务。该进程是通过 Flink 包装器来启动的，该包装器从 `conf/zoo.cfg` 读取配置，并确保设置一些必要的配置值以方便使用。

在生产环境中，建议你自行管理 ZooKeeper 的安装与部署。

{{< top >}}
