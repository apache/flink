---
title: Kerberos
weight: 3
type: docs
aliases:
  - /zh/deployment/security/security-kerberos.html
  - /zh/ops/security-kerberos.html
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

<a name="kerberos-authentication-setup-and-configuration"></a>

# Kerberos 身份认证设置和配置

本文简要描述了 Flink 如何在各种部署机制（Standalone, native Kubernetes, YARN）、文件系统、connector 以及 state backend 的上下文中安全工作。

<a name="objective"></a>

## 目标
Flink Kerberos 安全框架的主要目标如下：

1. 在集群内使用 connector（例如 Kafka）时确保作业安全地访问数据；
2. 对 zookeeper 进行身份认证（如果配置了 SASL）；
3. 对 Hadoop 组件进行身份认证（例如 HDFS，HBASE）。

生产部署场景中，流式作业通常会运行很长一段时间（天、周、月级别的时间段），并且需要在作业的整个生命周期中对其进行身份认证以保护数据源。与 Hadoop delegation token 和 ticket 缓存项不同，Kerberos keytab 不会在该时间段内过期。

当前的实现支持使用可配置的 keytab credential 或 Hadoop delegation token 来运行 Flink 集群（JobManager / TaskManager / 作业）。

请注意，所有作业都能共享为指定集群配置的凭据。如果想为一个作业使用不同的 keytab，只需单独启动一个具有不同配置的 Flink 集群。多个 Flink 集群可以在 Kubernetes 或 YARN 环境中并行运行。

<a name="how-flink-security-works"></a>

## Flink Security 如何工作

理论上，Flink 程序可以使用自己的或第三方的 connector（Kafka、HDFS、Cassandra、Flume、Kinesis 等），同时需要支持任意的认证方式（Kerberos、SSL/TLS、用户名/密码等）。满足所有 connector 的安全需求还在进行中，不过 Flink 提供了针对 Kerberos 身份认证的一流支持。Kerberos 身份认证支持以下服务和 connector：

- Kafka (0.9+)
- HDFS
- HBase
- ZooKeeper

请注意，你可以单独为每个服务或 connector 启用 Kerberos。例如，用户可以启用 Hadoop security，而无需为 ZooKeeper 开启 Kerberos，反之亦然。Kerberos 凭证是组件之间共享的配置，每个组件会显式地使用它。

Flink 安全内部架构是建立在安全模块（实现 `org.apache.flink.runtime.security.modules.SecurityModule`）上的，安全模块在 Flink 启动过程中被安装。后面部分描述了每个安全模块。

<a name="hadoop-security-module"></a>

### Hadoop Security 模块
该模块使用 Hadoop UserGroupInformation（UGI）类来建立进程范围的 *登录用户* 上下文。然后，登录用户用于与 Hadoop 组件的所有交互，包括 HDFS、HBase 和 YARN。

如果启用了 Hadoop security（在 `core-site.xml` 中），登录用户将拥有所有配置的 Kerberos 凭据。否则，登录用户仅继承启动集群的操作系统帐户的用户身份。

<a name="jaas-security-module"></a>

### JAAS Security 模块
该模块为集群提供动态 JAAS 配置，使已配置的 Kerberos 凭证对 ZooKeeper、Kafka 和其他依赖 JAAS 的组件可用。

请注意，用户还可以使用 [Java SE 文档](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html)中描述的机制提供静态 JAAS 配置文件。静态配置项会覆盖此模块提供的任何动态配置项。

<a name="zookeeper-security-module"></a>

### ZooKeeper Security 模块
该模块配置某些进程范围内 ZooKeeper 安全相关的设置，即 ZooKeeper 服务名称（默认为：`zookeeper`）和 JAAS 登录上下文名称（默认为：`Client`）。

<a name=deployment-models></a>

## 部署模式
以下是针对每种部署模式的一些信息。

<a name="standalone-mode"></a>

### Standalone 模式

在 standalone 模式或集群模式下运行安全 Flink 集群的步骤如下：

1. 将与安全相关的配置选项添加到 Flink 配置文件（在所有集群节点上执行）（详见[此处]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)）。
2. 确保 keytab 文件存在于每个群集节点通过 `security.kerberos.login.keytab` 指定的路径上。
3. 正常部署 Flink 集群。

<a name="native-kubernetes-and-yarn-mode"></a>

### 原生 Kubernetes 和 YARN 模式

在原生 Kubernetes 或 YARN 模式下运行安全 Flink 集群的步骤如下：

1. 在客户端的 Flink 配置文件中添加安全相关的配置选项（详见[此处]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)）。
2. 确保 keytab 文件存在于客户端通过 `security.kerberos.login.keytab` 指定的路径上。
3. 正常部署 Flink 集群。

在 YARN 和 原生 Kubernetes 模式下，keytab 文件会被自动从客户端拷贝到 Flink 容器中。

要启用 Kerberos 身份认证，还需要 Kerberos 配置文件。该文件可以从集群环境中获取，也可以由 Flink 上传。针对后者，你需要配置 `security.kerberos.krb5-conf.path` 来指定 Kerberos 配置文件的路径，Flink 会将此文件复制到相应容器或 pod。

请参阅 <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md">YARN security</a> 文档获取更多相关信息。

<a name="using-kinit-yarn-only"></a>

#### 使用 `kinit` (仅限 YARN)

在 YARN 模式下，可以不需要 keytab 而只使用 ticket 缓存（由 `kinit` 管理）来部署一个安全的 Flink 集群。这避免了生成 keytab 的复杂性，同时避免了将其委托给集群管理器。在这种情况下，使用 Flink CLI 获取 Hadoop delegation token（用于 HDFS 和 HBase）。主要缺点是集群必须是短暂的，因为生成的 delegation token 将会过期（通常在一周内）。

使用 `kinit` 运行安全 Flink 集群的步骤如下：

1. 在客户端的 Flink 配置文件中添加安全相关的配置选项（详见[此处]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)）。
2. 使用 `kinit` 命令登录。
3. 正常部署 Flink 集群。

<a name="further-details"></a>

## 更多细节

<a name="ticket-renewal"></a>

### Ticket 更新
使用 Kerberos 的每个组件都独立负责更新 Kerberos ticket-granting-ticket（TGT）。Hadoop、ZooKeeper 和 Kafka 都在提供 keytab 时自动更新 TGT。在 delegation token 场景中，YARN 本身会更新 token（更新至其最大生命周期）。

{{< top >}}
