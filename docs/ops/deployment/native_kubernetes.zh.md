---
title:  "原生 Kubernetes 设置"
nav-title: Native Kubernetes
nav-parent_id: deployment
is_beta: true
nav-pos: 7
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

本页面描述了如何在 [Kubernetes](https://kubernetes.io) 原生地部署 Flink session 集群。

* This will be replaced by the TOC
{:toc}

<div class="alert alert-warning">
Flink 的原生 Kubernetes 集成仍处于试验阶段。在以后的版本中，配置和 CLI flags 可能会发生变化。
</div>

## 要求

- Kubernetes 版本 1.9 或以上。
- KubeConfig 可以查看、创建、删除 pods 和 services，可以通过`~/.kube/config` 配置。你可以通过运行 `kubectl auth can-i <list|create|edit|delete> pods` 来验证权限。
- 启用 Kubernetes DNS。
- 具有 [RBAC](#rbac) 权限的 Service Account 可以创建、删除 pods。

## Flink Kubernetes Session

### 启动 Flink Session

按照以下说明在 Kubernetes 集群中启动 Flink Session。

Session 集群将启动所有必需的 Flink 服务（JobManager 和 TaskManagers），以便你可以将程序提交到集群。
注意你可以在每个 session 上运行多个程序。

{% highlight bash %}
$ ./bin/kubernetes-session.sh
{% endhighlight %}

所有 Kubernetes 配置项都可以在我们的[配置指南]({{ site.baseurl }}/zh/ops/config.html#kubernetes)中找到。

**示例**: 执行以下命令启动 session 集群，每个 TaskManager 分配 4 GB 内存、2 CPUs、4 slots：

在此示例中，我们覆盖了 `resourcemanager.taskmanager-timeout` 配置，为了使运行 taskmanager 的 pod 停留时间比默认的 30 秒更长。
尽管此设置可能在云环境下增加成本，但在某些情况下能够更快地启动新作业，并且在开发过程中，你有更多的时间检查作业的日志文件。

{% highlight bash %}
$ ./bin/kubernetes-session.sh \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dresourcemanager.taskmanager-timeout=3600000
{% endhighlight %}

系统将使用 `conf/flink-conf.yaml` 中的配置。
如果你更改某些配置，请遵循我们的[配置指南]({{ site.baseurl }}/zh/ops/config.html)。

如果你未通过 `kubernetes.cluster-id` 为 session 指定特定名称，Flink 客户端将会生成一个 UUID 名称。

### 自定义 Flink Docker 镜像

如果要使用自定义的 Docker 镜像部署 Flink 容器，请查看 [Flink Docker 镜像文档](docker.html)、[镜像 tags](docker.html#image-tags)、[如何自定义 Flink Docker 镜像](docker.html#customize-flink-image)和[启用插件](docker.html#using-plugins)。
如果创建了自定义的 Docker 镜像，则可以通过设置 [`kubernetes.container.image`](../config.html#kubernetes-container-image) 配置项来指定它：

{% highlight bash %}
$ ./bin/kubernetes-session.sh \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dresourcemanager.taskmanager-timeout=3600000 \
  -Dkubernetes.container.image=<CustomImageName>
{% endhighlight %}

### 将作业提交到现有 Session

使用以下命令将 Flink 作业提交到 Kubernetes 集群。

{% highlight bash %}
$ ./bin/flink run -d -e kubernetes-session -Dkubernetes.cluster-id=<ClusterId> examples/streaming/WindowJoin.jar
{% endhighlight %}

### 访问 Job Manager UI

有几种方法可以将服务暴露到外部（集群外部） IP 地址。
可以使用 `kubernetes.service.exposed.type` 进行配置。

- `ClusterIP`：通过集群内部 IP 暴露服务。
该服务只能在集群中访问。如果想访问 JobManager ui 或将作业提交到现有 session，则需要启动一个本地代理。
然后你可以使用 `localhost:8081` 将 Flink 作业提交到 session 或查看仪表盘。

{% highlight bash %}
$ kubectl port-forward service/<ServiceName> 8081
{% endhighlight %}

- `NodePort`：通过每个 Node 上的 IP 和静态端口（`NodePort`）暴露服务。`<NodeIP>:<NodePort>` 可以用来连接 JobManager 服务。`NodeIP` 可以很容易地用 Kubernetes ApiServer 地址替换。
你可以在 kube 配置文件找到它。

- `LoadBalancer`：默认值，使用云提供商的负载均衡器在外部暴露服务。
由于云提供商和 Kubernetes 需要一些时间来准备负载均衡器，因为你可能在客户端日志中获得一个 `NodePort` 的 JobManager Web 界面。
你可以使用 `kubectl get services/<ClusterId>` 获取 EXTERNAL-IP 然后手动构建负载均衡器 JobManager Web 界面 `http://<EXTERNAL-IP>:8081`。

- `ExternalName`：将服务映射到 DNS 名称，当前版本不支持。

有关更多信息，请参考官方文档[在 Kubernetes 上发布服务](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types)。

### 连接现有 Session

默认情况下，Kubernetes session 以后台模式启动，这意味着 Flink 客户端在将所有资源提交到 Kubernetes 集群后会退出。使用以下命令来连接现有 session。

{% highlight bash %}
$ ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=<ClusterId> -Dexecution.attached=true
{% endhighlight %}

### 停止 Flink Session

要停止 Flink Kubernetes session，将 Flink 客户端连接到集群并键入 `stop`。

{% highlight bash %}
$ echo 'stop' | ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=<ClusterId> -Dexecution.attached=true
{% endhighlight %}

#### 手动清理资源

Flink 用 [Kubernetes OwnerReference's](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) 来清理所有集群组件。
所有 Flink 创建的资源，包括 `ConfigMap`、`Service`、`Pod`，已经将 OwnerReference 设置为 `deployment/<ClusterId>`。
删除 deployment 后，所有其他资源将自动删除。

{% highlight bash %}
$ kubectl delete deployment/<ClusterID>
{% endhighlight %}

## 日志文件

默认情况下，JobManager 和 TaskManager 只把日志存储在每个 pod 中的 `/opt/flink/log` 下。
如果要使用 `kubectl logs <PodName>` 查看日志，必须执行以下操作：

1. 在 Flink 客户端的 log4j.properties 中增加新的 appender。
2. 在 log4j.properties 的 rootLogger 中增加如下 'appenderRef'，`rootLogger.appenderRef.console.ref = ConsoleAppender`。
3. 通过增加配置项 `-Dkubernetes.container-start-command-template="%java% %classpath% %jvmmem% %jvmopts% %logging% %class% %args%"` 来删除重定向的参数。
4. 停止并重启你的 session。现在你可以使用 `kubectl logs` 查看日志了。

{% highlight bash %}
# Log all infos to the console
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
{% endhighlight %}

如果 pod 正在运行，可以使用 `kubectl exec -it <PodName> bash` 进入 pod 并查看日志或调试进程。

## Flink Kubernetes Application

### 启动 Flink Application

Application 模式允许用户创建单个镜像，其中包含他们的作业和 Flink 运行时，该镜像将按需自动创建和销毁集群组件。Flink 社区提供了可以构建[多用途自定义镜像](docker.html#customize-flink-image)的基础镜像。

{% highlight dockerfile %}
FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY /path/of/my-flink-job-*.jar $FLINK_HOME/usrlib/my-flink-job.jar
{% endhighlight %}

使用以下命令启动 Flink Application。
{% highlight bash %}
$ ./bin/flink run-application -p 8 -t kubernetes-application \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dkubernetes.container.image=<CustomImageName> \
  local:///opt/flink/usrlib/my-flink-job.jar
{% endhighlight %}

注意：Application 模式只支持 "local" 作为 schema。默认 jar 位于镜像中，而不是 Flink 客户端中。

注意：镜像的 "$FLINK_HOME/usrlib" 目录下的所有 jar 将会被加到用户 classpath 中。

### 停止 Flink Application

当 Application 停止时，所有 Flink 集群资源都会自动销毁。
与往常一样，作业可能会在手动取消或执行完的情况下停止。

{% highlight bash %}
$ ./bin/flink cancel -t kubernetes-application -Dkubernetes.cluster-id=<ClusterID> <JobID>
{% endhighlight %}

## Kubernetes 概念

### 命名空间

[Kubernetes 中的命名空间](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/)是一种在多个用户之间划分集群资源的方法（通过资源配额）。
它类似于 Yarn 集群中的队列概念。Flink on Kubernetes 可以使用命名空间来启动 Flink 集群。
启动 Flink 集群时，可以使用 `-Dkubernetes.namespace=default` 参数来指定命名空间。

[资源配额](https://kubernetes.io/docs/concepts/policy/resource-quotas/)提供了限制每个命名空间的合计资源消耗的约束。
它可以按类型限制可在命名空间中创建的对象数量，以及该项目中的资源可能消耗的计算资源总量。

<a name="rbac"></a>
### 基于角色的访问控制

基于角色的访问控制（[RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)）是一种在企业内部基于单个用户的角色来调节对计算或网络资源的访问的方法。
用户可以配置 RBAC 角色和服务账户，Flink JobManager 使用这些角色和服务帐户访问 Kubernetes 集群中的 Kubernetes API server。

每个命名空间有默认的服务账户，但是`默认`服务账户可能没有权限在 Kubernetes 集群中创建或删除 pod。
用户可能需要更新`默认`服务账户的权限或指定另一个绑定了正确角色的服务账户。

{% highlight bash %}
$ kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
{% endhighlight %}

如果你不想使用`默认`服务账户，使用以下命令创建一个新的 `flink` 服务账户并设置角色绑定。
然后使用配置项 `-Dkubernetes.jobmanager.service-account=flink` 来使 JobManager pod 使用 `flink` 服务账户去创建和删除 TaskManager pod。

{% highlight bash %}
$ kubectl create serviceaccount flink
$ kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=default:flink
{% endhighlight %}

有关更多信息，请参考 Kubernetes 官方文档 [RBAC 授权](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)。

## 背景/内部构造

本节简要解释了 Flink 和 Kubernetes 如何交互。

<img src="{{ site.baseurl }}/fig/FlinkOnK8s.svg" class="img-responsive">

创建 Flink Kubernetes session 集群时，Flink 客户端首先将连接到 Kubernetes ApiServer 提交集群描述信息，包括 ConfigMap 描述信息、Job Manager Service 描述信息、Job Manager Deployment 描述信息和 Owner Reference。
Kubernetes 将创建 Flink master 的 deployment，在此期间 Kubelet 将拉取镜像，准备并挂载卷，然后执行 start 命令。
master pod 启动后，Dispatcher 和 KubernetesResourceManager 服务会相继启动，然后集群准备完成，并等待提交作业。

当用户通过 Flink 客户端提交作业时，将通过客户端生成 jobGraph 并将其与用户 jar 一起上传到 Dispatcher。
然后 Dispatcher 会为每个 job 启动一个单独的 JobMaster。

JobMaster 向 KubernetesResourceManager 请求被称为 slots 的资源。
如果没有可用的 slots，KubernetesResourceManager 将拉起 TaskManager pod 并且把它们注册到集群中。

{% top %}
