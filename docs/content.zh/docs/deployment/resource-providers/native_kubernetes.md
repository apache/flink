---
title: 原生 Kubernetes
weight: 3
type: docs
aliases:
  - /zh/deployment/resource-providers/native_kubernetes.html
  - /zh/ops/deployment/native_kubernetes.html
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

# 原生 Kubernetes

本页面描述了如何在 [Kubernetes](https://kubernetes.io/zh/) 上原生地部署 Flink。

## 入门

本*入门*部分指导你在 Kubernetes 上设置一个功能完备的 Flink 集群。

### 简介

Kubernetes 是一个流行的容器编排系统，用于自动化计算机应用的部署、扩缩容与管理。
Flink 的原生 Kubernetes 集成允许你直接在正在运行的 Kubernetes 集群上部署 Flink。
此外，Flink 能够根据所需地资源动态地分配和回收 TaskManager，因为它可以直接与 Kubernetes 通信。

### 准备工作

*入门*部分假设有一个正在运行地 Kubernetes 集群满足以下要求：

- Kubernetes >= 1.9。
- KubeConfig，具有列出、创建、删除 Pod 和 Service 的权限，可通过 `~/.kube/config` 配置。你可以通过运行 `kubectl auth can-i <list|create|edit|delete> pods` 来验证权限。
- 启用 Kubernetes DNS。
- `default` 服务账户具有 [RBAC](#rbac) 权限，可以创建与删除 Pod。

如果你在设置 Kubernetes 集群的时候遇到问题，那么请参照[如何设置 Kubernetes 集群](https://kubernetes.io/zh/docs/setup/)。

### 在 Kubernetes 上启动一个 Flink 会话

一旦你的 Kubernetes 集群正在运行，并且 `kubectl` 被配置为指向它，你就可以通过以下命令在 [Session 模式]({{< ref "docs/deployment/overview" >}}#session-mode)中启动 Flink 集群：

```bash
# (1) Start Kubernetes session
$ ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=my-first-flink-cluster

# (2) Submit example job
$ ./bin/flink run \
    --target kubernetes-session \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    ./examples/streaming/TopSpeedWindowing.jar

# (3) Stop Kubernetes session by deleting cluster deployment
$ kubectl delete deployment/my-first-flink-cluster

```

{{< hint info >}}
当使用 [Minikube](https://minikube.sigs.k8s.io/docs/) 时，你需要调用 `minikube tunnel` 以便[在 Minikube 上暴露 Flink 的 LoadBalancer 服务](https://minikube.sigs.k8s.io/docs/handbook/accessing/#using-minikube-tunnel)。
{{< /hint >}}

恭喜你！你已经成功地通过在 Kubernetes 上部署 Flink 运行了一个 Flink 应用。

{{< top >}}

## 部署模式

对于生产使用，我们建议在 [Application 模式]({{< ref "docs/deployment/overview" >}}#application-mode)下部署 Flink Application，因为该模式为 Application 提供了更好的隔离。

### Application 模式

[Application 模式]({{< ref "docs/deployment/overview" >}}#application-mode)需要将用户代码和 Flink 镜像捆绑在一起，因为它在集群上运行用户代码的 `main()` 方法。
Application 模式确保所有 Flink 组件在应用终止之后被妥当地清理。

Flink 社区提供了一个[基础 Docker 镜像]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#docker-hub-flink-images)可以被用来捆绑用户代码：

```dockerfile
FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY /path/of/my-flink-job.jar $FLINK_HOME/usrlib/my-flink-job.jar
```

以 `custom-image-name` 创建并发布 Docker 镜像后，你可以用以下命令启动一个 Application 集群：

```bash
$ ./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=my-first-application-cluster \
    -Dkubernetes.container.image=custom-image-name \
    local:///opt/flink/usrlib/my-flink-job.jar
```

<span class="label label-info">注意</span> `local` 是在 Application 模式下唯一支持的方案。

`kubernetes.cluster-id` 选项指定了集群的名称，它必须是唯一的。
如果你没有指定这个选项，那么 Flink 将生成一个随机的名称。

`kubernetes.container.image` 选项指定了用于启动 Pod 的镜像。

一旦应用集群部署完毕你就可以与它进行交互：

```bash
# List running job on the cluster
$ ./bin/flink list --target kubernetes-application -Dkubernetes.cluster-id=my-first-application-cluster
# Cancel running job
$ ./bin/flink cancel --target kubernetes-application -Dkubernetes.cluster-id=my-first-application-cluster <jobId>
```

你可以通过向 `bin/flink` 传递 `-Dkey=value` 键值对来覆盖在 `conf/flink-conf.yaml` 中设置的配置。

### Per-Job 集群模式

Kubernetes 上的 Flink 不支持 Per-Job 模式。

### Session 模式

你已经在本页面顶部的[入门](#入门)指南中看到了 Session 集群的部署。

Session 模式可以在两种模式下执行：

* **分离模式**（默认）：`kubernetes-session.sh` 在 Kubernetes 上部署 Flink 集群然后终止。

* **附加模式**（`-Dexecution.attached=true`）：`kubernetes-session.sh` 保持连接并允许输入命令来控制正在运行的 Flink 集群。
  例如，`stop` 停止正在运行的 Session 集群。
  输入 `help` 列出所有支持的命令。

为了重新附加到一个正在运行的集群 ID 为 `my-first-flink-cluster` 的 Session 集群，可以使用以下命令：

```bash
$ ./bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dexecution.attached=true
```

你可以通过向 `bin/kubernetes-session.sh` 传递 `-Dkey=value` 键值对来覆盖在 `conf/flink-conf.yaml` 中设置的配置。

#### 停止正在运行的 Session 集群

为了停止一个正在运行的集群 ID 为 `my-first-flink-cluster` 的 Session 集群，你可以[删除 Flink Deployment](#手动资源清理) 或者使用以下命令：

```bash
$ echo 'stop' | ./bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dexecution.attached=true
```

{{< top >}}

## Kubernetes 上的 Flink 参考资料

### 配置 Kubernetes 上的 Flink

Kubernetes 特定的配置选项在[配置页面]({{< ref "docs/deployment/config" >}}#kubernetes)中被列出。

Flink 使用 [Fabric8 Kubernetes 客户端](https://github.com/fabric8io/kubernetes-client)与 Kubernetes APIServer 进行通信，以创建或删除 Kubernetes 资源（如 Deployment、Pod、ConfigMap 与 Service 等），同时也对 Pod 与 ConfigMap 进行观测。
除了上述 Flink 的配置选项，一些 Fabric8 Kubernetes 客户端的[专业选项](https://github.com/fabric8io/kubernetes-client#configuring-the-client)可以通过系统属性或环境变量进行配置。

例如，用户可以使用以下 Flink 配置选项来设置最大并发请求数量，它允许在使用 Kubernetes 高可用服务时能够在 Session 集群中运行更多的作业。
请注意，每个 Flink 作业将消耗 `3` 个并发请求。

```yaml
containerized.master.env.KUBERNETES_MAX_CONCURRENT_REQUESTS: 200
env.java.opts.jobmanager: "-Dkubernetes.max.concurrent.requests=200"
```

### 访问 Flink 的 Web UI

Flink 的 Web UI 和 REST 端点可以通过 [kubernetes.rest-service.exposed.type]({{< ref "docs/deployment/config" >}}#kubernetes-rest-service-exposed-type) 配置选项以多种方式被暴露。

- **ClusterIP**：在集群内部 IP 上暴露服务。
  Service 只能在集群内部可达。
  如果你想要访问 JobManager UI 或者向现有会话提交任务，你需要启动一个本地代理。
  然后你可以使用 `localhost:8081` 来向会话提交 Flink 任务或者查看仪表板。

```bash
$ kubectl port-forward service/<ServiceName> 8081
```

- **NodePort**：在每个节点的 IP 上以静态端口（`NodePort`）暴露服务。
  `<NodeIP>:<NodePort>` 可以用来联系 JobManager 服务。
  `NodeIP` 也可以用 Kubernetes ApiServer 地址代替。
  你可以在你的 Kube 配置文件中找到它的地址。

- **LoadBalancer**：使用云提供商的负载均衡器向外部暴露服务。
  由于云提供商和 Kubernetes 需要一些时间来准备负载均衡器，你可能会在客户端日志中得到一个 `NodePort` JobManager Web 界面。
  你可以使用 `kubectl get services/<cluster-id>-rest` 来获取 EXTERNAL-IP 并手动构建负载均衡器 JobManager Web 界面 `http://<EXTERNAL-IP>:8081`。

更多信息请参考[在 Kubernetes 中发布服务](https://kubernetes.io/zh/docs/concepts/services-networking/service/#publishing-services-service-types)的官方文档。

{{< hint warning >}}
根据你的环境，使用 `LoadBalancer` REST 服务暴露类型启动一个 Flink 集群可能会使集群被公开访问（通常具有执行任意代码的能力）。
{{< /hint >}}

### 日志

Kubernetes 的集成将 `conf/log4j-console.properties` 与 `conf/logback-console.xml` 作为 ConfigMap 暴露给 Pod。
对这些文件的改变将对新启动的集群可见。

#### 访问日志

默认情况下，JobManager 与 TaskManager 将同时把日志输出到控制台与 `/opt/flink/log`。
`STDOUT` 与 `STDERR` 输出将只被重定向到控制台。
你可以通过以下方式访问它们

```bash
$ kubectl logs <pod-name>
```

如果 Pod 正在运行，你也可以使用 `kubectl exec -it <pod-name> bash` 进行连接并查看日志或调试进程。

#### 访问 TaskManager 的日志

为了不浪费资源，Flink 会自动回收空闲的 TaskManager。
这种行为可能会使访问相关的 Pod 的日志更加困难。
你可以通过配置 [resourcemanager.taskmanager-timeout]({{< ref "docs/deployment/config" >}}#resourcemanager-taskmanager-timeout) 来增加空闲的 TaskManager 被释放之前的时间，这样你就有更多的时间来检查日志文件。

#### 动态改变日志级别

如果你已经将你的日志记录器配置为[自动检测配置变化]({{< ref "docs/deployment/advanced/logging" >}})，那么你可以通过改变相关的 ConfigMap 来动态调整日志级别（假设集群 ID 为 `my-first-flink-cluster`）：

```bash
$ kubectl edit cm flink-config-my-first-flink-cluster
```

### 使用插件

为了使用[插件]({{< ref "docs/deployment/filesystems/plugins" >}})，你必须把它们复制到 Flink JobManager 或 TaskManager Pod 的正确位置。
你可以使用[内置插件]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#using-plugins)，而无需挂载卷或构建自定义 Docker 镜像。
例如，使用以下命令为你的 Flink Session 集群启动 S3 插件。

```bash
$ ./bin/kubernetes-session.sh
    -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{< version >}}}.jar \
    -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{< version >}}}.jar
```

### 自定义 Docker 镜像

如果你想要使用一个自定义的 Docker 镜像，那么你可以通过配置选项 `kubernetes.container.image` 指定它。
Flink 社区提供了丰富的 [Flink Docker 镜像]({{< ref "docs/deployment/resource-providers/standalone/docker" >}})，可以作为一个很好的起点。
参照[如何自定义 Flink Docker 镜像]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#customize-flink-image)了解如何启用插件、添加依赖与其它选项。

### 使用 Secret

[Kubernetes Secrets](https://kubernetes.io/zh/docs/concepts/configuration/secret/) 是一个包含少量敏感数据的对象，如密码、令牌或密钥。
这样的信息可能会被放在 Pod 对象规约或镜像中。
Kubernetes 上的 Flink 可以通过两种方式使用 Secret：

* 在 Pod 中使用 Secret 文件；

* 以环境变量的形式使用 Secret；

#### 在 Pod 中使用 Secret 文件

以下命令将把 Secret `mysecret` 挂载到已启动的 Pod 中的 `/path/to/secret` 路径之下：

```bash
$ ./bin/kubernetes-session.sh -Dkubernetes.secrets=mysecret:/path/to/secret
```

Secret `mysecret` 的用户名与密码可以在文件 `/path/to/secret/username` 与 `/path/to/secret/password` 中找到。
更多细节请参照 [Kubernetes 官方文档](https://kubernetes.io/zh/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod)。

#### 以环境变量的形式使用 Secret

以下命令将把 Secret `mysecret` 作为环境变量暴露在启动的 Pod 中：

```bash
$ ./bin/kubernetes-session.sh -Dkubernetes.env.secretKeyRef=\
    env:SECRET_USERNAME,secret:mysecret,key:username;\
    env:SECRET_PASSWORD,secret:mysecret,key:password
```

环境变量 `SECRET_USERNAME` 包含用户名，环境变量 `SECRET_PASSWORD` 包含 Secret `mysecret` 的密码。
更多细节请参照 [Kubernetes 官方文档](https://kubernetes.io/zh/docs/concepts/configuration/secret/#using-secrets-as-environment-variables)。

### Kubernetes 上的高可用性

对于 Kubernetes 上的高可用性，你可以使用[现有的高可用性服务]({{< ref "docs/deployment/ha/overview" >}})。

### 手动资源清理

Flink 使用 [Kubernetes OwnerReference](https://kubernetes.io/zh/docs/concepts/workloads/controllers/garbage-collection/) 来清理所有集群组件。
所有 Flink 创建的资源，包括 `ConfigMap`、`Service` 与 `Pod`，它们的 `OwnerReference` 都被设置为 `deployment/<cluster-id>`。
当 Deployment 被删除时，所有相关的资源都会被自动删除。

```bash
$ kubectl delete deployment/<cluster-id>
```

### 支持的 Kubernetes 版本

目前，所有版本 `>= 1.9` 的 Kubernetes 都被支持。

### 名字空间

[Kubernetes 名字空间](https://kubernetes.io/zh/docs/concepts/overview/working-with-objects/namespaces/)通过[资源配额](https://kubernetes.io/docs/concepts/policy/resource-quotas/)在多个用户之间划分集群资源。
在 Kubernetes 上的 Flink 可以使用名字空间来启动 Flink 集群。
名字空间可以通过 [kubernetes.namespace]({{< ref "docs/deployment/config" >}}#kubernetes-namespace) 进行配置。

### RBAC

基于角色的访问控制（[RBAC](https://kubernetes.io/zh/docs/reference/access-authn-authz/rbac/)）是一种根据企业内部个人用户的角色来调节计算或网络资源的方法。
用户可以配置 RBAC 角色和 JobManager 使用的服务账户在 Kubernetes 集群中访问 Kubernetes API Server。

每个名字空间都有一个默认的服务账户。然而，`default` 服务账户可能没有权限在 Kubernetes 集群中创建或删除 Pod。
用户可能需要更新 `default` 服务账户或者指定另外一个具有正确的角色绑定的服务账户。

```bash
$ kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
```

如果你不想使用 `default` 服务账户，使用以下命令创建一个新的 `flink-service-account` 服务账户并设置角色绑定。
然后使用配置选项 `-Dkubernetes.service-account=flink-service-account` 使得 JobManager Pod 使用 `flink-service-account` 服务账户来创建或删除 TaskManager Pod 和 Leader ConfigMap。
这也将允许 TaskManager 观测 Leader ConfigMap 以检索 JobManager 与 ResourceManager 的地址。

```bash
$ kubectl create serviceaccount flink-service-account
$ kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=default:flink-service-account
```

请参照 Kubernetes 官方文档的 [RBAC 授权](https://kubernetes.io/zh/docs/reference/access-authn-authz/rbac/)来获取更多信息。

### Pod 模板

Flink 允许用户通过模板文件来定义 JobManager 与 TaskManager Pod。这允许支持 Flink [Kubernetes 配置选项]({{< ref "docs/deployment/config" >}}#kubernetes)不直接支持的高级功能。
使用 [`kubernetes.pod-template-file`]({{< ref "docs/deployment/config" >}}#kubernetes-pod-template-file) 来制定一个包含 Pod 定义的本地文件。它将被用来初始化 JobManager 与 TaskManager。
请参照 [Pod 模板的例子](#pod-模板的例子)来获取更多信息。

#### 被 Flink 覆盖的字段

Pod 模板的一些字段将会被 Flink 覆盖。
解决有效的字段值的机制可以被分为以下几类：
* **由 Flink 定义：** 用户不能配置它。
* **由用户定义：** 用户可以自由指定该值。Flink 框架不会设置任何额外的值，有效值来源于配置选项与模板。

  优先顺序：首先会使用显式的配置选项的值，然后是 Pod 模板中的值，最后如果没有指定的话是配置选项的默认值。

* **与 Flink 合并：** Flink 会将设定的值与用户定义的值（参照“由用户定义”的优先顺序）进行合并。在同名字段的情况下 Flink 的值具有优先权。

关于会被覆盖的 Pod 的字段的完整列表请参照以下表格。
在 Pod 模板中被定义的字段，如果没有在表格中被列出的话将不会受到影响。

**Pod Metadata**
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 10%">键</th>
            <th class="text-left" style="width: 20%">分类</th>
            <th class="text-left" style="width: 30%">相关配置选项</th>
            <th class="text-left" style="width: 40%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>由 Flink 定义</td>
            <td></td>
            <td>JobManager Pod 名称会被 <a href="{{< ref "docs/deployment/config" >}}#kubernetes-cluster-id">kubernetes.cluster-id</a> 定义的 Deployment 覆盖。
                TaskManager Pod 名称会以 <code>&lt;clusterID&gt;-&lt;attempt&gt;-&lt;index&gt;</code> 模式被覆盖，由 Flink ResourceManager 生成</td>
        </tr>
        <tr>
            <td>namespace</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-namespace">kubernetes.namespace</a></td>
            <td>JobManager Deployment 与 TaskManager Pod 都将在用户指定的名字空间中创建。</td>
        </tr>
        <tr>
            <td>ownerReferences</td>
            <td>由 Flink 定义</td>
            <td></td>
            <td>JobManger 与 TaskManager 的 ownerReference 将始终被设置为 JobManager Deployment。
                请使用 <a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-owner-reference">kubernetes.jobmanager.owner.reference</a> 来控制何时删除 Deployment。</td>
        </tr>
        <tr>
            <td>annotations</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-annotations">kubernetes.jobmanager.annotations</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-annotations">kubernetes.taskmanager.annotations</a></td>
            <td>Flink 将添加由 Flink 配置选项指定的额外注解。</td>
        </tr>
        <tr>
            <td>labels</td>
            <td>由 Flink 定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-labels">kubernetes.jobmanager.labels</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-labels">kubernetes.taskmanager.labels</a></td>
            <td>Flink 将为用户定义的值添加一些内部标签。</td>
        </tr>
    </tbody>
</table>

**Pod Spec**
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 10%">键</th>
            <th class="text-left" style="width: 20%">分类</th>
            <th class="text-left" style="width: 30%">相关配置选项</th>
            <th class="text-left" style="width: 40%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>imagePullSecrets</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-container-image-pull-secrets">kubernetes.container.image.pull-secrets</a></td>
            <td>Flink 将添加由 Flink 配置选项指定的额外拉取 Secret。</td>
        </tr>
        <tr>
            <td>nodeSelector</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-node-selector">kubernetes.jobmanager.node-selector</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-node-selector">kubernetes.taskmanager.node-selector</a></td>
            <td>Flink 将添加由 Flink 配置选项指定的额外节点选择器。</td>
        </tr>
        <tr>
            <td>tolerations</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-tolerations">kubernetes.jobmanager.tolerations</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-tolerations">kubernetes.taskmanager.tolerations</a></td>
            <td>Flink 将添加由 Flink 配置选项指定的额外容忍度。</td>
        </tr>
        <tr>
            <td>restartPolicy</td>
            <td>由 Flink 定义</td>
            <td></td>
            <td>对于 JobManager Pod 是“always”，对于 TaskManager Pod 是“never”。
                <br>
                JobManager Pod 将总是被 Deployment 重新启动。而 TaskManager 不应该被重新启动。</td>
        </tr>
        <tr>
            <td>serviceAccount</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-service-account">kubernetes.service-account</a></td>
            <td>JobManager 与 TaskManager Pod 将以用户定义的服务账户创建。</td>
        </tr>
        <tr>
            <td>volumes</td>
            <td>与 Flink 合并</td>
            <td></td>
            <td>Flink 将添加一些内部的 ConfigMap 卷（如 flink-config-volume 与 hadoop-config-volume），这对于运送 Flink 配置与 Hadoop 配置是必要的。</td>
        </tr>
    </tbody>
</table>

**Main Container Spec**
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 10%">键</th>
            <th class="text-left" style="width: 20%">分类</th>
            <th class="text-left" style="width: 30%">相关配置选项</th>
            <th class="text-left" style="width: 40%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>env</td>
            <td>与 Flink 合并</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#forwarding-environment-variables">containerized.master.env.{ENV_NAME}</a>
                <a href="{{< ref "docs/deployment/config" >}}#forwarding-environment-variables">containerized.taskmanager.env.{ENV_NAME}</a></td>
            <td>Flink 会将一些内部环境变量添加到用户定义的值中。</td>
        </tr>
        <tr>
            <td>image</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-container-image">kubernetes.container.image</a></td>
            <td>容器镜像将会以用户定义的值根据定义的优先顺序被解析。</td>
        </tr>
        <tr>
            <td>imagePullPolicy</td>
            <td>由用户定义</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-container-image-pull-policy">kubernetes.container.image.pull-policy</a></td>
            <td>容器镜像拉取策略将会以用户定义的值根据定义的优先顺序被解析。</td>
        </tr>
        <tr>
            <td>name</td>
            <td>由 Flink 定义</td>
            <td></td>
            <td>容器名称将被 Flink 以“flink-main-container”覆盖。</td>
        </tr>
        <tr>
            <td>resources</td>
            <td>由用户定义</td>
            <td>内存：<br>
                    <a href="{{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size">jobmanager.memory.process.size</a>
                    <a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size">taskmanager.memory.process.size</a>
                <br>
                CPU：<br>
                    <a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-cpu">kubernetes.jobmanager.cpu</a>
                    <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-cpu">kubernetes.taskmanager.cpu</a></td>
            <td>内存和 CPU 资源（包括请求与限制）将被 Flink 配置选项覆盖。所有其它的资源（如 ephemeral-storage）将被保留。</td>
        </tr>
        <tr>
            <td>containerPorts</td>
            <td>与 Flink 合并</td>
            <td></td>
            <td>Flink 将添加一些内部容器端口（如 rest、jobmanager-rpc、blob 与 taskmanager-rpc）</td>
        </tr>
        <tr>
            <td>volumeMounts</td>
            <td>与 Flink 合并</td>
            <td></td>
            <td>Flink 将添加一些卷挂载（如 flink-config-volume 与 hadoop-config-volume），这对于运送 Flink 配置与 Hadoop 配置是必要的。</td>
        </tr>
    </tbody>
</table>

#### Pod 模板的例子

`pod-template.yaml`
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: jobmanager-pod-template
spec:
  initContainers:
    - name: artifacts-fetcher
      image: artifacts-fetcher:latest
      # Use wget or other tools to get user jars from remote storage
      command: [ 'wget', 'https://path/of/StateMachineExample.jar', '-O', '/flink-artifact/myjob.jar' ]
      volumeMounts:
        - mountPath: /flink-artifact
          name: flink-artifact
  containers:
    # Do not change the main container name
    - name: flink-main-container
      resources:
        requests:
          ephemeral-storage: 2048Mi
        limits:
          ephemeral-storage: 2048Mi
      volumeMounts:
        - mountPath: /opt/flink/volumes/hostpath
          name: flink-volume-hostpath
        - mountPath: /opt/flink/artifacts
          name: flink-artifact
        - mountPath: /opt/flink/log
          name: flink-logs
      # Use sidecar container to push logs to remote storage or do some other debugging things
    - name: sidecar-log-collector
      image: sidecar-log-collector:latest
      command: [ 'command-to-upload', '/remote/path/of/flink-logs/' ]
      volumeMounts:
        - mountPath: /flink-logs
          name: flink-logs
  volumes:
    - name: flink-volume-hostpath
      hostPath:
        path: /tmp
        type: Directory
    - name: flink-artifact
      emptyDir: { }
    - name: flink-logs
      emptyDir: { }
```

{{< top >}}
