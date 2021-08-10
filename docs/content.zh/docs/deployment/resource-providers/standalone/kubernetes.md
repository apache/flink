---
title: Kubernetes 设置
weight: 5
type: docs
aliases:
  - /zh/deployment/resource-providers/standalone/kubernetes.html
  - /zh/ops/deployment/kubernetes.html
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

# Kubernetes 安装

<a name="getting-started"> </a>

## 入门

本 *入门* 指南描述了如何在 Kubernetes 上部署 *Flink Seesion 集群*。

### 介绍

本页面描述了如何使用 Flink [standalone]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) 部署模式在 Kubernetes 上部署 standalone 模式的 Flink 集群。通常我们建议新用户使用 [native Kubernetes 部署]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}) 模式在 Kubernetes上部署 Flink。

### 准备

本指南假设存在一个 Kubernets 的运行环境。可以通过运行 `kubectl get nodes` 命令来确保 Kubernetes 环境运行正常，该命令展示所有连接到 Kubernets 集群的 node 节点信息。

如果想在本地运行 Kubernetes，建议使用 [MiniKube](https://minikube.sigs.k8s.io/docs/start/)。

{{< hint info >}}
如果使用 MiniKube，请确保在部署 Flink 集群之前先执行 `minikube ssh 'sudo ip link set docker0 promisc on'`，否则 Flink 组件不能自动地将自己映射到 Kubernetes Service 中。
{{< /hint >}}

### Kubernetes 上的 Flink session 集群

*Flink session 集群* 是以一种长期运行的 Kubernetes Deployment 形式执行的。可以在一个 *session 集群* 上运行多个 Flink 作业。当然，只有 session 集群部署好以后才可以在上面提交 Flink 作业。

在 Kubernetes 上部署一个基本的 *Flink session 集群* 时，一般包括下面三个组件：

* 运行 [JobManager]({{< ref "docs/concepts/glossary" >}}#flink-jobmanager) 的 *Deployment*；
* 运行 [TaskManagers]({{< ref "docs/concepts/glossary" >}}#flink-taskmanager) 的 *Deployment*；
* 暴露了 *JobManager* 上 REST 和 UI 端口的 *Service*；

使用 [通用集群资源定义](#common-cluster-resource-definitions)中提供的文件内容来创建以下文件，并使用 `kubectl` 命令来创建相应的组件：

```sh
    # 配置和 service 的定义
    $ kubectl create -f flink-configuration-configmap.yaml
    $ kubectl create -f jobmanager-service.yaml
    # 为集群创建 deployments
    $ kubectl create -f jobmanager-session-deployment.yaml
    $ kubectl create -f taskmanager-session-deployment.yaml
```

接下来，我们设置端口转发以访问 Flink UI 页面并提交作业：

1. 运行 `kubectl port-forward ${flink-jobmanager-pod} 8081:8081` 将 jobmanager 的 web ui 端口映射到本地 8081。
2. 在浏览器中导航到 [http://localhost:8081](http://localhost:8081) 页面。
3. 此后，也可以使用以下命令向集群提交作业：
```bash
$ ./bin/flink run -m localhost:8081 ./examples/streaming/TopSpeedWindowing.jar
```

可以使用以下命令停止运行 flink 集群：

```sh
    $ kubectl delete -f jobmanager-service.yaml
    $ kubectl delete -f flink-configuration-configmap.yaml
    $ kubectl delete -f taskmanager-session-deployment.yaml
    $ kubectl delete -f jobmanager-session-deployment.yaml
```

{{< top >}}

<a name="deployment-modes"> </a>

## 部署模式

### Application 集群模式

*Flink Application 集群* 是运行单个应用的专用集群，需要保证部署应用时该集群可用。

在 Kubernetes 上部署一个基本的 *Flink Application 集群* 时，一般包括下面三个组件：

*  *Application* 作业，同时在该 *Application* 中运行 *JobManager*；
* 运行若干个 TaskManager 的 Deployment；
* 暴露了 JobManager 上 REST 和 UI 端口的 Service；

Check [the Application cluster specific resource definitions](#application-cluster-resource-definitions) and adjust them accordingly:

The `args` attribute in the `jobmanager-job.yaml` has to specify the main class of the user job.
See also [how to specify the JobManager arguments]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#jobmanager-additional-command-line-arguments) to understand
how to pass other `args` to the Flink image in the `jobmanager-job.yaml`.

The *job artifacts* should be available from the `job-artifacts-volume` in [the resource definition examples](#application-cluster-resource-definitions).
The definition examples mount the volume as a local directory of the host assuming that you create the components in a minikube cluster.
If you do not use a minikube cluster, you can use any other type of volume, available in your Kubernetes cluster, to supply the *job artifacts*.
Alternatively, you can build [a custom image]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#advanced-customization) which already contains the artifacts instead.

After creating [the common cluster components](#common-cluster-resource-definitions), use [the Application cluster specific resource definitions](#application-cluster-resource-definitions) to launch the cluster with the `kubectl` command:

```sh
    $ kubectl create -f jobmanager-job.yaml
    $ kubectl create -f taskmanager-job-deployment.yaml
```

To terminate the single application cluster, these components can be deleted along with [the common ones](#common-cluster-resource-definitions)
with the `kubectl` command:

```sh
    $ kubectl delete -f taskmanager-job-deployment.yaml
    $ kubectl delete -f jobmanager-job.yaml
```

### Per-Job 集群模式

在 Kubernetes 上部署 Standalone 集群时不支持 Per-Job 集群模式。

### Session 集群模式

本页面顶部的[入门](#getting-started)指南中描述了 Session 集群模式的部署。

{{< top >}}

<a name="flink-on-standalone-kubernetes-reference"> </a>

## Kubernetes 上运行 Standalone 集群指南

<a name="configuration"> </a>

### Configuration

All configuration options are listed on the [configuration page]({{< ref "docs/deployment/config" >}}). Configuration options can be added to the `flink-conf.yaml` section of the `flink-configuration-configmap.yaml` config map.

### Accessing Flink in Kubernetes

You can then access the Flink UI and submit jobs via different ways:
*  `kubectl proxy`:

    1. Run `kubectl proxy` in a terminal.
    2. Navigate to [http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:webui/proxy](http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:webui/proxy) in your browser.

*  `kubectl port-forward`:
    1. Run `kubectl port-forward ${flink-jobmanager-pod} 8081:8081` to forward your jobmanager's web ui port to local 8081.
    2. Navigate to [http://localhost:8081](http://localhost:8081) in your browser.
    3. Moreover, you can use the following command below to submit jobs to the cluster:
    ```bash
    $ ./bin/flink run -m localhost:8081 ./examples/streaming/TopSpeedWindowing.jar
    ```

*  Create a `NodePort` service on the rest service of jobmanager:
    1. Run `kubectl create -f jobmanager-rest-service.yaml` to create the `NodePort` service on jobmanager. The example of `jobmanager-rest-service.yaml` can be found in [appendix](#common-cluster-resource-definitions).
    2. Run `kubectl get svc flink-jobmanager-rest` to know the `node-port` of this service and navigate to [http://&lt;public-node-ip&gt;:&lt;node-port&gt;](http://<public-node-ip>:<node-port>) in your browser.
    3. If you use minikube, you can get its public ip by running `minikube ip`.
    4. Similarly to the `port-forward` solution, you can also use the following command below to submit jobs to the cluster:

    ```bash
    $ ./bin/flink run -m <public-node-ip>:<node-port> ./examples/streaming/TopSpeedWindowing.jar
    ```

### Debugging and Log Access

Many common errors are easy to detect by checking Flink's log files. If you have access to Flink's web user interface, you can access the JobManager and TaskManager logs from there.

If there are problems starting Flink, you can also use Kubernetes utilities to access the logs. Use `kubectl get pods` to see all running pods.
For the quickstart example from above, you should see three pods:
```
$ kubectl get pods
NAME                                 READY   STATUS             RESTARTS   AGE
flink-jobmanager-589967dcfc-m49xv    1/1     Running            3          3m32s
flink-taskmanager-64847444ff-7rdl4   1/1     Running            3          3m28s
flink-taskmanager-64847444ff-nnd6m   1/1     Running            3          3m28s
```

You can now access the logs by running `kubectl logs flink-jobmanager-589967dcfc-m49xv`

### High-Availability with Standalone Kubernetes

For high availability on Kubernetes, you can use the [existing high availability services]({{< ref "docs/deployment/ha/overview" >}}).

#### Kubernetes High-Availability Services

Session Mode and Application Mode clusters support using the [Kubernetes high availability service]({{< ref "docs/deployment/ha/kubernetes_ha" >}}).
You need to add the following Flink config options to [flink-configuration-configmap.yaml](#common-cluster-resource-definitions).

<span class="label label-info">Note</span> The filesystem which corresponds to the scheme of your configured HA storage directory must be available to the runtime. Refer to [custom Flink image]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#advanced-customization) and [enable plugins]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#using-filesystem-plugins) for more information.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  labels:
    app: flink
data:
  flink-conf.yaml: |+
  ...
    kubernetes.cluster-id: <cluster-id>
    high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
    high-availability.storageDir: hdfs:///flink/recovery
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: 10
  ...
```

Moreover, you have to start the JobManager and TaskManager pods with a service account which has the permissions to create, edit, delete ConfigMaps.
See [how to configure service accounts for pods](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/) for more information.

When High-Availability is enabled, Flink will use its own HA-services for service discovery.
Therefore, JobManager pods should be started with their IP address instead of a Kubernetes service as its `jobmanager.rpc.address`.
Refer to the [appendix](#appendix) for full configuration.

#### Standby JobManagers

Usually, it is enough to only start a single JobManager pod, because Kubernetes will restart it once the pod crashes.
If you want to achieve faster recovery, configure the `replicas` in `jobmanager-session-deployment-ha.yaml` or `parallelism` in `jobmanager-application-ha.yaml` to a value greater than `1` to start standby JobManagers.

### Enabling Queryable State

You can access the queryable state of TaskManager if you create a `NodePort` service for it:
  1. Run `kubectl create -f taskmanager-query-state-service.yaml` to create the `NodePort` service for the `taskmanager` pod. The example of `taskmanager-query-state-service.yaml` can be found in [appendix](#common-cluster-resource-definitions).
  2. Run `kubectl get svc flink-taskmanager-query-state` to get the `<node-port>` of this service. Then you can create the [QueryableStateClient(&lt;public-node-ip&gt;, &lt;node-port&gt;]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}#querying-state) to submit state queries.

### Using Standalone Kubernetes with Reactive Mode

[Reactive Mode]({{< ref "docs/deployment/elastic_scaling" >}}#reactive-mode) allows to run Flink in a mode, where the *Application Cluster* is always adjusting the job parallelism to the available resources. In combination with Kubernetes, the replica count of the TaskManager deployment determines the available resources. Increasing the replica count will scale up the job, reducing it will trigger a scale down. This can also be done automatically by using a [Horizontal Pod Autoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/).

To use Reactive Mode on Kubernetes, follow the same steps as for [deploying a job using an Application Cluster](#deploy-application-cluster). But instead of `flink-configuration-configmap.yaml` use this config map: `flink-reactive-mode-configuration-configmap.yaml`. It contains the `scheduler-mode: reactive` setting for Flink.

Once you have deployed the *Application Cluster*, you can scale your job up or down by changing the replica count in the `flink-taskmanager` deployment.

{{< top >}}

<a name="appendix"> </a>

## 附录

<a name="common-cluster-resource-definitions"> </a>

### 通用集群资源定义

`flink-configuration-configmap.yaml`
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  labels:
    app: flink
data:
  flink-conf.yaml: |+
    jobmanager.rpc.address: flink-jobmanager
    taskmanager.numberOfTaskSlots: 2
    blob.server.port: 6124
    jobmanager.rpc.port: 6123
    taskmanager.rpc.port: 6122
    queryable-state.proxy.ports: 6125
    jobmanager.memory.process.size: 1600m
    taskmanager.memory.process.size: 1728m
    parallelism.default: 2
  log4j-console.properties: |+
    # This affects logging for both user code and Flink
    rootLogger.level = INFO
    rootLogger.appenderRef.console.ref = ConsoleAppender
    rootLogger.appenderRef.rolling.ref = RollingFileAppender

    # Uncomment this if you want to _only_ change Flink's logging
    #logger.flink.name = org.apache.flink
    #logger.flink.level = INFO

    # The following lines keep the log level of common libraries/connectors on
    # log level INFO. The root logger does not override this. You have to manually
    # change the log levels here.
    logger.akka.name = akka
    logger.akka.level = INFO
    logger.kafka.name= org.apache.kafka
    logger.kafka.level = INFO
    logger.hadoop.name = org.apache.hadoop
    logger.hadoop.level = INFO
    logger.zookeeper.name = org.apache.zookeeper
    logger.zookeeper.level = INFO

    # Log all infos to the console
    appender.console.name = ConsoleAppender
    appender.console.type = CONSOLE
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n

    # Log all infos in the given rolling file
    appender.rolling.name = RollingFileAppender
    appender.rolling.type = RollingFile
    appender.rolling.append = false
    appender.rolling.fileName = ${sys:log.file}
    appender.rolling.filePattern = ${sys:log.file}.%i
    appender.rolling.layout.type = PatternLayout
    appender.rolling.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
    appender.rolling.policies.type = Policies
    appender.rolling.policies.size.type = SizeBasedTriggeringPolicy
    appender.rolling.policies.size.size=100MB
    appender.rolling.strategy.type = DefaultRolloverStrategy
    appender.rolling.strategy.max = 10

    # Suppress the irrelevant (wrong) warnings from the Netty channel handler
    logger.netty.name = org.jboss.netty.channel.DefaultChannelPipeline
    logger.netty.level = OFF
```


`flink-reactive-mode-configuration-configmap.yaml`

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  labels:
    app: flink
data:
  flink-conf.yaml: |+
    jobmanager.rpc.address: flink-jobmanager
    taskmanager.numberOfTaskSlots: 2
    blob.server.port: 6124
    jobmanager.rpc.port: 6123
    taskmanager.rpc.port: 6122
    queryable-state.proxy.ports: 6125
    jobmanager.memory.process.size: 1600m
    taskmanager.memory.process.size: 1728m
    parallelism.default: 2
    scheduler-mode: reactive
    execution.checkpointing.interval: 10s
  log4j-console.properties: |+
    # This affects logging for both user code and Flink
    rootLogger.level = INFO
    rootLogger.appenderRef.console.ref = ConsoleAppender
    rootLogger.appenderRef.rolling.ref = RollingFileAppender

    # Uncomment this if you want to _only_ change Flink's logging
    #logger.flink.name = org.apache.flink
    #logger.flink.level = INFO

    # The following lines keep the log level of common libraries/connectors on
    # log level INFO. The root logger does not override this. You have to manually
    # change the log levels here.
    logger.akka.name = akka
    logger.akka.level = INFO
    logger.kafka.name= org.apache.kafka
    logger.kafka.level = INFO
    logger.hadoop.name = org.apache.hadoop
    logger.hadoop.level = INFO
    logger.zookeeper.name = org.apache.zookeeper
    logger.zookeeper.level = INFO

    # Log all infos to the console
    appender.console.name = ConsoleAppender
    appender.console.type = CONSOLE
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n

    # Log all infos in the given rolling file
    appender.rolling.name = RollingFileAppender
    appender.rolling.type = RollingFile
    appender.rolling.append = false
    appender.rolling.fileName = ${sys:log.file}
    appender.rolling.filePattern = ${sys:log.file}.%i
    appender.rolling.layout.type = PatternLayout
    appender.rolling.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
    appender.rolling.policies.type = Policies
    appender.rolling.policies.size.type = SizeBasedTriggeringPolicy
    appender.rolling.policies.size.size=100MB
    appender.rolling.strategy.type = DefaultRolloverStrategy
    appender.rolling.strategy.max = 10

    # Suppress the irrelevant (wrong) warnings from the Netty channel handler
    logger.netty.name = org.jboss.netty.channel.DefaultChannelPipeline
    logger.netty.level = OFF
```

`jobmanager-service.yaml` Optional service, which is only necessary for non-HA mode.
```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager
spec:
  type: ClusterIP
  ports:
  - name: rpc
    port: 6123
  - name: blob-server
    port: 6124
  - name: webui
    port: 8081
  selector:
    app: flink
    component: jobmanager
```

`jobmanager-rest-service.yaml`. Optional service, that exposes the jobmanager `rest` port as public Kubernetes node's port.
```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager-rest
spec:
  type: NodePort
  ports:
  - name: rest
    port: 8081
    targetPort: 8081
    nodePort: 30081
  selector:
    app: flink
    component: jobmanager
```

`taskmanager-query-state-service.yaml`. Optional service, that exposes the TaskManager port to access the queryable state as a public Kubernetes node's port.
```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-taskmanager-query-state
spec:
  type: NodePort
  ports:
  - name: query-state
    port: 6125
    targetPort: 6125
    nodePort: 30025
  selector:
    app: flink
    component: taskmanager
```

<a name="session-cluster-resource-definitions"> </a>

### Session 集群资源定义

`jobmanager-session-deployment-non-ha.yaml`
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      containers:
      - name: jobmanager
        image: apache/flink:{{< stable >}}{{< version >}}-scala{{< scala_version >}}{{< /stable >}}{{< unstable >}}latest{{< /unstable >}}
        args: ["jobmanager"]
        ports:
        - containerPort: 6123
          name: rpc
        - containerPort: 6124
          name: blob-server
        - containerPort: 8081
          name: webui
        livenessProbe:
          tcpSocket:
            port: 6123
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf
        securityContext:
          runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
          items:
          - key: flink-conf.yaml
            path: flink-conf.yaml
          - key: log4j-console.properties
            path: log4j-console.properties
```

`jobmanager-session-deployment-ha.yaml`
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  replicas: 1 # Set the value to greater than 1 to start standby JobManagers
  selector:
    matchLabels:
      app: flink
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      containers:
      - name: jobmanager
        image: apache/flink:{{< stable >}}{{< version >}}-scala{{< scala_version >}}{{< /stable >}}{{< unstable >}}latest{{< /unstable >}}
        env:
        - name: POD_IP
          valueFrom:
            fieldRef:
              apiVersion: v1
              fieldPath: status.podIP
        # The following args overwrite the value of jobmanager.rpc.address configured in the configuration config map to POD_IP.
        args: ["jobmanager", "$(POD_IP)"]
        ports:
        - containerPort: 6123
          name: rpc
        - containerPort: 6124
          name: blob-server
        - containerPort: 8081
          name: webui
        livenessProbe:
          tcpSocket:
            port: 6123
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf
        securityContext:
          runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      serviceAccountName: flink-service-account # Service account which has the permissions to create, edit, delete ConfigMaps
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
          items:
          - key: flink-conf.yaml
            path: flink-conf.yaml
          - key: log4j-console.properties
            path: log4j-console.properties
```

`taskmanager-session-deployment.yaml`
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
spec:
  replicas: 2
  selector:
    matchLabels:
      app: flink
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      containers:
      - name: taskmanager
        image: apache/flink:{{< stable >}}{{< version >}}-scala{{< scala_version >}}{{< /stable >}}{{< unstable >}}latest{{< /unstable >}}
        args: ["taskmanager"]
        ports:
        - containerPort: 6122
          name: rpc
        - containerPort: 6125
          name: query-state
        livenessProbe:
          tcpSocket:
            port: 6122
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf/
        securityContext:
          runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
          items:
          - key: flink-conf.yaml
            path: flink-conf.yaml
          - key: log4j-console.properties
            path: log4j-console.properties
```

<a name="application-cluster-resource-definitions"> </a>

### Application 集群资源定义

`jobmanager-application-non-ha.yaml`
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-jobmanager
spec:
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      restartPolicy: OnFailure
      containers:
        - name: jobmanager
          image: apache/flink:{{< stable >}}{{< version >}}-scala{{< scala_version >}}{{< /stable >}}{{< unstable >}}latest{{< /unstable >}}
          env:
          args: ["standalone-job", "--job-classname", "com.job.ClassName", <optional arguments>, <job arguments>] # optional arguments: ["--job-id", "<job id>", "--fromSavepoint", "/path/to/savepoint", "--allowNonRestoredState"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 8081
              name: webui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: job-artifacts-volume
              mountPath: /opt/flink/usrlib
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: flink-conf.yaml
                path: flink-conf.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: job-artifacts-volume
          hostPath:
            path: /host/path/to/job/artifacts
```

`jobmanager-application-ha.yaml`
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-jobmanager
spec:
  parallelism: 1 # Set the value to greater than 1 to start standby JobManagers
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      restartPolicy: OnFailure
      containers:
        - name: jobmanager
          image: apache/flink:{{< stable >}}{{< version >}}-scala{{< scala_version >}}{{< /stable >}}{{< unstable >}}latest{{< /unstable >}}
          env:
          - name: POD_IP
            valueFrom:
              fieldRef:
                apiVersion: v1
                fieldPath: status.podIP
          # The following args overwrite the value of jobmanager.rpc.address configured in the configuration config map to POD_IP.
          args: ["standalone-job", "--host", "$(POD_IP)", "--job-classname", "com.job.ClassName", <optional arguments>, <job arguments>] # optional arguments: ["--job-id", "<job id>", "--fromSavepoint", "/path/to/savepoint", "--allowNonRestoredState"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 8081
              name: webui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: job-artifacts-volume
              mountPath: /opt/flink/usrlib
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      serviceAccountName: flink-service-account # Service account which has the permissions to create, edit, delete ConfigMaps
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: flink-conf.yaml
                path: flink-conf.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: job-artifacts-volume
          hostPath:
            path: /host/path/to/job/artifacts
```

`taskmanager-job-deployment.yaml`
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
spec:
  replicas: 2
  selector:
    matchLabels:
      app: flink
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      containers:
      - name: taskmanager
        image: apache/flink:{{< stable >}}{{< version >}}-scala{{< scala_version >}}{{< /stable >}}{{< unstable >}}latest{{< /unstable >}}
        env:
        args: ["taskmanager"]
        ports:
        - containerPort: 6122
          name: rpc
        - containerPort: 6125
          name: query-state
        livenessProbe:
          tcpSocket:
            port: 6122
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf/
        - name: job-artifacts-volume
          mountPath: /opt/flink/usrlib
        securityContext:
          runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
          items:
          - key: flink-conf.yaml
            path: flink-conf.yaml
          - key: log4j-console.properties
            path: log4j-console.properties
      - name: job-artifacts-volume
        hostPath:
          path: /host/path/to/job/artifacts
```

{{< top >}}
