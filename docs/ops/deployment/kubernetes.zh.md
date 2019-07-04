---
title:  "Kubernetes 安装"
nav-title: Kubernetes
nav-parent_id: deployment
nav-pos: 4
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

This page describes how to deploy a Flink job and session cluster on [Kubernetes](https://kubernetes.io).

* This will be replaced by the TOC
{:toc}

## 安装Kubernetes

请先参阅[Kubernetes安装教程](https://kubernetes.io/docs/setup/)来部署Kubernetes集群。

如果想在本地运行Kubernetes ，这里建议使用[MiniKube](https://kubernetes.io/docs/setup/minikube/)。

<div class="alert alert-info" markdown="span">
  <strong>注意：</strong> 如果使用MiniKube 请确保在部署Flink集群之前先执行 `minikube ssh 'sudo ip link set docker0 promisc on'` ，否则Flink组件不能自动地将自己映射到Kubernetes Service中。
</div>


## 部署在Kubernetes上的Flink session集群

Flink session集群执行时是作为Kubernetes上长期运行的Deployment。这里值得注意的是，可以在一个session集群上运行多个Flink作业。当然，只有session集群部署好以后才可以在上面提交Flink作业。

在Kubernetes上部署一个基本的Flink session集群时，一般包括下面三个组件：

* 运行JobManager的Deployment或者Job
* 运行TaskManager池的Deployment
* 暴露了JobManager上REST和UI端口的Service

### 在Kubernetes上部署Flink session集群

对于[session集群](#Session集群资源定义)，通过`kubectl`命令使用对应的资源定义文件来启动集群，如下所示：

    kubectl create -f jobmanager-service.yaml
    kubectl create -f jobmanager-deployment.yaml
    kubectl create -f taskmanager-deployment.yaml

接下来可以通过`kubectl proxy`命令来访问Flink UI，步骤如下：

1. 在终端运行`kubectl proxy`命令
2. 在浏览器中导航到http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:ui/proxy页面

关闭集群时，仍然使用`kubectl`命令，如下所示：

    kubectl delete -f jobmanager-deployment.yaml
    kubectl delete -f taskmanager-deployment.yaml
    kubectl delete -f jobmanager-service.yaml

## 部署在Kubernetes上的Flink job集群

Flink job集群是只运行单个flink job的专用集群。

Job本身就是镜像的一部分，因此也无需再提交额外的job。

### 创建特定Job镜像

Flink job集群镜像需要包含作业的用户代码打成的jar包才能启动。

因此，需要为每个作业都构建一个该作业专用的容器镜像。

Docker镜像构建请参考[Docker镜像构建](https://github.com/apache/flink/blob/master/flink-container/docker/README.md)。    

### 在Kubernetes上部署Flink job集群

在Kubernetes上部署job集群，请参考[Flink job集群部署](https://github.com/apache/flink/blob/master/flink-container/kubernetes/README.md#deploy-flink-job-cluster)。

## 高级集群部署

GitHub上提供了[Flink Helm chart](https://github.com/docker-flink/examples) 的早期版本。

## 附录

### Session集群资源定义

Session集群Deployment的资源定义可使用内置镜像：`flink:latest`。

该镜像是从[docker-flink](https://github.com/docker-flink/docker-flink)这个GitHub项目中构建的，可以在[Docker Hub](https://hub.docker.com/r/_/flink/)上找到该镜像。

`jobmanager-deployment.yaml`
{% highlight yaml %}
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      containers:
      - name: jobmanager
        image: flink:latest
        args:
        - jobmanager
        ports:
        - containerPort: 6123
          name: rpc
        - containerPort: 6124
          name: blob
        - containerPort: 6125
          name: query
        - containerPort: 8081
          name: ui
        env:
        - name: JOB_MANAGER_RPC_ADDRESS
          value: flink-jobmanager
{% endhighlight %}

`taskmanager-deployment.yaml`
{% highlight yaml %}
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: flink-taskmanager
spec:
  replicas: 2
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      containers:
      - name: taskmanager
        image: flink:latest
        args:
        - taskmanager
        ports:
        - containerPort: 6121
          name: data
        - containerPort: 6122
          name: rpc
        - containerPort: 6125
          name: query
        env:
        - name: JOB_MANAGER_RPC_ADDRESS
          value: flink-jobmanager
{% endhighlight %}

`jobmanager-service.yaml`
{% highlight yaml %}
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager
spec:
  ports:
  - name: rpc
    port: 6123
  - name: blob
    port: 6124
  - name: query
    port: 6125
  - name: ui
    port: 8081
    selector:
    app: flink
    component: jobmanager
{% endhighlight %}

{% top %}
