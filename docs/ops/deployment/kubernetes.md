---
title:  "Kubernetes Setup"
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

[Kubernetes](https://kubernetes.io) is a container orchestration system.

* This will be replaced by the TOC
{:toc}

## Simple Kubernetes Flink Cluster

A basic Flink cluster deployment in Kubernetes has three components:

* a Deployment for a single Jobmanager
* a Deployment for a pool of Taskmanagers
* a Service exposing the Jobmanager's RPC and UI ports

### Launching the cluster

Using the [resource definitions found below](#simple-kubernetes-flink-cluster-
resources), launch the cluster with the `kubectl` command:

    kubectl create -f jobmanager-deployment.yaml
    kubectl create -f jobmanager-service.yaml
    kubectl create -f taskmanager-deployment.yaml

You can then access the Flink UI via `kubectl proxy`:

1. Run `kubectl proxy` in a terminal
2. Navigate to [http://localhost:8001/api/v1/proxy/namespaces/default/services/flink-jobmanager:8081
](http://localhost:8001/api/v1/proxy/namespaces/default/services/flink-
jobmanager:8081) in your browser

### Deleting the cluster

Again, use `kubectl` to delete the cluster:

    kubectl delete -f jobmanager-deployment.yaml
    kubectl delete -f jobmanager-service.yaml
    kubectl delete -f taskmanager-deployment.yaml

## Advanced Cluster Deployment

An early version of a [Flink Helm chart](https://github.com/docker-flink/
examples) is available on GitHub.

## Appendix

### Simple Kubernetes Flink cluster resources

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
