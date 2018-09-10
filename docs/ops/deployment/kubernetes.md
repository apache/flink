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

This page describes how to deploy a Flink job and session cluster on [Kubernetes](https://kubernetes.io).

* This will be replaced by the TOC
{:toc}

## Setup Kubernetes

Please follow [Kubernetes' setup guide](https://kubernetes.io/docs/setup/) in order to deploy a Kubernetes cluster.
If you want to run Kubernetes locally, we recommend using [MiniKube](https://kubernetes.io/docs/setup/minikube/).

<div class="alert alert-info" markdown="span">
  <strong>Note:</strong> If using MiniKube please make sure to execute `minikube ssh 'sudo ip link set docker0 promisc on'` before deploying a Flink cluster. 
  Otherwise Flink components are not able to self reference themselves through a Kubernetes service. 
</div>

## Flink session cluster on Kubernetes

A Flink session cluster is executed as a long-running Kubernetes Deployment. 
Note that you can run multiple Flink jobs on a session cluster.
Each job needs to be submitted to the cluster after the cluster has been deployed.

A basic Flink session cluster deployment in Kubernetes has three components:

* a Deployment/Job which runs the JobManager
* a Deployment for a pool of TaskManagers
* a Service exposing the JobManager's REST and UI ports

### Deploy Flink session cluster on Kubernetes

Using the resource definitions for a [session cluster](#session-cluster-resource-definitions), launch the cluster with the `kubectl` command:

    kubectl create -f jobmanager-service.yaml
    kubectl create -f jobmanager-deployment.yaml
    kubectl create -f taskmanager-deployment.yaml

You can then access the Flink UI via `kubectl proxy`:

1. Run `kubectl proxy` in a terminal
2. Navigate to [http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:ui/proxy](http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:ui/proxy) in your browser

In order to terminate the Flink session cluster, use `kubectl`:

    kubectl delete -f jobmanager-deployment.yaml
    kubectl delete -f taskmanager-deployment.yaml
    kubectl delete -f jobmanager-service.yaml

## Flink job cluster on Kubernetes

A Flink job cluster is a dedicated cluster which runs a single job. 
The job is part of the image and, thus, there is no extra job submission needed. 

### Creating the job-specific image

The Flink job cluster image needs to contain the user code jars of the job for which the cluster is started.
Therefore, one needs to build a dedicated container image for every job.
Please follow these [instructions](https://github.com/apache/flink/blob/{{ site.github_branch }}/flink-container/docker/README.md) to build the Docker image.
    
### Deploy Flink job cluster on Kubernetes

In order to deploy the a job cluster on Kubernetes please follow these [instructions](https://github.com/apache/flink/blob/{{ site.github_branch }}/flink-container/kubernetes/README.md#deploy-flink-job-cluster).

## Advanced Cluster Deployment

An early version of a [Flink Helm chart](https://github.com/docker-flink/examples) is available on GitHub.

## Appendix

### Session cluster resource definitions

The Deployment definitions use the pre-built image `flink:latest` which can be found [on Docker Hub](https://hub.docker.com/r/_/flink/).
The image is built from this [Github repository](https://github.com/docker-flink/docker-flink).

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
