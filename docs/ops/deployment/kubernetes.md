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

    kubectl create -f flink-configuration-configmap.yaml
    kubectl create -f jobmanager-service.yaml
    kubectl create -f jobmanager-deployment.yaml
    kubectl create -f taskmanager-deployment.yaml

Note that you could define your own customized options of `flink-conf.yaml` within `flink-configuration-configmap.yaml`.

You can then access the Flink UI via different ways:
*  `kubectl proxy`:

    1. Run `kubectl proxy` in a terminal.
    2. Navigate to [http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:ui/proxy](http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:ui/proxy) in your browser.

*  `kubectl port-forward`:
    1. Run `kubectl port-forward ${flink-jobmanager-pod} 8081:8081` to forward your jobmanager's web ui port to local 8081.
    2. Navigate to [http://localhost:8081](http://localhost:8081) in your browser.
    3. Moreover, you could use the following command below to submit jobs to the cluster:
    {% highlight bash %}./bin/flink run -m localhost:8081 ./examples/streaming/WordCount.jar{% endhighlight %}

*  Create a `NodePort` service on the rest service of jobmanager:
    1. Run `kubectl create -f jobmanager-rest-service.yaml` to create the `NodePort` service on jobmanager. The example of `jobmanager-rest-service.yaml` can be found in [appendix](#session-cluster-resource-definitions).
    2. Run `kubectl get svc flink-jobmanager-rest` to know the `node-port` of this service and navigate to [http://&lt;public-node-ip&gt;:&lt;node-port&gt;](http://<public-node-ip>:<node-port>) in your browser.
    3. Similarly to `port-forward` solution, you could also use the following command below to submit jobs to the cluster:

        {% highlight bash %}./bin/flink run -m <public-node-ip>:<node-port> ./examples/streaming/WordCount.jar{% endhighlight %}

In order to terminate the Flink session cluster, use `kubectl`:

    kubectl delete -f jobmanager-deployment.yaml
    kubectl delete -f taskmanager-deployment.yaml
    kubectl delete -f jobmanager-service.yaml
    kubectl delete -f flink-configuration-configmap.yaml

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

`flink-configuration-configmap.yaml`
{% highlight yaml %}
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  labels:
    app: flink
data:
  flink-conf.yaml: |+
    jobmanager.rpc.address: flink-jobmanager
    taskmanager.numberOfTaskSlots: 1
    blob.server.port: 6124
    jobmanager.rpc.port: 6123
    taskmanager.rpc.port: 6122
    jobmanager.heap.size: 1024m
    taskmanager.heap.size: 1024m
  log4j.properties: |+
    log4j.rootLogger=INFO, file
    log4j.logger.akka=INFO
    log4j.logger.org.apache.kafka=INFO
    log4j.logger.org.apache.hadoop=INFO
    log4j.logger.org.apache.zookeeper=INFO
    log4j.appender.file=org.apache.log4j.FileAppender
    log4j.appender.file.file=${log.file}
    log4j.appender.file.layout=org.apache.log4j.PatternLayout
    log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
    log4j.logger.org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline=ERROR, file
{% endhighlight %}

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
        workingDir: /opt/flink
        command: ["/bin/bash", "-c", "$FLINK_HOME/bin/jobmanager.sh start;\
          while :;
          do
            if [[ -f $(find log -name '*jobmanager*.log' -print -quit) ]];
              then tail -f -n +1 log/*jobmanager*.log;
            fi;
          done"]
        ports:
        - containerPort: 6123
          name: rpc
        - containerPort: 6124
          name: blob
        - containerPort: 8081
          name: ui
        livenessProbe:
          tcpSocket:
            port: 6123
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
          items:
          - key: flink-conf.yaml
            path: flink-conf.yaml
          - key: log4j.properties
            path: log4j.properties
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
        workingDir: /opt/flink
        command: ["/bin/bash", "-c", "$FLINK_HOME/bin/taskmanager.sh start; \
          while :;
          do
            if [[ -f $(find log -name '*taskmanager*.log' -print -quit) ]];
              then tail -f -n +1 log/*taskmanager*.log;
            fi;
          done"]
        ports:
        - containerPort: 6122
          name: rpc
        livenessProbe:
          tcpSocket:
            port: 6122
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf/
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
          items:
          - key: flink-conf.yaml
            path: flink-conf.yaml
          - key: log4j.properties
            path: log4j.properties

{% endhighlight %}

`jobmanager-service.yaml`
{% highlight yaml %}
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager
spec:
  type: ClusterIP
  ports:
  - name: rpc
    port: 6123
  - name: blob
    port: 6124
  - name: ui
    port: 8081
  selector:
    app: flink
    component: jobmanager
{% endhighlight %}

`jobmanager-rest-service.yaml`. Optional service, that exposes the jobmanager `rest` port as public Kubernetes node's port.
{% highlight yaml %}
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
  selector:
    app: flink
    component: jobmanager
{% endhighlight %}

{% top %}
