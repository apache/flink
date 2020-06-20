---
title:  "Kubernetes Setup"
nav-title: Kubernetes
nav-parent_id: deployment
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

This page describes how to deploy a *Flink Job* and *Session cluster* on [Kubernetes](https://kubernetes.io).

* This will be replaced by the TOC
{:toc}

{% info %} This page describes deploying a [standalone](#cluster_setup.html) Flink cluster on top of Kubernetes.
You can find more information on native Kubernetes deployments [here]({{ site.baseurl }}/ops/deployment/native_kubernetes.html).

## Setup Kubernetes

Please follow [Kubernetes' setup guide](https://kubernetes.io/docs/setup/) in order to deploy a Kubernetes cluster.
If you want to run Kubernetes locally, we recommend using [MiniKube](https://kubernetes.io/docs/setup/minikube/).

<div class="alert alert-info" markdown="span">
  <strong>Note:</strong> If using MiniKube please make sure to execute `minikube ssh 'sudo ip link set docker0 promisc on'` before deploying a Flink cluster.
  Otherwise Flink components are not able to self reference themselves through a Kubernetes service.
</div>

## Flink Docker image

Before deploying the Flink Kubernetes components, please read [the Flink Docker image documentation](docker.html),
[its tags](docker.html#image-tags), [how to customize the Flink Docker image](docker.html#customize-flink-image) and
[enable plugins](docker.html#using-plugins) to use the image in the Kubernetes definition files.

## Deploy Flink cluster on Kubernetes

Using [the common resource definitions](#common-cluster-resource-definitions), launch the common cluster components
with the `kubectl` command:

```sh
    kubectl create -f flink-configuration-configmap.yaml
    kubectl create -f jobmanager-service.yaml
```

Note that you could define your own customized options of `flink-conf.yaml` within `flink-configuration-configmap.yaml`.

Then launch the specific components depending on whether you want to deploy a [Session](#deploy-session-cluster) or [Job](#deploy-job-cluster) cluster.

You can then access the Flink UI via different ways:
*  `kubectl proxy`:

    1. Run `kubectl proxy` in a terminal.
    2. Navigate to [http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:webui/proxy](http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:webui/proxy) in your browser.

*  `kubectl port-forward`:
    1. Run `kubectl port-forward ${flink-jobmanager-pod} 8081:8081` to forward your jobmanager's web ui port to local 8081.
    2. Navigate to [http://localhost:8081](http://localhost:8081) in your browser.
    3. Moreover, you could use the following command below to submit jobs to the cluster:
    {% highlight bash %}./bin/flink run -m localhost:8081 ./examples/streaming/WordCount.jar{% endhighlight %}

*  Create a `NodePort` service on the rest service of jobmanager:
    1. Run `kubectl create -f jobmanager-rest-service.yaml` to create the `NodePort` service on jobmanager. The example of `jobmanager-rest-service.yaml` can be found in [appendix](#common-cluster-resource-definitions).
    2. Run `kubectl get svc flink-jobmanager-rest` to know the `node-port` of this service and navigate to [http://&lt;public-node-ip&gt;:&lt;node-port&gt;](http://<public-node-ip>:<node-port>) in your browser.
    3. If you use minikube, you can get its public ip by running `minikube ip`.
    4. Similarly to the `port-forward` solution, you could also use the following command below to submit jobs to the cluster:

        {% highlight bash %}./bin/flink run -m <public-node-ip>:<node-port> ./examples/streaming/WordCount.jar{% endhighlight %}

In order to terminate the Flink cluster, delete the specific [Session](#deploy-session-cluster) or [Job](#deploy-job-cluster) cluster components
and use `kubectl` to terminate the common components:

```sh
    kubectl delete -f jobmanager-service.yaml
    kubectl delete -f flink-configuration-configmap.yaml
    # if created then also the rest service
    kubectl delete -f jobmanager-rest-service.yaml
```

### Deploy Session Cluster

A *Flink Session cluster* is executed as a long-running Kubernetes Deployment.
Note that you can run multiple Flink jobs on a *Session cluster*.
Each job needs to be submitted to the cluster after the cluster has been deployed.

A *Flink Session cluster* deployment in Kubernetes has at least three components:

* a *Deployment* which runs a [Flink Master]({{ site.baseurl }}/concepts/glossary.html#flink-master)
* a *Deployment* for a pool of [TaskManagers]({{ site.baseurl }}/concepts/glossary.html#flink-taskmanager)
* a *Service* exposing the *Flink Master's* REST and UI ports

After creating [the common cluster components](#deploy-flink-cluster-on-kubernetes), use [the Session specific resource definitions](#session-cluster-resource-definitions)
to launch the *Session cluster* with the `kubectl` command:

```sh
    kubectl create -f jobmanager-session-deployment.yaml
    kubectl create -f taskmanager-session-deployment.yaml
```

To terminate the *Session cluster*, these components can be deleted along with [the common ones](#deploy-flink-cluster-on-kubernetes) with the `kubectl` command:

```sh
    kubectl delete -f taskmanager-session-deployment.yaml
    kubectl delete -f jobmanager-session-deployment.yaml
```

### Deploy Job Cluster

A *Flink Job cluster* is a dedicated cluster which runs a single job.
You can find more details [here](#start-a-job-cluster).

A basic *Flink Job cluster* deployment in Kubernetes has three components:

* a *Job* which runs a *Flink Master*
* a *Deployment* for a pool of *TaskManagers*
* a *Service* exposing the *Flink Master's* REST and UI ports

Check [the Job cluster specific resource definitions](#job-cluster-resource-definitions) and adjust them accordingly.

The `args` attribute in the `jobmanager-job.yaml` has to specify the main class of the user job.
See also [how to specify the Flink Master arguments](docker.html#flink-master-additional-command-line-arguments) to understand
how to pass other `args` to the Flink image in the `jobmanager-job.yaml`.

The *job artifacts* should be available from the `job-artifacts-volume` in [the resource definition examples](#job-cluster-resource-definitions).
The definition examples mount the volume as a local directory of the host assuming that you create the components in a minikube cluster.
If you do not use a minikube cluster, you can use any other type of volume, available in your Kubernetes cluster, to supply the *job artifacts*.
Alternatively, you can build [a custom image](docker.html#start-a-job-cluster) which already contains the artifacts instead.

After creating [the common cluster components](#deploy-flink-cluster-on-kubernetes), use [the Job cluster specific resource definitions](#job-cluster-resource-definitions)
to launch the cluster with the `kubectl` command:

```sh
    kubectl create -f jobmanager-job.yaml
    kubectl create -f taskmanager-job-deployment.yaml
```

To terminate the single job cluster, these components can be deleted along with [the common ones](#deploy-flink-cluster-on-kubernetes)
with the `kubectl` command:

```sh
    kubectl delete -f taskmanager-job-deployment.yaml
    kubectl delete -f jobmanager-job.yaml
```

## Appendix

### Common cluster resource definitions

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
    taskmanager.numberOfTaskSlots: 2
    blob.server.port: 6124
    jobmanager.rpc.port: 6123
    taskmanager.rpc.port: 6122
    queryable-state.server.ports: 6125
    jobmanager.memory.process.size: 1600m
    taskmanager.memory.process.size: 1728m
    parallelism.default: 2
  log4j.properties: |+
    rootLogger.level = INFO
    rootLogger.appenderRef.file.ref = MainAppender
    logger.akka.name = akka
    logger.akka.level = INFO
    logger.kafka.name= org.apache.kafka
    logger.kafka.level = INFO
    logger.hadoop.name = org.apache.hadoop
    logger.hadoop.level = INFO
    logger.zookeeper.name = org.apache.zookeeper
    logger.zookeeper.level = INFO
    appender.main.name = MainAppender
    appender.main.type = File
    appender.main.append = false
    appender.main.fileName = ${sys:log.file}
    appender.main.layout.type = PatternLayout
    appender.main.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
    logger.netty.name = org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline
    logger.netty.level = ERROR
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
  - name: blob-server
    port: 6124
  - name: query-state
    port: 6125
  - name: webui
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
    nodePort: 8081
  selector:
    app: flink
    component: jobmanager
{% endhighlight %}

### Session cluster resource definitions

`jobmanager-session-deployment.yaml`
{% highlight yaml %}
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
        image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
        args: ["jobmanager"]
        ports:
        - containerPort: 6123
          name: rpc
        - containerPort: 6124
          name: blob-server
        - containerPort: 6125
          name: query-state
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
          - key: log4j.properties
            path: log4j.properties
{% endhighlight %}

`taskmanager-session-deployment.yaml`
{% highlight yaml %}
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
        image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
        args: ["taskmanager"]
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
        securityContext:
          runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
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

### Job cluster resource definitions

`jobmanager-job.yaml`
{% highlight yaml %}
apiVersion: batch/v1
kind: Job
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
      restartPolicy: OnFailure
      containers:
        - name: jobmanager
          image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
          env:
          args: ["standalone-job", "--job-classname", "com.job.ClassName", ["--job-id", "<job id>",] ["--fromSavepoint", "/path/to/savepoint", ["--allowNonRestoredState",]] [job arguments]]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 6125
              name: query-state
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
              - key: log4j.properties
                path: log4j.properties
        - name: job-artifacts-volume
          hostPath:
            path: /host/path/to/job/artifacts
{% endhighlight %}

`taskmanager-job-deployment.yaml`
{% highlight yaml %}
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
        image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
        env:
        args: ["taskmanager"]
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
          - key: log4j.properties
            path: log4j.properties
      - name: job-artifacts-volume
        hostPath:
          path: /host/path/to/job/artifacts
{% endhighlight %}

{% top %}
