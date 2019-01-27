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
    kubectl create -f taskmanager-deployment.yaml
    kubectl create -f jobmanager-service.yaml

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

## Run Flink Natively On Kubernetes

<div class="alert alert-warning">
Running Flink natively on kubernetes is an experimental feature. There may be behavioral changes of configuration and cli arguments.
</div>

### Requirements

- Kubernetes 1.6 or above
- kubectl has access to list, create, delete pods and services (can be configured in ~/.kube/config). You can verify that by `kubectl auth can-i <list|create|edit|delete> pods`
- [Kubernetes DNS](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/) enabled

### Docker images

Use the following command to build a image with user jar

{% highlight bash %}
cd flink-container/docker

./build.sh --job-jar examples/streaming/WordCount.jar --from-local-dist --image-name <ImageName>

docker push <ImageName>
{% endhighlight %}

### Flink Kubernetes Session

#### Start a Session

Use the following command to start a session
{% highlight bash %}
./bin/kubernetes-session.sh
{% endhighlight %}

This command will show you the following overview:
{% highlight bash %}
Usage:
   Required
     -ms,--master <arg>   Kubernetes cluster master url
     -n,--pods <arg>      Number of kubernetes pods to allocate (=Number of Task Managers)
   Optional
     -D <property=value>             use value for given property
     -d,--detached                   If present, runs the job in detached mode
     -h,--help                       Help for the kubernetes session CLI.
     -i,--image <arg>                Container image to use for Flink containers.
                                     Individual container types (e.g. jobmanager or taskmanager)
                                     can also be configured to use different images if desired,
                                     by setting the container type-specific image name.
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container [in MB]
     -nm,--name <arg>                Set a custom name for the flink cluster on kubernetes
     -ns,--namespace <arg>           Specify kubernetes namespace.
     -s,--slots <arg>                Number of slots per TaskManager
     -sa,--serviceaddress <arg>      The exposed address of kubernetes service to submit job and view dashboard.
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container [in MB]
{% endhighlight %}

**Example:** Issue the following command to allocate 4 Task Managers, with 8GB of memory and 32 processing slots each:
{% highlight bash %}
./bin/kubernetes-session.sh -ms https://k8s-master:port -n 4 -tm 8192 -i flink-k8s:latest
{% endhighlight %}

Blob Server and Task Manager are required to use nonrandom RPC ports. They can be configured with the following config options, either in flink-conf.yaml or as `-D` flags at starting the session.

{% highlight bash %}
blob.server.port: 7788
taskmanager.rpc.port: 7789
{% endhighlight %}

Once Flink is deployed in your kubernetes cluster, it will show you the connection details of the Job Manager.

#### Detached Kubernetes Session

In detached mode, the Flink client will exit after submitting the the service to the kubernetes cluster. If you want to stop the Kubernetes session, please use the Kubernetes utilities(`kubectl delete service <ServiceName>`). You can also start another client and attach to the session to stop it.

#### Accessing Job Manager UI

There are several ways to expose a Service onto an external (outside of your cluster) IP address. This could be changed by `kubernetes.service.exposed.type`.

- ClusterIP: Default value, exposes the service on a cluster-internal IP. The Service is only reachable from within the cluster. If you want to access the Job Manager ui or submit job to the existing session, you need to start a local proxy.
{% highlight bash %}
kubectl port-forward service/<ServiceName> 8081
{% endhighlight %}
- NodePort: Exposes the service on each Node’s IP at a static port (the `NodePort`). `<NodeIP>:<NodePort>` could be used to contact the Job Manager Service.
- LoadBalancer: Exposes the service externally using a cloud provider’s load balancer. You could use `kubectl get services/<ServiceName>` to get EXTERNAL-IP for ServiceAddress argument. 
- ExternalName: Map a service to a DNS name, not supported in current version.

Navigate to [publishing services in Kubernetes](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types) to get more information.

#### Submitting job to an existing Session

Use the following command to submit job
{% highlight bash %}
bin/flink run -m kubernetes-cluster -knm <ClusterId> -ksa <ServiceAddress> examples/streaming/WordCount.jar
{% endhighlight %}

- The ClusterId is specified by `-nm` when starting a session. If you do not specify a certain name, Flink client will generate a UUID for you session cluster.
- The ServiceName is auto generated following the pattern `<ClusterId>-service`
- The ServiceAddress is the address of Job Manager service. It could be `localhost`, `<NodeIP>:<NodePort>` or `EXTERNAL-IP` based on exposed type.

#### Attach to an existing Session
Use the following command to attach to a session.
{% highlight bash %}
./bin/kubernetes-session.sh -ms https://k8s-master:port -nm <ClusterId> -sa <ServiceAddress>
{% endhighlight %}

### Run a single Flink job on Kubernetes

The documentation above describes how to start a Flink cluster within a Kubernetes environment. It is also possible to launch a new Flink cluster for executing each individual job with better isolation.

***Example:***

{% highlight bash %}
./bin/flink run -m kubernetes-cluster -kms https://k8s-master:port -kn 4 -ki flink-k8s:latest examples/streaming/WordCount.jar
{% endhighlight %}

The command line options of the Kubernetes session are also available with the ./bin/flink tool. They are prefixed with a k or kubernetes (for the long argument options).

Note: In attach mode, the argument `-kn` (number of TaskManagers) is required and `kubernetes.service.exposed.type` must be either `NODE_PORT` or `LOAD_BALANCER`.

Note: You can use a different configuration directory per job by setting the environment variable FLINK_CONF_DIR. To use this copy the conf directory from the Flink distribution and modify, for example, the logging settings on a per-job basis.

Note: It is also possible to "fire and forget" a Flink job to the Kubernetes cluster in detached mode. Use -m to specify the kubernetes-cluster and -d for detached mode. The `-kn` argument will not take effect and **resource is allocated as demand.** Also in this case, your application will not get any accumulator results or exceptions from the ExecutionEnvironment.execute() call!

Note: If you want to accessing the Job Manager UI or get the logs, set `kubernetes.destroy-perjob-cluster.after-job-finished=false` and the Flink cluster will not be destroyed after finished.

### Debug a failed Kubernetes cluster

Users could use the following command to retrieve logs of Job Manager and Task Manager.
{% highlight bash %}
kubectl logs pod/<PodName>
{% endhighlight %}

### Kubernetes concepts

#### NameSpaces

[Namespaces in Kubernetes](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) are a way to divide cluster resources between multiple users (via resource quota). It is similar to queue concept in Yarn cluster. Flink on Kubernetes can use namespaces to launch Flink clusters. The namespace could be specified by `-ns` argument when starting a Flink cluster.

[ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/) provides constraints that limit aggregate resource consumption per namespace. It can limit the quantity of objects that can be created in a namespace by type, as well as the total amount of compute resources that may be consumed by resources in that project.

#### RBAC

Role-based access control ([RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)) is a method of regulating access to compute or network resources based on the roles of individual users within an enterprise. So users can configure RBAC roles and service accounts used by Flink JobManager to access the Kubernetes API server within the Kubernetes cluster. 

Every namespace will have a default service account. However, the `default` service account may not have the permission to create or delete pods within the Kubernetes cluster. So users may need to specify another service account that has the right role binded. The configuration option `kubernetes.jobmanager.service-account` could be used to set the service account.
Use the following command to make the JobManager pod use the `flink` service account to create and delete TaskManager pods.

{% highlight bash %}
-D kubernetes.jobmanager.service-account=flink
{% endhighlight %}

If the `flink` service account does not exist, use the following command to create a new one and set the role binding.

{% highlight bash %}
kubectl create serviceaccount flink
kubectl create clusterrolebinding flink-role-binding --clusterrole=edit --serviceaccount=default:flink --namespace=default
{% endhighlight %}

Navigate to [RBAC Authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) for more information.

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

### Run Flink Natively On Kubernetes Internals

This section briefly describes how Flink and Kubernetes interact.

<img src="{{ site.baseurl }}/fig/FlinkOnKubernetesNative.svg" class="img-responsive">

When starting a Kubernetes session, Flink client will first (step 1) contact to Kubernetes ApiServer to submit the cluster description, including ConfigMap spec, Job Manager Service spec, Job Manager Replica Controller spec and Owner Reference.

The next step (step 2), Kubernetes Master create the required components. The Kubelet will pull the image, prepare and mount the volume and then execute the start command.

Once Flink JobManager pod is launched, the ResourceManager will allocate (step 3) the specified number of Task Managers. The JobManager will generate a new configuration for the TaskManagers, with the address of Job Manager set to ServiceName. This allows the TaskManagers to connect back to the JobManager after failover).

After all TaskManagers are launched and registered to ResourceManager and JobManager, the session is ready to accept jobs.
{% top %}
