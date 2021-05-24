---
title: Native Kubernetes
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

# Native Kubernetes

This page describes how to deploy Flink natively on [Kubernetes](https://kubernetes.io).

## Getting Started

This *Getting Started* section guides you through setting up a fully functional Flink Cluster on Kubernetes.

### Introduction

Kubernetes is a popular container-orchestration system for automating computer application deployment, scaling, and management.
Flink's native Kubernetes integration allows you to directly deploy Flink on a running Kubernetes cluster.
Moreover, Flink is able to dynamically allocate and de-allocate TaskManagers depending on the required resources because it can directly talk to Kubernetes.

### Preparation

The *Getting Started* section assumes a running Kubernetes cluster fulfilling the following requirements:

- Kubernetes >= 1.9.
- KubeConfig, which has access to list, create, delete pods and services, configurable via `~/.kube/config`. You can verify permissions by running `kubectl auth can-i <list|create|edit|delete> pods`.
- Enabled Kubernetes DNS.
- `default` service account with [RBAC](#rbac) permissions to create, delete pods.

If you have problems setting up a Kubernetes cluster, then take a look at [how to setup a Kubernetes cluster](https://kubernetes.io/docs/setup/).

### Starting a Flink Session on Kubernetes

Once you have your Kubernetes cluster running and `kubectl` is configured to point to it, you can launch a Flink cluster in [Session Mode]({{< ref "docs/deployment/overview" >}}#session-mode) via

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
When using [Minikube](https://minikube.sigs.k8s.io/docs/), you need to call `minikube tunnel` in order to [expose Flink's LoadBalancer service on Minikube](https://minikube.sigs.k8s.io/docs/handbook/accessing/#using-minikube-tunnel).
{{< /hint >}}

Congratulations! You have successfully run a Flink application by deploying Flink on Kubernetes.

{{< top >}}

## Deployment Modes

For production use, we recommend deploying Flink Applications in the [Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode), as these modes provide a better isolation for the Applications.

### Application Mode

The [Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode) requires that the user code is bundled together with the Flink image because it runs the user code's `main()` method on the cluster.
The Application Mode makes sure that all Flink components are properly cleaned up after the termination of the application.

The Flink community provides a [base Docker image]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#docker-hub-flink-images) which can be used to bundle the user code:

```dockerfile
FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY /path/of/my-flink-job.jar $FLINK_HOME/usrlib/my-flink-job.jar
```

After creating and publishing the Docker image under `custom-image-name`, you can start an Application cluster with the following command:

```bash
$ ./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=my-first-application-cluster \
    -Dkubernetes.container.image=custom-image-name \
    local:///opt/flink/usrlib/my-flink-job.jar
```

<span class="label label-info">Note</span> `local` is the only supported scheme in Application Mode.

The `kubernetes.cluster-id` option specifies the cluster name and must be unique.
If you do not specify this option, then Flink will generate a random name.

The `kubernetes.container.image` option specifies the image to start the pods with.

Once the application cluster is deployed you can interact with it:

```bash
# List running job on the cluster
$ ./bin/flink list --target kubernetes-application -Dkubernetes.cluster-id=my-first-application-cluster
# Cancel running job
$ ./bin/flink cancel --target kubernetes-application -Dkubernetes.cluster-id=my-first-application-cluster <jobId>
```

You can override configurations set in `conf/flink-conf.yaml` by passing key-value pairs `-Dkey=value` to `bin/flink`.

### Per-Job Cluster Mode

Flink on Kubernetes does not support Per-Job Cluster Mode.

### Session Mode

You have seen the deployment of a Session cluster in the [Getting Started](#getting-started) guide at the top of this page.

The Session Mode can be executed in two modes:

* **detached mode** (default): The `kubernetes-session.sh` deploys the Flink cluster on Kubernetes and then terminates.

* **attached mode** (`-Dexecution.attached=true`): The `kubernetes-session.sh` stays alive and allows entering commands to control the running Flink cluster.
  For example, `stop` stops the running Session cluster.
  Type `help` to list all supported commands.

In order to re-attach to a running Session cluster with the cluster id `my-first-flink-cluster` use the following command:

```bash
$ ./bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dexecution.attached=true
```

You can override configurations set in `conf/flink-conf.yaml` by passing key-value pairs `-Dkey=value` to `bin/kubernetes-session.sh`.

#### Stop a Running Session Cluster

In order to stop a running Session Cluster with cluster id `my-first-flink-cluster` you can either [delete the Flink deployment](#manual-resource-cleanup) or use:

```bash
$ echo 'stop' | ./bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dexecution.attached=true
```

{{< top >}}

## Flink on Kubernetes Reference

### Configuring Flink on Kubernetes

The Kubernetes-specific configuration options are listed on the [configuration page]({{< ref "docs/deployment/config" >}}#kubernetes).

Flink uses [Fabric8 Kubernetes client](https://github.com/fabric8io/kubernetes-client) to communicate with Kubernetes APIServer to create/delete Kubernetes resources(e.g. Deployment, Pod, ConfigMap, Service, etc.), as well as watch the Pods and ConfigMaps.
Except for the above Flink config options, some [expert options](https://github.com/fabric8io/kubernetes-client#configuring-the-client) of Fabric8 Kubernetes client could be configured via system properties or environment variables.

For example, users could use the following Flink config options to set the concurrent max requests, which allows running more jobs in a session cluster when [Kubernetes HA Services]({{< ref "docs/deployment/ha/kubernetes_ha" >}}) are used.
Please note that, each Flink job will consume `3` concurrent requests.

```yaml
containerized.master.env.KUBERNETES_MAX_CONCURRENT_REQUESTS: 200
env.java.opts.jobmanager: "-Dkubernetes.max.concurrent.requests=200"
```

### Accessing Flink's Web UI

Flink's Web UI and REST endpoint can be exposed in several ways via the [kubernetes.rest-service.exposed.type]({{< ref "docs/deployment/config" >}}#kubernetes-rest-service-exposed-type) configuration option.

- **ClusterIP**: Exposes the service on a cluster-internal IP.
  The Service is only reachable within the cluster.
  If you want to access the JobManager UI or submit job to the existing session, you need to start a local proxy.
  You can then use `localhost:8081` to submit a Flink job to the session or view the dashboard.

```bash
$ kubectl port-forward service/<ServiceName> 8081
```

- **NodePort**: Exposes the service on each Node’s IP at a static port (the `NodePort`).
  `<NodeIP>:<NodePort>` can be used to contact the JobManager service.
  `NodeIP` can also be replaced with the Kubernetes ApiServer address. 
  You can find its address in your kube config file.

- **LoadBalancer**: Exposes the service externally using a cloud provider’s load balancer.
  Since the cloud provider and Kubernetes needs some time to prepare the load balancer, you may get a `NodePort` JobManager Web Interface in the client log.
  You can use `kubectl get services/<cluster-id>-rest` to get EXTERNAL-IP and construct the load balancer JobManager Web Interface manually `http://<EXTERNAL-IP>:8081`.

Please refer to the official documentation on [publishing services in Kubernetes](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types) for more information.

{{< hint warning >}}
Depending on your environment, starting a Flink cluster with `LoadBalancer` REST service exposed type might make the cluster accessible publicly (usually with the ability to execute arbitrary code).
{{< /hint >}}

### Logging

The Kubernetes integration exposes `conf/log4j-console.properties` and `conf/logback-console.xml` as a ConfigMap to the pods.
Changes to these files will be visible to a newly started cluster.

#### Accessing the Logs

By default, the JobManager and TaskManager will output the logs to the console and `/opt/flink/log` in each pod simultaneously.
The `STDOUT` and `STDERR` output will only be redirected to the console.
You can access them via

```bash
$ kubectl logs <pod-name>
```

If the pod is running, you can also use `kubectl exec -it <pod-name> bash` to tunnel in and view the logs or debug the process.

#### Accessing the Logs of the TaskManagers

Flink will automatically de-allocate idling TaskManagers in order to not waste resources.
This behaviour can make it harder to access the logs of the respective pods.
You can increase the time before idling TaskManagers are released by configuring [resourcemanager.taskmanager-timeout]({{< ref "docs/deployment/config" >}}#resourcemanager-taskmanager-timeout) so that you have more time to inspect the log files.

#### Changing the Log Level Dynamically

If you have configured your logger to [detect configuration changes automatically]({{< ref "docs/deployment/advanced/logging" >}}), then you can dynamically adapt the log level by changing the respective ConfigMap (assuming that the cluster id is `my-first-flink-cluster`):

```bash
$ kubectl edit cm flink-config-my-first-flink-cluster
```

### Using Plugins

In order to use [plugins]({{< ref "docs/deployment/filesystems/plugins" >}}), you must copy them to the correct location in the Flink JobManager/TaskManager pod.
You can use the [built-in plugins]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#using-plugins) without mounting a volume or building a custom Docker image.
For example, use the following command to enable the S3 plugin for your Flink session cluster.

```bash
$ ./bin/kubernetes-session.sh
    -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{< version >}}.jar \
    -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{< version >}}.jar
```

### Custom Docker Image

If you want to use a custom Docker image, then you can specify it via the configuration option `kubernetes.container.image`.
The Flink community provides a rich [Flink Docker image]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}) which can be a good starting point.
See [how to customize Flink's Docker image]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#customize-flink-image) for how to enable plugins, add dependencies and other options.

### Using Secrets

[Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/) is an object that contains a small amount of sensitive data such as a password, a token, or a key.
Such information might otherwise be put in a pod specification or in an image.
Flink on Kubernetes can use Secrets in two ways:

* Using Secrets as files from a pod;

* Using Secrets as environment variables;

#### Using Secrets as Files From a Pod

The following command will mount the secret `mysecret` under the path `/path/to/secret` in the started pods:

```bash
$ ./bin/kubernetes-session.sh -Dkubernetes.secrets=mysecret:/path/to/secret
```

The username and password of the secret `mysecret` can then be found stored in the files `/path/to/secret/username` and `/path/to/secret/password`.
For more details see the [official Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod).

#### Using Secrets as Environment Variables

The following command will expose the secret `mysecret` as environment variable in the started pods:

```bash
$ ./bin/kubernetes-session.sh -Dkubernetes.env.secretKeyRef=\
    env:SECRET_USERNAME,secret:mysecret,key:username;\
    env:SECRET_PASSWORD,secret:mysecret,key:password
```

The env variable `SECRET_USERNAME` contains the username and the env variable `SECRET_PASSWORD` contains the password of the secret `mysecret`.
For more details see the [official Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables).

### High-Availability on Kubernetes

For high availability on Kubernetes, you can use the [existing high availability services]({{< ref "docs/deployment/ha/overview" >}}).

### Manual Resource Cleanup

Flink uses [Kubernetes OwnerReference's](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) to clean up all cluster components.
All the Flink created resources, including `ConfigMap`, `Service`, and `Pod`, have the `OwnerReference` being set to `deployment/<cluster-id>`.
When the deployment is deleted, all related resources will be deleted automatically.

```bash
$ kubectl delete deployment/<cluster-id>
```

### Supported Kubernetes Versions

Currently, all Kubernetes versions `>= 1.9` are supported.

### Namespaces

[Namespaces in Kubernetes](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) divide cluster resources between multiple users via [resource quotas](https://kubernetes.io/docs/concepts/policy/resource-quotas/).
Flink on Kubernetes can use namespaces to launch Flink clusters.
The namespace can be configured via [kubernetes.namespace]({{< ref "docs/deployment/config" >}}#kubernetes-namespace).

### RBAC

Role-based access control ([RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)) is a method of regulating access to compute or network resources based on the roles of individual users within an enterprise.
Users can configure RBAC roles and service accounts used by JobManager to access the Kubernetes API server within the Kubernetes cluster.

Every namespace has a default service account. However, the `default` service account may not have the permission to create or delete pods within the Kubernetes cluster.
Users may need to update the permission of the `default` service account or specify another service account that has the right role bound.

```bash
$ kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
```

If you do not want to use the `default` service account, use the following command to create a new `flink-service-account` service account and set the role binding.
Then use the config option `-Dkubernetes.service-account=flink-service-account` to make the JobManager pod use the `flink-service-account` service account to create/delete TaskManager pods and leader ConfigMaps. 
Also this will allow the TaskManager to watch leader ConfigMaps to retrieve the address of JobManager and ResourceManager.

```bash
$ kubectl create serviceaccount flink-service-account
$ kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=default:flink-service-account
```

Please refer to the official Kubernetes documentation on [RBAC Authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) for more information.

### Pod Template

Flink allows users to define the JobManager and TaskManager pods via template files. This allows to support advanced features
that are not supported by Flink [Kubernetes config options]({{< ref "docs/deployment/config" >}}#kubernetes) directly.
Use [`kubernetes.pod-template-file`]({{< ref "docs/deployment/config" >}}#kubernetes-pod-template-file)
to specify a local file that contains the pod definition. It will be used to initialize the JobManager and TaskManager.
The main container should be defined with name `flink-main-container`.
Please refer to the [pod template example](#example-of-pod-template) for more information.

#### Fields Overwritten by Flink

Some fields of the pod template will be overwritten by Flink.
The mechanism for resolving effective field values can be categorized as follows:
* **Defined by Flink:** User cannot configure it.
* **Defined by the user:** User can freely specify this value. Flink framework won't set any additional values and the effective value derives from the config option and the template.

  Precedence order: First an explicit config option value is taken, then the value in pod template and at last the default value of a config option if nothing is specified.
* **Merged with Flink:** Flink will merge values for a setting with a user defined value (see precedence order for "Defined by the user"). Flink values have precedence in case of same name fields.

Refer to the following tables for the full list of pod fields that will be overwritten.
All the fields defined in the pod template that are not listed in the tables will be unaffected.

**Pod Metadata**
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 10%">Key</th>
            <th class="text-left" style="width: 20%">Category</th>
            <th class="text-left" style="width: 30%">Related Config Options</th>
            <th class="text-left" style="width: 40%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>Defined by Flink</td>
            <td></td>
            <td>The JobManager pod name will be overwritten with the deployment which is defined by <a href="{{< ref "docs/deployment/config" >}}#kubernetes-cluster-id">kubernetes.cluster-id</a>.
                The TaskManager pod names will be overwritten with the pattern <code>&lt;clusterID&gt;-&lt;attempt&gt;-&lt;index&gt;</code> which is generated by Flink ResourceManager.</td>
        </tr>
        <tr>
            <td>namespace</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-namespace">kubernetes.namespace</a></td>
            <td>Both the JobManager deployment and TaskManager pods will be created in the user specified namespace.</td>
        </tr>
        <tr>
            <td>ownerReferences</td>
            <td>Defined by Flink</td>
            <td></td>
            <td>The owner reference of JobManager and TaskManager pods will always be set to the JobManager deployment.
                Please use <a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-owner-reference">kubernetes.jobmanager.owner.reference</a> to control when the deployment is deleted.</td>
        </tr>
        <tr>
            <td>annotations</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-annotations">kubernetes.jobmanager.annotations</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-annotations">kubernetes.taskmanager.annotations</a></td>
            <td>Flink will add additional annotations specified by the Flink configuration options.</td>
        </tr>
        <tr>
            <td>labels</td>
            <td>Merged with Flink</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-labels">kubernetes.jobmanager.labels</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-labels">kubernetes.taskmanager.labels</a></td>
            <td>Flink will add some internal labels to the user defined values.</td>
        </tr>
    </tbody>
</table>

**Pod Spec**
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 10%">Key</th>
            <th class="text-left" style="width: 20%">Category</th>
            <th class="text-left" style="width: 30%">Related Config Options</th>
            <th class="text-left" style="width: 40%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>imagePullSecrets</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-container-image-pull-secrets">kubernetes.container.image.pull-secrets</a></td>
            <td>Flink will add additional pull secrets specified by the Flink configuration options.</td>
        </tr>
        <tr>
            <td>nodeSelector</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-node-selector">kubernetes.jobmanager.node-selector</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-node-selector">kubernetes.taskmanager.node-selector</a></td>
            <td>Flink will add additional node selectors specified by the Flink configuration options.</td>
        </tr>
        <tr>
            <td>tolerations</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-tolerations">kubernetes.jobmanager.tolerations</a>
                <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-tolerations">kubernetes.taskmanager.tolerations</a></td>
            <td>Flink will add additional tolerations specified by the Flink configuration options.</td>
        </tr>
        <tr>
            <td>restartPolicy</td>
            <td>Defined by Flink</td>
            <td></td>
            <td>"always" for JobManager pod and "never" for TaskManager pod.
                <br>
                The JobManager pod will always be restarted by deployment. And the TaskManager pod should not be restarted.</td>
        </tr>
        <tr>
            <td>serviceAccount</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-service-account">kubernetes.service-account</a></td>
            <td>The JobManager and TaskManager pods will be created with the user defined service account.</td>
        </tr>
        <tr>
            <td>volumes</td>
            <td>Merged with Flink</td>
            <td></td>
            <td>Flink will add some internal ConfigMap volumes(e.g. flink-config-volume, hadoop-config-volume) which is necessary for shipping the Flink configuration and hadoop configuration.</td>
        </tr>
    </tbody>
</table>

**Main Container Spec**
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 10%">Key</th>
            <th class="text-left" style="width: 20%">Category</th>
            <th class="text-left" style="width: 30%">Related Config Options</th>
            <th class="text-left" style="width: 40%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>env</td>
            <td>Merged with Flink</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#forwarding-environment-variables">containerized.master.env.{ENV_NAME}</a>
                <a href="{{< ref "docs/deployment/config" >}}#forwarding-environment-variables">containerized.taskmanager.env.{ENV_NAME}</a></td>
            <td>Flink will add some internal environment variables to the user defined values.</td>
        </tr>
        <tr>
            <td>image</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-container-image">kubernetes.container.image</a></td>
            <td>The container image will be resolved with respect to the defined precedence order for user defined values.</td>
        </tr>
        <tr>
            <td>imagePullPolicy</td>
            <td>Defined by the user</td>
            <td><a href="{{< ref "docs/deployment/config" >}}#kubernetes-container-image-pull-policy">kubernetes.container.image.pull-policy</a></td>
            <td>The container image pull policy will be resolved with respect to the defined precedence order for user defined values.</td>
        </tr>
        <tr>
            <td>name</td>
            <td>Defined by Flink</td>
            <td></td>
            <td>The container name will be overwritten by Flink with "flink-main-container".</td>
        </tr>
        <tr>
            <td>resources</td>
            <td>Defined by the user</td>
            <td>Memory: <br>
                    <a href="{{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size">jobmanager.memory.process.size</a>
                    <a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size">taskmanager.memory.process.size</a>
                <br>
                CPU: <br>
                    <a href="{{< ref "docs/deployment/config" >}}#kubernetes-jobmanager-cpu">kubernetes.jobmanager.cpu</a>
                    <a href="{{< ref "docs/deployment/config" >}}#kubernetes-taskmanager-cpu">kubernetes.taskmanager.cpu</a></td>
            <td>The memory and cpu resources(including requests and limits) will be overwritten by Flink configuration options. All other resources(e.g. ephemeral-storage) will be retained.</td>
        </tr>
        <tr>
            <td>containerPorts</td>
            <td>Merged with Flink</td>
            <td></td>
            <td>Flink will add some internal container ports(e.g. rest, jobmanager-rpc, blob, taskmanager-rpc).</td>
        </tr>
        <tr>
            <td>volumeMounts</td>
            <td>Merged with Flink</td>
            <td></td>
            <td>Flink will add some internal volume mounts(e.g. flink-config-volume, hadoop-config-volume) which is necessary for shipping the Flink configuration and hadoop configuration.</td>
        </tr>
    </tbody>
</table>

#### Example of Pod Template
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
