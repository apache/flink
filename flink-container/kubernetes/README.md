# Apache Flink Job Cluster Deployment on Kubernetes

## Job Cluster Container Images

In order to deploy a Job Cluster on Kubernetes, you first need to build a Docker image containing
Flink and the user code jar. Please follow the instructions you can find [here](../docker/README.md)
to build a job container image.

## Flink Job Cluster Deployment on Kubernetes

This directory contains three Kubernetes resource definitions:

* Kubernetes Job for the Job Cluster entrypoint
* Kubernetes Deployment for the Task Managers
* Kubernetes Service backed by the Job Cluster Pod

**Kubernetes Job for the Job Cluster Entrypoint**

The Kubernetes Job for the Job Cluster entrypoint will start & restart the Job Cluster until the Job
has reached a terminal state (e.g. CANCELLED).

In case of a high-availability setup (needs to be configured in the Flink configuration) the
previous job will be recovered from the latest checkpoint. For recovery the JobID of the Flink Job
needs to be stable over the lifetime of the the Kubernetes Job. For this, the JobID is seeded with
the name of the application (`${FLINK_APPLICATION_NAME}`).

Without high-availability configuration, the JobID needs to change upon restarts of the Job
Cluster Pod. For this, the JobID should not be seeded and the respective argument ought to be
removed from the given Pod specification of the Job.

**Kubernetes Deployment for the Task Managers**

The Task Manager Deployment is a simple Kubernetes Deployment. The number of replicas needs to be
configured by replacing `${FLINK_JOB_PARALLELISM}` (by default each Task Manager provides one Task
Slot). The Task Managers are pointed to the Kubernetes Services (see below), which is not necessary
in case of a high-availability setup. In this case the Task Managers query Zookeeper for the address
of the current Job Manager.

**Kubernetes Service backed by the Job Cluster Pod**

This service is used to let the Task Manager Pods find the Job Cluster Pod. If you start the Flink
cluster in HA mode, then this is not necessary, because the HA implementation is used to detect
leaders.

## Templating & Applying the K8s Resource Definitions

In order to use the template files, please replace the `${VARIABLES}` in the file with concrete
values. The files contain the following variables:

- `${FLINK_APPLICATION_NAME}`: Name of the Application (only needed for high-availability setups)
- `${FLINK_IMAGE_NAME}`: Name of the image to use for the container
- `${FLINK_JOB_PARALLELISM}`: Degree of parallelism with which to start the Flink job and the number of required task managers
- `${FLINK_JOB_ARGUMENTS}`: Additional arguments to pass to the Flink Job

One way to substitute the variables is to use `envsubst`.
See [here](https://stackoverflow.com/a/23622446/4815083) for a guide to install it on Mac OS X.

Alternatively, copy the template files (suffixed with `*.template`) and replace the variables.

In a non high-availability setup, you should first start the Job Cluster Service:

`FLINK_APPLICATION_NAME=<APPLICATION_NAME> envsubst < job-cluster-service.yaml.template | kubectl create -f -`

In order to deploy the Job Cluster entrypoint run:

`FLINK_APPLICATION_NAME=<APPLICATION_NAME> FLINK_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB_PARALLELISM=<PARALLELISM> FLINK_JOB_ARGUMENTS=<ARGUMENTS> envsubst < job-cluster-job.yaml.template | kubectl create -f -`

With high-availability configured you can directly start the Job Cluster Job:

`FLINK_APPLICATION_NAME=<APPLICATION_NAME> FLINK_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB_PARALLELISM=<PARALLELISM> FLINK_JOB_ARGUMENTS=<ARGUMENTS> envsubst < job-cluster-job.yaml.template | kubectl create -f -`

Now you should see the `${FLINK_APPLICATION_NAME}-master` job being started by calling `kubectl get job`.

At last, you should start the task manager deployment:

`FLINK_APPLICATION_NAME=<APPLICATION_NAME> FLINK_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB_PARALLELISM=<PARALLELISM> envsubst < task-manager-deployment.yaml.template | kubectl create -f -`

### Additional command line arguments

You can provide the following additional command line arguments to the cluster entrypoint:

- `--job-classname <job class name>`: Class name of the job to run. By default, the Flink class path
is scanned for a JAR with a `Main-Class` or `program-class` manifest entry and chosen as the job
class. Use this command line argument to manually set the job class. This argument is required in
case that no or more than one JAR with such a manifest entry is available on the class path.
- `--job-id <job id>`: Manually set a Flink job ID for the job (only recommended in
high-availability mode). `--job-id-` and `--job-id-seed` are mutually exclusive. If you would like
to specify the JobID manually, you need to replace the `--job-id-seed` parameter.
- `--job-id-seed <job id seed>`: Alternative way to manually set a Flink job ID by providing a seed
to generate it from (only recommended in high-availability mode).

## Resuming from a savepoint

In order to resume from a savepoint, one needs to pass the savepoint path to the cluster entrypoint.
This can be achieved by adding `"--fromSavepoint", "<SAVEPOINT_PATH>"` to the `args` field in the
[job-cluster-job.yaml.template](job-cluster-job.yaml.template).

Note that `<SAVEPOINT_PATH>` needs to be accessible from the `${FLINK_APPLICATION_NAME}-master` pod (e.g. adding it
to the image or storing it on a DFS). Additionally one can specify `"--allowNonRestoredState"` to
allow that savepoint state is skipped which cannot be restored.

## Interact with Flink Job Cluster

After starting the Job Cluster service, the web UI will be available under `<NODE_IP>:30081`.
In the case of Minikube, `<NODE_IP>` equals `minikube ip`.
You can then use the Flink client to send Flink commands to the cluster:

`bin/flink list -m <NODE_IP:30081>`

## Terminate Flink Job Cluster

The Job Cluster entry point pod is part of the Kubernetes job and terminates once the Flink job
reaches a globally terminal state. Alternatively, you can also stop the job manually.

`kubectl delete job ${FLINK_APPLICATION_NAME}-master`

The task manager pods are part of the task manager deployment and need to be deleted manually by
calling

`kubectl delete deployment ${FLINK_APPLICATION_NAME}-task-manager`

Last but not least you should also stop the Job Cluster service

`kubectl delete service ${FLINK_APPLICATION_NAME}-master`
