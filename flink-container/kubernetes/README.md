# Apache Flink job cluster deployment on Kubernetes

## Build container image using Docker

In order to deploy a job cluster on Kubernetes, you first need to build a Docker image containing Flink and the user code jar.
Please follow the instructions you can find [here](../docker/README.md) to build a job container image.

## Deploy Flink job cluster

This directory contains a predefined K8s service and two template files for the job cluster entry point and the task managers.

The K8s service is used to let the cluster pods find each other.
If you start the Flink cluster in HA mode, then this is not necessary, because the HA implementation is used to detect leaders.

In order to use the template files, please replace the `${VARIABLES}` in the file with concrete values.
The files contain the following variables:

- `${FLINK_IMAGE_NAME}`: Name of the image to use for the container
- `${FLINK_JOB_PARALLELISM}`: Degree of parallelism with which to start the Flink job and the number of required task managers

One way to substitute the variables is to use `envsubst`.
See [here](https://stackoverflow.com/a/23622446/4815083) for a guide to install it on Mac OS X.

Alternatively, copy the template files (suffixed with `*.template`) and replace the variables.

In non HA mode, you should first start the job cluster service:

`kubectl create -f job-cluster-service.yaml`

In order to deploy the job cluster entrypoint run:

`FLINK_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB_PARALLELISM=<PARALLELISM> envsubst < job-cluster-job.yaml.template | kubectl create -f -`

Now you should see the `flink-job-cluster` job being started by calling `kubectl get job`.

At last, you should start the task manager deployment:

`FLINK_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB_PARALLELISM=<PARALLELISM> envsubst < task-manager-deployment.yaml.template | kubectl create -f -`

### Additional command line arguments

You can provide the following additional command line arguments to the cluster entrypoint:

- `--job-classname <job class name>`: Class name of the job to run. By default, the Flink class path is scanned for a JAR with a `Main-Class` or `program-class` manifest entry and chosen as the job class. Use this command line argument to manually set the job class. This argument is required in case that no or more than one JAR with such a manifest entry is available on the class path.
- `--job-id <job id>`: Manually set a Flink job ID for the job (default: `00000000000000000000000000000000`)

## Resuming from a savepoint

In order to resume from a savepoint, one needs to pass the savepoint path to the cluster entrypoint.
This can be achieved by adding `"--fromSavepoint", "<SAVEPOINT_PATH>"` to the `args` field in the [job-cluster-job.yaml.template](job-cluster-job.yaml.template).
Note that `<SAVEPOINT_PATH>` needs to be accessible from the `job-cluster-job` pod (e.g. adding it to the image or storing it on a DFS).
Additionally one can specify `"--allowNonRestoredState"` to allow that savepoint state is skipped which cannot be restored.

## Interact with Flink job cluster

After starting the job cluster service, the web UI will be available under `<NODE_IP>:30081`.
In the case of Minikube, `<NODE_IP>` equals `minikube ip`.
You can then use the Flink client to send Flink commands to the cluster:

`bin/flink list -m <NODE_IP:30081>`

## Terminate Flink job cluster

The job cluster entry point pod is part of the Kubernetes job and terminates once the Flink job reaches a globally terminal state.
Alternatively, you can also stop the job manually.

`kubectl delete job flink-job-cluster`

The task manager pods are part of the task manager deployment and need to be deleted manually by calling

`kubectl delete deployment flink-task-manager`

Last but not least you should also stop the job cluster service

`kubectl delete service flink-job-cluster`
