##Docker image for run flink natively on Kubernetes

###Run Flink natively on Alibaba cloud

1. create docker image

```bash
cd $FLINK_DIST

#build flink
mvn install -DskipTests

#go to flink docker image folder
cd flink-contrib/docker-k8s-native

#build docker image
./build.sh --from-local-dist --image-name flink-k8s

#upload to alicloud
#docker login
docker login --username=isunjin registry.cn-hangzhou.aliyuncs.com

#tag the flink image
docker tag flink-k8s registry.cn-hangzhou.aliyuncs.com/maxflink/flink:latest

#push
docker push registry.cn-hangzhou.aliyuncs.com/maxflink/flink

```

###Dummy Docker image
Build a official docker image is time consuming, it require a full build of flink distribution
and then build docker image. This make development process very inefficient.

Ideally, we want to edit the source and can debug instance without go thought the whole process.
To do this we cannot package flink distribution into docker image, instead, we can build a dummy container
and mount flink distribution as a volume.

we have add the script to build dummy docker image, the commandline is:

```bash
./build.sh --for-dev
```

this will create docker image flink-dev.

###Develop with minikube

while develop in minikube, we want the following experience:
1. minikube can access flink-dev image
```bash
eval $(minikube docker-env)
./build.sh --for-dev
```
note that we need to set 'imagePullPolicy' to "IfNotPresent" otherwise minikube will not pull image

2. mount dev machine volume to minikube
```bash
minikube mount ~/git/isunjin/flink_1/:/flink-root/
```

3. test yaml
```bash
kubectl create -f flink.yml
```

4. set class path for debug

```bash
ln -s ~/git/isunjin/flink/flink-kubernetes/target/classes/ ./build-target/lib/classes

```

to view, netstat -nr



debug in kubernetes

```bash
############################################
#test k8s-dev docker image
#in minikube
docker run -it -v  /flink-root/build-target:/opt/flink -v /flink-root:/flink-root -e EXTRA_CLASSPATHS=/flink-root/flink-kubernetes/target/classes flink-dev cluster
#run

#attach to JM container log of JM
kubectl log -f $(kubectl get pod -l app=flink | grep flink-session | awk '{print $1}')

#get JM container shell
kubectl exec -it $(kubectl get pod -l app=flink | grep flink-session | awk '{print $1}') -- /bin/bash


```
