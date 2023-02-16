#!/bin/bash

set -eux

ECR_HOST="460511261886.dkr.ecr.us-east-1.amazonaws.com"
ECR_REPOSITORY="prod-flink"
ECR_REGION='us-east-1'

PLATFORM=`uname -m`

if [ "$PLATFORM" == "x86_64" ]; then
    BUILDARCH='linux-x64'
elif [ "$PLATFORM" == "arm64" ]; then
    BUILDARCH='linux-arm64'
fi

ECR_REPO_FQN="$ECR_HOST/$ECR_REPOSITORY"
GIT_SHA=$(git rev-parse HEAD)

tar zcfh flink.tgz -C .. build-target

aws ecr get-login-password --region $ECR_REGION | docker login --username AWS --password-stdin $ECR_HOST
docker build -t "$ECR_REPO_FQN:$PLATFORM-$GIT_SHA" -t "$ECR_REPO_FQN:$PLATFORM-latest" --build-arg "BUILDARCH=$BUILDARCH" .
docker push -a "$ECR_REPO_FQN"
rm flink.tgz
