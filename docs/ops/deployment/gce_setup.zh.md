---
title:  "Google Compute Engine 安装"
nav-title: Google Compute Engine
nav-parent_id: deployment
nav-pos: 6
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


This documentation provides instructions on how to setup Flink fully automatically with Hadoop 1 or Hadoop 2 on top of a [Google Compute Engine](https://cloud.google.com/compute/) cluster. This is made possible by Google's [bdutil](https://cloud.google.com/hadoop/bdutil) which starts a cluster and deploys Flink with Hadoop. To get started, just follow the steps below.

* This will be replaced by the TOC
{:toc}

# Prerequisites

## Install Google Cloud SDK

Please follow the instructions on how to setup the [Google Cloud SDK](https://cloud.google.com/sdk/). In particular, make sure to authenticate with Google Cloud using the following command:

    gcloud auth login

## Install bdutil

At the moment, there is no bdutil release yet which includes the Flink
extension. However, you can get the latest version of bdutil with Flink support
from [GitHub](https://github.com/GoogleCloudPlatform/bdutil):

    git clone https://github.com/GoogleCloudPlatform/bdutil.git

After you have downloaded the source, change into the newly created `bdutil` directory and continue with the next steps.

# Deploying Flink on Google Compute Engine

## Set up a bucket

If you have not done so, create a bucket for the bdutil config and staging files. A new bucket can be created with gsutil:

    gsutil mb gs://<bucket_name>

## Adapt the bdutil config

To deploy Flink with bdutil, adapt at least the following variables in
bdutil_env.sh.

    CONFIGBUCKET="<bucket_name>"
    PROJECT="<compute_engine_project_name>"
    NUM_WORKERS=<number_of_workers>

    # set this to 'n1-standard-2' if you're using the free trial
    GCE_MACHINE_TYPE="<gce_machine_type>"

    # for example: "europe-west1-d"
    GCE_ZONE="<gce_zone>"

## Adapt the Flink config

bdutil's Flink extension handles the configuration for you. You may additionally adjust configuration variables in `extensions/flink/flink_env.sh`. If you want to make further configuration, please take a look at [configuring Flink](../config.html). You will have to restart Flink after changing its configuration using `bin/stop-cluster` and `bin/start-cluster`.

## Bring up a cluster with Flink

To bring up the Flink cluster on Google Compute Engine, execute:

    ./bdutil -e extensions/flink/flink_env.sh deploy

## Run a Flink example job:

    ./bdutil shell
    cd /home/hadoop/flink-install/bin
    ./flink run ../examples/batch/WordCount.jar gs://dataflow-samples/shakespeare/othello.txt gs://<bucket_name>/output

## Shut down your cluster

Shutting down a cluster is as simple as executing

    ./bdutil -e extensions/flink/flink_env.sh delete

{% top %}
