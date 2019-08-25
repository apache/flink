---
title: "Amazon Web Services (AWS)"
nav-title: AWS
nav-parent_id: deployment
nav-pos: 5
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

Amazon Web Services offers cloud computing services on which you can run Flink.

* ToC
{:toc}

## EMR: Elastic MapReduce

[Amazon Elastic MapReduce](https://aws.amazon.com/elasticmapreduce/) (Amazon EMR) is a web service that makes it easy to  quickly setup a Hadoop cluster. This is the **recommended way** to run Flink on AWS as it takes care of setting up everything.

### Standard EMR Installation

Flink is a supported application on Amazon EMR. [Amazon's documentation](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-flink.html)
describes configuring Flink, creating and monitoring a cluster, and working with jobs.

### Custom EMR Installation

Amazon EMR services are regularly updated to new releases but a version of Flink which is not available
can be manually installed in a stock EMR cluster.

**Create EMR Cluster**

The EMR documentation contains [examples showing how to start an EMR cluster](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-gs-launch-sample-cluster.html). You can follow that guide and install any EMR release. You don't need to install the *All Applications* part of the EMR release, but can stick to *Core Hadoop*.

{% warn Note %}
Access to S3 buckets requires
[configuration of IAM roles](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-iam-roles.html)
when creating an EMR cluster.

**Install Flink on EMR Cluster**

After creating your cluster, you can [connect to the master node](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-connect-master-node.html) and install Flink:

1. Go the [Downloads Page]({{ site.download_url }}) and **download a binary version of Flink matching the Hadoop version** of your EMR cluster, e.g. Hadoop 2.7 for EMR releases 4.3.0, 4.4.0, or 4.5.0.
2. Make sure all the Hadoop dependencies are in the classpath before you submit any jobs to EMR:

{% highlight bash %}
export HADOOP_CLASSPATH=`hadoop classpath`
{% endhighlight %}

3. Extract the Flink distribution and you are ready to deploy [Flink jobs via YARN](yarn_setup.html) after **setting the Hadoop config directory**:

{% highlight bash %}
HADOOP_CONF_DIR=/etc/hadoop/conf ./bin/flink run -m yarn-cluster -yn 1 examples/streaming/WordCount.jar
{% endhighlight %}

{% top %}
