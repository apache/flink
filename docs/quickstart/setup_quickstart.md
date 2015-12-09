---
title: "Quickstart: Setup"
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

* This will be replaced by the TOC
{:toc}

Get Flink up and running in a few simple steps.

## Requirements

Flink runs on __Linux, Mac OS X, and Windows__. To be able to run Flink, the
only requirement is to have a working __Java 7.x__ (or higher)
installation. Windows users, please take a look at the
[Flink on Windows]({{ site.baseurl }}/setup/local_setup.html#flink-on-windows) guide which describes
how to run Flink on Windows for local setups.

## Download
Download the ready to run binary package. Choose the Flink distribution that __matches your Hadoop version__. If you are unsure which version to choose or you just want to run locally, pick the package for Hadoop 1.2.

<ul class="nav nav-tabs">
  <li class="active"><a href="#bin-hadoop1" data-toggle="tab">Hadoop 1.2</a></li>
  <li><a href="#bin-hadoop2" data-toggle="tab">Hadoop 2 (YARN)</a></li>
</ul>
<p>
<div class="tab-content text-center">
  <div class="tab-pane active" id="bin-hadoop1">
    <a class="btn btn-info btn-lg" onclick="_gaq.push(['_trackEvent','Action','download-quickstart-setup-1',this.href]);" href="{{site.FLINK_DOWNLOAD_URL_HADOOP1_STABLE}}"><i class="icon-download"> </i> Download Flink for Hadoop 1.2</a>
  </div>
  <div class="tab-pane" id="bin-hadoop2">
    <a class="btn btn-info btn-lg" onclick="_gaq.push(['_trackEvent','Action','download-quickstart-setup-2',this.href]);" href="{{site.FLINK_DOWNLOAD_URL_HADOOP2_STABLE}}"><i class="icon-download"> </i> Download Flink for Hadoop 2</a>
  </div>
</div>
</p>


## Start

1. Go to the download directory.
2. Unpack the downloaded archive.
3. Start Flink.


~~~bash
$ cd ~/Downloads        # Go to download directory
$ tar xzf flink-*.tgz   # Unpack the downloaded archive
$ cd flink-{{site.version}}
$ bin/start-local.sh    # Start Flink
~~~

Check the __JobManager's web frontend__ at [http://localhost:8081](http://localhost:8081) and make
sure everything is up and running.

## Run Example

Run the __Word Count example__ to see Flink at work.

* __Download test data__:

  ~~~bash
  $ wget -O hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt
  ~~~

* You now have a text file called _hamlet.txt_ in your working directory.
* __Start the example program__:

  ~~~bash
  $ bin/flink run ./examples/WordCount.jar file://`pwd`/hamlet.txt file://`pwd`/wordcount-result.txt
  ~~~

* You will find a file called __wordcount-result.txt__ in your current directory.


## Cluster Setup

__Running Flink on a cluster__ is as easy as running it locally. Having __passwordless SSH__ and
__the same directory structure__ on all your cluster nodes lets you use our scripts to control
everything.

1. Copy the unpacked __flink__ directory from the downloaded archive to the same file system path
on each node of your setup.
2. Choose a __master node__ (JobManager) and set the `jobmanager.rpc.address` key in
`conf/flink-conf.yaml` to its IP or hostname. Make sure that all nodes in your cluster have the same
`jobmanager.rpc.address` configured.
3. Add the IPs or hostnames (one per line) of all __worker nodes__ (TaskManager) to the slaves files
in `conf/slaves`.

You can now __start the cluster__ at your master node with `bin/start-cluster.sh`.

The following __example__ illustrates the setup with three nodes (with IP addresses from _10.0.0.1_
to _10.0.0.3_ and hostnames _master_, _worker1_, _worker2_) and shows the contents of the
configuration files, which need to be accessible at the same path on all machines:

<div class="row">
  <div class="col-md-6 text-center">
    <img src="{{ site.baseurl }}/page/img/quickstart_cluster.png" style="width: 85%">
  </div>
<div class="col-md-6">
  <div class="row">
    <p class="lead text-center">
      /path/to/<strong>flink/conf/<br>flink-conf.yaml</strong>
    <pre>jobmanager.rpc.address: 10.0.0.1</pre>
    </p>
  </div>
<div class="row" style="margin-top: 1em;">
  <p class="lead text-center">
    /path/to/<strong>flink/<br>conf/slaves</strong>
  <pre>
10.0.0.2
10.0.0.3</pre>
  </p>
</div>
</div>
</div>

Have a look at the [Configuration]({{ site.baseurl }}/setup/config.html) section of the documentation to see other available configuration options.
For Flink to run efficiently, a few configuration values need to be set.

In particular,

 * the amount of available memory per TaskManager (`taskmanager.heap.mb`),
 * the number of available CPUs per machine (`taskmanager.numberOfTaskSlots`),
 * the total number of CPUs in the cluster (`parallelism.default`) and
 * the temporary directories (`taskmanager.tmp.dirs`)


are very important configuration values.

## Flink on YARN
You can easily deploy Flink on your existing __YARN cluster__.

1. Download the __Flink Hadoop2 package__: [Flink with Hadoop 2]({{site.FLINK_DOWNLOAD_URL_HADOOP2_STABLE}})
2. Make sure your __HADOOP_HOME__ (or _YARN_CONF_DIR_ or _HADOOP_CONF_DIR_) __environment variable__ is set to read your YARN and HDFS configuration.
3. Run the __YARN client__ with: `./bin/yarn-session.sh`. You can run the client with options `-n 10 -tm 8192` to allocate 10 TaskManagers with 8GB of memory each.

For __more detailed instructions__, check out the programming Guides and examples.