---
title: "Quickstart: Setup"
# Top navigation
top-nav-group: quickstart
top-nav-pos: 1
top-nav-title: Setup & Run Example
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

Get a Flink example program up and running in a few simple steps.

## Setup: Download and Start

Flink runs on __Linux, Mac OS X, and Windows__. To be able to run Flink, the only requirement is to have a working __Java 7.x__ (or higher) installation. Windows users, please take a look at the [Flink on Windows]({{ site.baseurl }}/setup/local_setup.html#flink-on-windows) guide which describes how to run Flink on Windows for local setups.

### Download

Download a binary from the [downloads page](http://flink.apache.org/downloads.html). You can pick any Hadoop/Scala combination you like, for instance [Flink for Hadoop 2]({{ site.FLINK_DOWNLOAD_URL_HADOOP2_STABLE }}).

### Start a Local Flink Cluster

1. Go to the download directory.
2. Unpack the downloaded archive.
3. Start Flink.

~~~bash
$ cd ~/Downloads        # Go to download directory
$ tar xzf flink-*.tgz   # Unpack the downloaded archive
$ cd flink-{{site.version}}
$ bin/start-local.sh    # Start Flink
~~~

Check the __JobManager's web frontend__ at [http://localhost:8081](http://localhost:8081) and make sure everything is up and running. The web frontend should report a single available TaskManager instance.

<a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" alt="JobManager: Overview"/></a>

## Run Example

Now, we are going to run the [SocketTextStreamWordCount example](https://github.com/apache/flink/blob/release-1.0.0/flink-quickstart/flink-quickstart-java/src/main/resources/archetype-resources/src/main/java/SocketTextStreamWordCount.java) and read text from a socket and count the number of distinct words.

* First of all, we use **netcat** to start local server via

  ~~~bash
  $ nc -l 9000
  ~~~ 

* Submit the Flink program:

  ~~~bash
  $ bin/flink run examples/streaming/SocketTextStreamWordCount.jar \
    --hostname localhost \
    --port 9000
  Printing result to stdout. Use --output to specify output path.
  03/08/2016 17:21:56 Job execution switched to status RUNNING.
  03/08/2016 17:21:56 Source: Socket Stream -> Flat Map(1/1) switched to SCHEDULED
  03/08/2016 17:21:56 Source: Socket Stream -> Flat Map(1/1) switched to DEPLOYING
  03/08/2016 17:21:56 Keyed Aggregation -> Sink: Unnamed(1/1) switched to SCHEDULED
  03/08/2016 17:21:56 Keyed Aggregation -> Sink: Unnamed(1/1) switched to DEPLOYING
  03/08/2016 17:21:56 Source: Socket Stream -> Flat Map(1/1) switched to RUNNING
  03/08/2016 17:21:56 Keyed Aggregation -> Sink: Unnamed(1/1) switched to RUNNING
  ~~~

  The program connects to the socket and waits for input. You can check the web interface to verify that the job is running as expected:

  <div class="row">
    <div class="col-sm-6">
      <a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-2.png" alt="JobManager: Overview (cont'd)"/></a>
    </div>
    <div class="col-sm-6">
      <a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-3.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-3.png" alt="JobManager: Running Jobs"/></a>
    </div>
  </div>

* Counts are printed to `stdout`. Monitor the JobManager's output file and write some text in `nc`:

  ~~~bash
  $ nc -l 9000
  lorem ipsum
  ipsum ipsum ipsum
  bye
  ~~~

  The `.out` file will print the counts immediately:

  ~~~bash
  $ tail -f log/flink-*-jobmanager-*.out
  (lorem,1)
  (ipsum,1)
  (ipsum,2)
  (ipsum,3)
  (ipsum,4)
  (bye,1)
  ~~~~

  To **stop** Flink when you're done type:

  ~~~bash
  $ bin/stop-local.sh
  ~~~

  <a href="{{ site.baseurl }}/page/img/quickstart-setup/setup.gif" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/setup.gif" alt="Quickstart: Setup"/></a>

## Next Steps

Check out the [step-by-step example](run_example_quickstart.html) in order to get a first feel of Flink's programming APIs. When you are done with that, go ahead and read the [streaming guide]({{ site.baseurl }}/apis/streaming/).

### Cluster Setup

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

### Flink on YARN

You can easily deploy Flink on your existing __YARN cluster__.

1. Download the __Flink Hadoop2 package__: [Flink with Hadoop 2]({{site.FLINK_DOWNLOAD_URL_HADOOP2_STABLE}})
2. Make sure your __HADOOP_HOME__ (or _YARN_CONF_DIR_ or _HADOOP_CONF_DIR_) __environment variable__ is set to read your YARN and HDFS configuration.
3. Run the __YARN client__ with: `./bin/yarn-session.sh`. You can run the client with options `-n 10 -tm 8192` to allocate 10 TaskManagers with 8GB of memory each.
