---
title: "Quickstart: Setup"
---

Get Stratosphere up and running in a few simple steps.

# Requirements
Stratosphere runs on all __UNIX-like__ environments: __Linux__, __Mac OS X__, __Cygwin__. The only requirement is to have a working __Java 6.x__ (or higher) installation.

# Download
Download the ready to run binary package. Choose the Stratosphere distribution that __matches your Hadoop version__. If you are unsure which version to choose or you just want to run locally, pick the package for Hadoop 1.2.

<ul class="nav nav-tabs">
   <li class="active"><a href="#bin-hadoop1" data-toggle="tab">Hadoop 1.2</a></li>
   <li><a href="#bin-hadoop2" data-toggle="tab">Hadoop 2 (YARN)</a></li>
 </ul>
 <div class="tab-content text-center">
   <div class="tab-pane active" id="bin-hadoop1">
     <a class="btn btn-info btn-lg" onclick="_gaq.push(['_trackEvent','Action','download-quickstart-setup-1',this.href]);" href="{{site.current_stable_dl}}"><i class="icon-download"> </i> Download Stratosphere for Hadoop 1.2</a>
   </div>
   <div class="tab-pane" id="bin-hadoop2">
     <a class="btn btn-info btn-lg" onclick="_gaq.push(['_trackEvent','Action','download-quickstart-setup-2',this.href]);" href="{{site.current_stable_dl_yarn}}"><i class="icon-download"> </i> Download Stratosphere for Hadoop 2 (YARN)</a>
   </div>
 </div>
</p>


# Start
You are almost done.
  
1. Go to the download directory.
2. Unpack the downloaded archive.
3. Start Stratosphere.


```bash
$ cd ~/Downloads              # Go to download directory
$ tar xzf stratosphere-*.tgz  # Unpack the downloaded archive
$ cd stratosphere
$ bin/start-local.sh          # Start Stratosphere
```

Check the __JobManager's web frontend__ at [http://localhost:8081](http://localhost:8081) and make sure everything is up and running.

# Run Example

Run the __Word Count example__ to see Stratosphere at work.

* __Download test data__:
```bash
$ wget -O hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt
```
* You now have a text file called _hamlet.txt_ in your working directory.
* __Start the example program__:
```bash
$ bin/stratosphere run \
    --jarfile ./examples/stratosphere-java-examples-{{site.current_stable}}-WordCount.jar \
    --arguments file://`pwd`/hamlet.txt file://`pwd`/wordcount-result.txt
```
* You will find a file called __wordcount-result.txt__ in your current directory.
  

# Cluster Setup
  
__Running Stratosphere on a cluster__ is as easy as running it locally. Having __passwordless SSH__ and __the same directory structure__ on all your cluster nodes lets you use our scripts to control everything.

1. Copy the unpacked __stratosphere__ directory from the downloaded archive to the same file system path on each node of your setup.
2. Choose a __master node__ (JobManager) and set the `jobmanager.rpc.address` key in `conf/stratosphere-conf.yaml` to its IP or hostname. Make sure that all nodes in your cluster have the same `jobmanager.rpc.address` configured.
3. Add the IPs or hostnames (one per line) of all __worker nodes__ (TaskManager) to the slaves files in `conf/slaves`.

You can now __start the cluster__ at your master node with `bin/start-cluster.sh`.


The following __example__ illustrates the setup with three nodes (with IP addresses from _10.0.0.1_ to _10.0.0.3_ and hostnames _master_, _worker1_, _worker2_) and shows the contents of the configuration files, which need to be accessible at the same path on all machines:

<div class="row">
  <div class="col-md-6 text-center">
    <img src="{{ site.baseurl }}/img/quickstart_cluster.png" style="width: 85%">
  </div>
<div class="col-md-6">
  <div class="row">
    <p class="lead text-center">
      /path/to/<strong>stratosphere/conf/<br>stratosphere-conf.yaml</strong>
    <pre>jobmanager.rpc.address: 10.0.0.1</pre>
    </p>
  </div>
<div class="row" style="margin-top: 1em;">
  <p class="lead text-center">
    /path/to/<strong>stratosphere/<br>conf/slaves</strong>
  <pre>
    10.0.0.2
    10.0.0.3
  </pre>
  </p>
</div>
</div>
</div>

# Stratosphere on YARN
You can easily deploy Stratosphere on your existing __YARN cluster__. 

1. Download the __Stratosphere YARN package__ with the YARN client: [Stratosphere for YARN]({{site.current_stable_uberjar}})
2. Make sure your __HADOOP_HOME__ (or _YARN_CONF_DIR_ or _HADOOP_CONF_DIR_) __environment variable__ is set to read your YARN and HDFS configuration.
3. Run the __YARN client__ with: `./bin/yarn-session.sh`. You can run the client with options `-n 10 -tm 8192` to allocate 10 TaskManagers with 8GB of memory each.

For __more detailed instructions__, check out the programming Guides and examples.