---
title: "Quickstart: Setup"
---

<p class="lead">Get Stratosphere up and running in a few simple steps.</p>

<section id="requirements">
  <div class="page-header"><h2>Requirements</h2></div>
  <p class="lead">Stratosphere runs on all <em>UNIX-like</em> environments: <strong>Linux</strong>, <strong>Mac OS X</strong>, <strong>Cygwin</strong>. The only requirement is to have a working <strong>Java 6.x</strong> (or higher) installation.</p>
</section>

<section id="download">
  <div class="page-header"><h2>Download</h2></div>
  <p class="lead">Download the ready to run binary package. Choose the Stratosphere distribution that <strong>matches your Hadoop version</strong>. If you are unsure which version to choose or you just want to run locally, pick the package for Hadoop 1.2.</p>
  <p>
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
</section>

<section id="start">
  <div class="page-header"><h2>Start</h2></div> 
  <p class="lead">You are almost done.</p>
  <ol>
  	<li class="lead"><strong>Go to the download directory</strong>,</li>
  	<li class="lead"><strong>Unpack the downloaded archive</strong>, and</li>
  	<li class="lead"><strong>Start Stratosphere</strong>.</li>
  </ol>

{% highlight bash %}
$ cd ~/Downloads              # Go to download directory
$ tar xzf stratosphere-*.tgz  # Unpack the downloaded archive
$ cd stratosphere
$ bin/start-local.sh          # Start Stratosphere
{% endhighlight %}

  <p class="lead">Check the <strong>JobManager's web frontend</strong> at <a href="http://localhost:8081">http://localhost:8081</a> and make sure everything is up and running.</p>
</section>

<section id="example">
  <div class="page-header"><h2>Run Example</h2></div>
  <p class="lead">Run the <strong>Word Count example</strong> to see Stratosphere at work.</p>

  <ol>
  	<li class="lead"><strong>Download test data:</strong>
{% highlight bash %}
$ wget -O hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt
{% endhighlight %}
		  You now have a text file called <em>hamlet.txt</em> in your working directory.
		</li>
  	<li class="lead"><strong>Start the example program</strong>:
{% highlight bash %}
$ bin/stratosphere run \
    --jarfile ./examples/stratosphere-java-examples-{{site.current_stable}}-WordCount.jar \
    --arguments file://`pwd`/hamlet.txt file://`pwd`/wordcount-result.txt
{% endhighlight %}
      You will find a file called <strong>wordcount-result.txt</strong> in your current directory.
  	</li>
  </ol>

</section>

<section id="cluster">
  <div class="page-header"><h2>Cluster Setup</h2></div>
  <p class="lead"><strong>Running Stratosphere on a cluster</strong> is as easy as running it locally. Having <strong>passwordless SSH</strong> and <strong>the same directory structure</strong> on all your cluster nodes lets you use our scripts to control everything.</p>
  <ol>
  	<li class="lead">Copy the unpacked <strong>stratosphere</strong> directory from the downloaded archive to the same file system path on each node of your setup.</li>
  	<li class="lead">Choose a <strong>master node</strong> (JobManager) and set the <code>jobmanager.rpc.address</code> key in <code>conf/stratosphere-conf.yaml</code> to its IP or hostname. Make sure that all nodes in your cluster have the same <code>jobmanager.rpc.address</code> configured.</li>
  	<li class="lead">Add the IPs or hostnames (one per line) of all <strong>worker nodes</strong> (TaskManager) to the slaves files in <code>conf/slaves</code>.</li>
  </ol>
  <p class="lead">You can now <strong>start the cluster</strong> at your master node with <code>bin/start-cluster.sh</code>.</p>
  <p class="lead">
    The following <strong>example</strong> illustrates the setup with three nodes (with IP addresses from <em>10.0.0.1</em> to <em>10.0.0.3</em> and hostnames <em>master</em>, <em>worker1</em>, <em>worker2</em>) and shows the contents of the configuration files, which need to be accessible at the same path on all machines:
  </p>
  <div class="row">
    <div class="col-md-6 text-center">
      <img src="{{ site.baseurl }}/img/quickstart_cluster.png" style="width: 85%">
    </div>
    <div class="col-md-6">
      <div class="row">
        <p class="lead text-center">
        /path/to/<strong>stratosphere/conf/<br>stratosphere-conf.yaml</strong>
<pre>
jobmanager.rpc.address: 10.0.0.1
</pre>
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
</section>

<section id="yarn">
  <div class="page-header"><h2>Stratosphere on YARN</h2></div>
  <p class="lead">You can easily deploy Stratosphere on your existing <strong>YARN cluster</strong>. 
    <ol>
    <li class="lead">Download the <strong>Stratosphere YARN package</strong> with the YARN client:
      <div class="text-center" style="padding: 1em;">
      <a style="padding-left:10px" onclick="_gaq.push(['_trackEvent','Action','download-quickstart-yarn',this.href]);" class="btn btn-info btn-lg" href="{{site.current_stable_uberjar}}"><i class="icon-download"> </i> Stratosphere {{ site.current_stable }} for YARN</a>
      </div>
    </li>
    <li class="lead">Make sure your <strong>HADOOP_HOME</strong> (or <em>YARN_CONF_DIR</em> or <em>HADOOP_CONF_DIR</em>) <strong>environment variable</strong> is set to read your YARN and HDFS configuration.</li>
    <li class="lead">Run the <strong>YARN client</strong> with:
      <div class="text-center" style="padding:1em;">
        <code>./bin/yarn-session.sh</code>
      </div>
      
      You can run the client with options <code>-n 10 -tm 8192</code> to allocate 10 TaskManagers with 8GB of memory each.</li>
  </ol>
  </p>
</section>

<hr />
<p class="lead">For <strong>more detailed instructions</strong>, check out the <a href="{{site.baseurl}}/docs/{{site.current_stable_documentation}}">Documentation</a>.</p>