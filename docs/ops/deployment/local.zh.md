---
title: "本地集群"
nav-title: 'Local Cluster'
nav-parent_id: deployment
nav-pos: 2
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

Get a local Flink cluster up and running in a few simple steps.

* This will be replaced by the TOC
{:toc}

## Setup: Download and Start Flink

Flink runs on __Linux and Mac OS X__.
<span class="label label-info">Note:</span> Windows users can run Flink in Cygwin or WSL.
To be able to run Flink, the only requirement is to have a working __Java 8 or 11__ installation.

You can check the correct installation of Java by issuing the following command:

{% highlight bash %}
java -version
{% endhighlight %}

If you have Java 8, the output will look something like this:

{% highlight bash %}
java version "1.8.0_111"
Java(TM) SE Runtime Environment (build 1.8.0_111-b14)
Java HotSpot(TM) 64-Bit Server VM (build 25.111-b14, mixed mode)
{% endhighlight %}

{% if site.is_stable %}
<div class="codetabs" markdown="1">
<div data-lang="Download and Unpack" markdown="1">
1. Download a binary from the [downloads page](https://flink.apache.org/downloads.html). You can pick
   any Scala variant you like. For certain features you may also have to download one of the pre-bundled Hadoop jars
   and place them into the `/lib` directory.
2. Go to the download directory.
3. Unpack the downloaded archive.

{% highlight bash %}
$ cd ~/Downloads        # Go to download directory
$ tar xzf flink-*.tgz   # Unpack the downloaded archive
$ cd flink-{{site.version}}
{% endhighlight %}
</div>

<div data-lang="MacOS X" markdown="1">
For MacOS X users, Flink can be installed through [Homebrew](https://brew.sh/).

{% highlight bash %}
$ brew install apache-flink
...
$ flink --version
Version: 1.2.0, Commit ID: 1c659cf
{% endhighlight %}
</div>

</div>

{% else %}
### Download and Compile
Clone the source code from one of our [repositories](https://flink.apache.org/community.html#source-code), e.g.:

{% highlight bash %}
$ git clone https://github.com/apache/flink.git
$ cd flink

# Building Flink will take up to 25 minutes
# You can speed up the build by skipping the
# web ui by passing the flag '-Pskip-webui-build'

$ mvn clean package -DskipTests -Pfast 

# This is where Flink is installed
$ cd build-target               
{% endhighlight %}
{% endif %}

### Start a Local Flink Cluster

{% highlight bash %}
$ ./bin/start-cluster.sh  # Start Flink
{% endhighlight %}

Check the __Dispatcher's web frontend__ at [http://localhost:8081](http://localhost:8081) and make sure everything is up and running. The web frontend should report a single available TaskManager instance.

<a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" alt="Dispatcher: Overview"/></a>

You can also verify that the system is running by checking the log files in the `logs` directory:

{% highlight bash %}
$ tail log/flink-*-standalonesession-*.log
INFO ... - Rest endpoint listening at localhost:8081
INFO ... - http://localhost:8081 was granted leadership ...
INFO ... - Web frontend listening at http://localhost:8081.
INFO ... - Starting RPC endpoint for StandaloneResourceManager at akka://flink/user/resourcemanager .
INFO ... - Starting RPC endpoint for StandaloneDispatcher at akka://flink/user/dispatcher .
INFO ... - ResourceManager akka.tcp://flink@localhost:6123/user/resourcemanager was granted leadership ...
INFO ... - Starting the SlotManager.
INFO ... - Dispatcher akka.tcp://flink@localhost:6123/user/dispatcher was granted leadership ...
INFO ... - Recovering all persisted jobs.
INFO ... - Registering TaskManager ... at ResourceManager
{% endhighlight %}

#### Windows Cygwin Users

If you are installing Flink from the git repository and you are using the Windows git shell, Cygwin can produce a failure similar to this one:

{% highlight bash %}
c:/flink/bin/start-cluster.sh: line 30: $'\r': command not found
{% endhighlight %}

This error occurs because git is automatically transforming UNIX line endings to Windows style line endings when running in Windows. The problem is that Cygwin can only deal with UNIX style line endings. The solution is to adjust the Cygwin settings to deal with the correct line endings by following these three steps:

1. Start a Cygwin shell.

2. Determine your home directory by entering

{% highlight bash %}
cd; pwd
{% endhighlight %}

    This will return a path under the Cygwin root path.

3. Using NotePad, WordPad or a different text editor open the file `.bash_profile` in the home directory and append the following: (If the file does not exist you will have to create it)

{% highlight bash %}
export SHELLOPTS
set -o igncr
{% endhighlight %}

Save the file and open a new bash shell.

### Stop a Local Flink Cluster

To **stop** Flink when you're done type:

<div class="codetabs" markdown="1">
<div data-lang="Bash" markdown="1">
{% highlight bash %}
$ ./bin/stop-cluster.sh
{% endhighlight %}
</div>
<div data-lang="Windows Shell" markdown="1">
You can terminate the processes via CTRL-C in the spawned shell windows.
</div>
</div>

{% top %}
