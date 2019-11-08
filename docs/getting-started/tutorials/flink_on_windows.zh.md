---
title:  "在 Windows 上运行 Flink"
nav-parent_id: setuptutorials
nav-pos: 30
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

If you want to run Flink locally on a Windows machine you need to [download](https://flink.apache.org/downloads.html) and unpack the binary Flink distribution. After that you can either use the **Windows Batch** file (`.bat`), or use **Cygwin** to run the Flink Jobmanager.

## Starting with Windows Batch Files

To start Flink in from the *Windows Command Line*, open the command window, navigate to the `bin/` directory of Flink and run `start-cluster.bat`.

Note: The ``bin`` folder of your Java Runtime Environment must be included in Window's ``%PATH%`` variable. Follow this [guide](http://www.java.com/en/download/help/path.xml) to add Java to the ``%PATH%`` variable.

{% highlight bash %}
$ cd flink
$ cd bin
$ start-cluster.bat
Starting a local cluster with one JobManager process and one TaskManager process.
You can terminate the processes via CTRL-C in the spawned shell windows.
Web interface by default on http://localhost:8081/.
{% endhighlight %}

After that, you need to open a second terminal to run jobs using `flink.bat`.

{% top %}

## Starting with Cygwin and Unix Scripts

With *Cygwin* you need to start the Cygwin Terminal, navigate to your Flink directory and run the `start-cluster.sh` script:

{% highlight bash %}
$ cd flink
$ bin/start-cluster.sh
Starting cluster.
{% endhighlight %}

{% top %}

## Installing Flink from Git

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

{% top %}
