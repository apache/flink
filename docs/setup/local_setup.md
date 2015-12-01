---
title:  "Local Setup"
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

This documentation is intended to provide instructions on how to run Flink locally on a single machine.

* This will be replaced by the TOC
{:toc}

## Download

Go to the [downloads page]({{ site.download_url}}) and get the ready to run package. If you want to interact with Hadoop (e.g. HDFS or HBase), make sure to pick the Flink package **matching your Hadoop version**. When in doubt or you plan to just work with the local file system pick the package for Hadoop 1.2.x.

## Requirements

Flink runs on **Linux**, **Mac OS X** and **Windows**. The only requirement for a local setup is **Java 1.7.x** or higher. The following manual assumes a *UNIX-like environment*, for Windows see [Flink on Windows](#flink-on-windows).

You can check the correct installation of Java by issuing the following command:

~~~bash
java -version
~~~

The command should output something comparable to the following:

~~~bash
java version "1.6.0_22"
Java(TM) SE Runtime Environment (build 1.6.0_22-b04)
Java HotSpot(TM) 64-Bit Server VM (build 17.1-b03, mixed mode)
~~~

## Configuration

**For local mode Flink is ready to go out of the box and you don't need to change the default configuration.**

The out of the box configuration will use your default Java installation. You can manually set the environment variable `JAVA_HOME` or the configuration key `env.java.home` in `conf/flink-conf.yaml` if you want to manually override the Java runtime to use. Consult the [configuration page](config.html) for further details about configuring Flink.

## Starting Flink

**You are now ready to start Flink.** Unpack the downloaded archive and change to the newly created `flink` directory. There you can start Flink in local mode:

~~~bash
$ tar xzf flink-*.tgz
$ cd flink
$ bin/start-local.sh
Starting job manager
~~~

You can check that the system is running by checking the log files in the `logs` directory:

~~~bash
$ tail log/flink-*-jobmanager-*.log
INFO ... - Initializing memory manager with 409 megabytes of memory
INFO ... - Trying to load org.apache.flinknephele.jobmanager.scheduler.local.LocalScheduler as scheduler
INFO ... - Setting up web info server, using web-root directory ...
INFO ... - Web info server will display information about nephele job-manager on localhost, port 8081.
INFO ... - Starting web info server for JobManager on port 8081
~~~

The JobManager will also start a web frontend on port 8081, which you can check with your browser at `http://localhost:8081`.

## Flink on Windows

If you want to run Flink on Windows you need to download, unpack and configure the Flink archive as mentioned above. After that you can either use the **Windows Batch** file (`.bat`) or use **Cygwin**  to run the Flink Jobmanager.

### Starting with Windows Batch Files

To start Flink in local mode from the *Windows Batch*, open the command window, navigate to the `bin/` directory of Flink and run `start-local.bat`.

Note: The ``bin`` folder of your Java Runtime Environment must be included in Window's ``%PATH%`` variable. Follow this [guide](http://www.java.com/en/download/help/path.xml) to add Java to the ``%PATH%`` variable.

~~~bash
$ cd flink
$ cd bin
$ start-local.bat
Starting Flink job manager. Webinterface by default on http://localhost:8081/.
Do not close this batch window. Stop job manager by pressing Ctrl+C.
~~~

After that, you need to open a second terminal to run jobs using `flink.bat`.

### Starting with Cygwin and Unix Scripts

With *Cygwin* you need to start the Cygwin Terminal, navigate to your Flink directory and run the `start-local.sh` script:

~~~bash
$ cd flink
$ bin/start-local.sh
Starting Nephele job manager
~~~

### Installing Flink from Git

If you are installing Flink from the git repository and you are using the Windows git shell, Cygwin can produce a failure similiar to this one:

~~~bash
c:/flink/bin/start-local.sh: line 30: $'\r': command not found
~~~

This error occurs, because git is automatically transforming UNIX line endings to Windows style line endings when running in Windows. The problem is, that Cygwin can only deal with UNIX style line endings. The solution is to adjust the Cygwin settings to deal with the correct line endings by following these three steps:

1. Start a Cygwin shell.

2. Determine your home directory by entering

~~~bash
cd; pwd
~~~

It will return a path under the Cygwin root path.

2.  Using NotePad, WordPad or a different text editor open the file `.bash_profile` in the home directory and append the following: (If the file does not exist you have to create it)

~~~bash
export SHELLOPTS
set -o igncr
~~~

Save the file and open a new bash shell.

