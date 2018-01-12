---
title:  "Running Flink on Windows"
nav-parent_id: start
nav-pos: 12
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

If you want to run Flink locally on a Windows machine you need to [download](http://flink.apache.org/downloads.html) and unpack the binary Flink distribution. After that you can either use the **Windows Batch** file (`.bat`), or use **Cygwin** to run the Flink Jobmanager.

## Starting with Windows Batch Files

To start Flink in local mode from the *Windows Batch*, open the command window, navigate to the `bin/` directory of Flink and run `start-local.bat`.

Note: The ``bin`` folder of your Java Runtime Environment must be included in Window's ``%PATH%`` variable. Follow this [guide](http://www.java.com/en/download/help/path.xml) to add Java to the ``%PATH%`` variable.

~~~bash
$ cd flink
$ cd bin
$ start-local.bat
Starting Flink job manager. Web interface by default on http://localhost:8081/.
Do not close this batch window. Stop job manager by pressing Ctrl+C.
~~~

After that, you need to open a second terminal to run jobs using `flink.bat`.

{% top %}

## Starting with Cygwin and Unix Scripts

With *Cygwin* you need to start the Cygwin Terminal, navigate to your Flink directory and run the `start-local.sh` script:

~~~bash
$ cd flink
$ bin/start-local.sh
Starting jobmanager.
~~~

{% top %}

## Installing Flink from Git

If you are installing Flink from the git repository and you are using the Windows git shell, Cygwin can produce a failure similar to this one:

~~~bash
c:/flink/bin/start-local.sh: line 30: $'\r': command not found
~~~

This error occurs because git is automatically transforming UNIX line endings to Windows style line endings when running in Windows. The problem is that Cygwin can only deal with UNIX style line endings. The solution is to adjust the Cygwin settings to deal with the correct line endings by following these three steps:

1. Start a Cygwin shell.

2. Determine your home directory by entering

    ~~~bash
    cd; pwd
    ~~~

    This will return a path under the Cygwin root path.

3. Using NotePad, WordPad or a different text editor open the file `.bash_profile` in the home directory and append the following: (If the file does not exist you will have to create it)

~~~bash
export SHELLOPTS
set -o igncr
~~~

Save the file and open a new bash shell.

{% top %}
