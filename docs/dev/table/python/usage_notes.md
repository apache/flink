---
title: "Usage Notes"
nav-parent_id: python_tableapi
nav-pos: 140
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

This page shows solutions to common problems encountered by PyFlink users after getting started.

* This will be replaced by the TOC
{:toc}
## How to Add Jars
A PyFlink job often depends on some jar packages, such as connector jar packages, java udf jar packages and so on.
So how to make PyFlink refer to these jar packages? 

You can specify the dependencies with the following Python Table APIs or through command line arguments directly when submitting the job。
{% highlight python %}
table_env.get_config().set_configuration("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
{% endhighlight %}
For details, you can refer to <a href="{{ site.baseurl }}/dev/table/python/dependency_management.html#usage">dependency management

## How to Watch UDF Log
There are different solutions for watching UDF log in local mode and cluster mode.
#### Local
you will see all udf log infos in the console directly.
#### Cluster
you can see the udf log infos in the taskmanager log.

## How to Prepare a Python Virtual Env Used by PyFlink
You can refer to the following script to prepare a Python virtual env zip which can be used in mac os and most Linux distributions
{% highlight shell %}
# you need to install wget and zip manually
echo "required command: wget, zip"

# download miniconda.sh 
set -e
if [ `uname -s` == "Darwin" ]; then
    # If you live in China, you can change the url to https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-4.7.10-MacOSX-x86_64.sh
    wget "https://repo.continuum.io/miniconda/Miniconda3-4.7.10-MacOSX-x86_64.sh" -O "miniconda.sh"
else
    # If you live in China, you can change the url to https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-4.7.10-Linux-x86_64.sh
    wget "https://repo.continuum.io/miniconda/Miniconda3-4.7.10-Linux-x86_64.sh" -O "miniconda.sh"
fi

chmod +x miniconda.sh

# create python virtual env
./miniconda.sh -b -p venv
TMP_PATH=$PATH
PATH=`cd venv/bin;pwd`:$PATH

# install dependency
pip install apache-flink
rm -rf venv/pkgs
PATH=$TMP_PATH
zip -r venv.zip venv
{% endhighlight %}

## How to Run Python UDF Locally
Firstly, you need to prepare a Python Virtual Env Used by PyFlink (You can refer to the previous section).

Then, you should execute the following script to activate your used Python virtual environment

{% highlight shell %}
$ venv/bin/conda init
# you need to restart the shell
$ conda activate
{% endhighlight %}
Now, you can run your Python UDF script directly.
{% highlight shell %}
python xxx.py
{% endhighlight %}

## How to Run Python UDF in Cluster
There are two ways of preparing a Python environment installed PyFlink which can be used in the Cluster

#### Install Packages in the machine of the cluster

{% highlight shell %}
$ pip install apache-flink
{% endhighlight %}

After installing, you can use `pyexec` command line arg or `set_python_executable` api of table_env to specify the path of the python interpreter which is used to execute the python udf workers.

#### Use Uploaded Virtual Environment
You can use `pyarch` command line arg or `add_python_archive` api of table_env to upload Python virtual environment and use `pyexec` command line arg or `set_python_executable` api to specify the path of the python interpreter which is used to execute the python udf workers.

For details about the command line args of `pyarch` and `pyexec`, you can refer to <a href="{{ site.baseurl }}/ops/cli.html#usage">command line arguments.</a>
For details about the usage of api `add_python_archive` and `set_python_executable`, you can refer to <a href="{{ site.baseurl }}/dev/table/python/dependency_management.html#usage">dependency management.</a>

## How to Add Python Files
You can use `pyfs` command line arg or `add_python_file` api of table_env to add python file dependencies which could be python files, python packages or local directories.

For example, if you want to add a python directory `myDir`, you can call the api `add_python_file` of table_env to add the whole myDir directory.
e.g. 
{% highlight python %}
table_env.add_python_file('myDir')
{% endhighlight %}

If you have a myudf.py in the directory `myDir/connectors`, you can use following code to import myudf.py
{% highlight python %}
from connectors import myudf
{% endhighlight %}
