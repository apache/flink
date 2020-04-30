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

This page describes the solutions to some frequently encountered problems for PyFlink users.

* This will be replaced by the TOC
{:toc}
## Preparing Python Virtual Environment
You can prepare the Python virtual environment as following:
{% highlight shell %}
# download miniconda.sh
set -e
if [ `uname -s` == "Darwin" ]; then
    wget "https://repo.continuum.io/miniconda/Miniconda3-4.7.10-MacOSX-x86_64.sh" -O "miniconda.sh"
else
    wget "https://repo.continuum.io/miniconda/Miniconda3-4.7.10-Linux-x86_64.sh" -O "miniconda.sh"
fi

# add the execution permission
chmod +x miniconda.sh

# create python virtual environment
./miniconda.sh -b -p venv

# activate the conda python virtual environment
source venv/bin/activate

# install PyFlink
pip install apache-flink

# deactivate the conda python virtual environment
conda deactivate

# remove the cached packages
rm -rf venv/pkgs

# package the prepared conda python virtual environment
zip -r venv.zip venv
{% endhighlight %}

## Execute PyFlink jobs with Python virtual environment
You can refer to the section [Preparing Python Virtual Environment](#preparing-python-virtual-environment) on how to
prepare the Python virtual environment.

You should activate the Python virtual environment before executing the PyFlink job.

#### Local

{% highlight shell %}
# activate the conda python virtual environment
$ source venv/bin/activate
$ python xxx.py
{% endhighlight %}

#### Cluster

{% highlight shell %}
$ # specify the Python virtual environment
$ table_env.add_python_archive("venv.zip")
$ # specify the path of the python interpreter which is used to execute the python udf workers
$ table_env.get_config().set_python_executable("venv.zip/venv/bin/python")
{% endhighlight %}

You can refer to <a href="{{ site.baseurl }}/dev/table/python/dependency_management.html#usage">dependency management</a> for more details on the usage of `add_python_archive` and `set_python_executable`.

## Adding Jar Files
A PyFlink job may depend on jar files, i.e. connectors, Java udfs, etc.

You can specify the dependencies with the following Python Table APIs or through <a href="{{ site.baseurl }}/ops/cli.html#usage">command line arguments</a> directly when submitting the job.
{% highlight python %}
# NOTE: Only local file URLs (start with "file://") are supported.
table_env.get_config().set_configuration("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")

# NOTE: The Paths must specify a protocol (e.g. file://) and users should ensure that the URLs are accessible on both the client and the cluster.
table_env.get_config().set_configuration("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
{% endhighlight %}

You can refer to <a href="{{ site.baseurl }}/dev/table/python/dependency_management.html##java-dependency">Adding Java Dependency</a> for more details.

## Adding Python Files
You can use the command line arguments `pyfs` or the API `add_python_file` of `TableEnvironment` to add python file dependencies which could be python files, python packages or local directories.

For example, if you have a directory named `myDir` which has the following hierarchy:
```
myDir
├──utils
    ├──__init__.py
    ├──my_util.py
```

you can add the Python files of directory `myDir` as following:
{% highlight python %}
table_env.add_python_file('myDir')

def my_udf():
    from utils import my_util
{% endhighlight %}
