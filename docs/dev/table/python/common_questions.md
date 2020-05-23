---
title: "Common Questions"
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

This page describes the solutions to some common questions for PyFlink users.

* This will be replaced by the TOC
{:toc}

## Preparing Python Virtual Environment

You can download a [convenience script]({{ site.baseurl }}/downloads/setup-pyflink-virtual-env.sh) to prepare a Python virtual env zip which can be used on Mac OS and most Linux distributions.
You can specify the version parameter to generate a Python virtual environment required for the corresponding PyFlink version, otherwise the most recent version will be installed.

{% highlight bash %}
{% if site.is_stable %}
$ setup-pyflink-virtual-env.sh {{ site.version }}
{% else %}
$ setup-pyflink-virtual-env.sh
{% endif %}
{% endhighlight bash %}

## Execute PyFlink jobs with Python virtual environment

After setting up a [python virtual environment](#preparing-python-virtual-environment), as described in the previous section, you should activate the environment before executing the PyFlink job.

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
$ # specify the path of the python interpreter which is used to execute the python UDF workers
$ table_env.get_config().set_python_executable("venv.zip/venv/bin/python")
{% endhighlight %}

For details on the usage of `add_python_archive` and `set_python_executable`, you can refer to [the relevant documentation]({{ site.baseurl }}/dev/table/python/dependency_management.html#usage).

## Adding Jar Files

A PyFlink job may depend on jar files, i.e. connectors, Java UDFs, etc.
You can specify the dependencies with the following Python Table APIs or through <a href="{{ site.baseurl }}/ops/cli.html#usage">command-line arguments</a> directly when submitting the job.

{% highlight python %}
# NOTE: Only local file URLs (start with "file://") are supported.
table_env.get_config().set_configuration("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")

# NOTE: The Paths must specify a protocol (e.g. file://) and users should ensure that the URLs are accessible on both the client and the cluster.
table_env.get_config().set_configuration("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
{% endhighlight %}

For details about the APIs of adding Java dependency, you can refer to [the relevant documentation]({{ site.baseurl }}/dev/table/python/dependency_management.html##java-dependency)

## Adding Python Files
You can use the command-line arguments `pyfs` or the API `add_python_file` of `TableEnvironment` to add python file dependencies which could be python files, python packages or local directories.
For example, if you have a directory named `myDir` which has the following hierarchy:

```
myDir
├──utils
    ├──__init__.py
    ├──my_util.py
```

You can add the Python files of directory `myDir` as following:

{% highlight python %}
table_env.add_python_file('myDir')

def my_udf():
    from utils import my_util
{% endhighlight %}
