---
title: "Installation"
nav-parent_id: python
nav-pos: 15
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

## Environment Requirements
<span class="label label-info">Note</span> Python version (3.5, 3.6, 3.7 or 3.8) is required for PyFlink. Please run the following command to make sure that it meets the requirements:

{% highlight bash %}
$ python --version
# the version printed here must be 3.5, 3.6, 3.7 or 3.8
{% endhighlight %}

## Environment Setup

Your system may include multiple Python versions, and thus also include multiple Python binary executables. You can run the following
`ls` command to find out what Python binary executables are available in your system:

{% highlight bash %}
$ ls /usr/bin/python*
{% endhighlight %}

To satisfy the PyFlink requirement regarding the Python environment version, you can choose to soft link `python` to point to your `python3` interpreter:

{% highlight bash %}
ln -s /usr/bin/python3 python
{% endhighlight %}

In addition to creating a soft link, you can also choose to create a Python virtual environment (`venv`). You can refer to the [Preparing Python Virtual Environment]({% link dev/python/faq.md %}#preparing-python-virtual-environment) documentation page for details on how to achieve that setup.

If you don’t want to use a soft link to change the system's `python` interpreter point to, you can use the configuration way to specify the Python interpreter.
For specifying the Python interpreter used to compile the jobs, you can refer to the [python client executable]({% link dev/python/python_config.md %}#python-client-executable)
For specifying the Python interpreter used to execute the python UDF worker, you can refer to the [python.executable]({% link dev/python/python_config.md %}#python-executable)

## Installation of PyFlink

PyFlink is available through [PyPi](https://pypi.org/project/apache-flink/) as a regular Python package and can be installed as follows:

{% highlight bash %}
{% if site.is_stable %}
$ python -m pip install apache-flink {{ site.version }}
{% else %}
$ python -m pip install apache-flink
{% endif %}
{% endhighlight %}

You can also build PyFlink from source by following the [development guide]({% link flinkDev/building.md %}#build-pyflink).

<span class="label label-info">Note</span> Starting from Flink 1.11, it's also supported to run PyFlink jobs locally on Windows and so you could develop and debug PyFlink jobs on Windows.
