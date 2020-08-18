---
title: "Installation"
nav-parent_id: python_start
nav-pos: 10
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
<span class="label label-info">Note</span> Python version (3.5, 3.6 or 3.7) is required for PyFlink. Please run the following command to make sure that it meets the requirements:

{% highlight bash %}
$ python --version
# the version printed here must be 3.5, 3.6 or 3.7
{% endhighlight %}

## Installation of PyFlink

PyFlink has already been deployed to PyPi and can be installed as following:

{% highlight bash %}
$ python -m pip install apache-flink
{% endhighlight %}

You can also build PyFlink from source by following the [development guide]({% link flinkDev/building.md %}#build-pyflink).

<span class="label label-info">Note</span> Starting from Flink 1.11, it's also supported to run PyFlink jobs locally on Windows and so you could develop and debug PyFlink jobs on Windows.
