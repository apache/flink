---
title: "环境安装"
nav-parent_id: python_tableapi
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
<span class="label label-info">Note</span> Python 3.5+ is required for PyFlink. Please run the following command to make sure that it meets the requirements:

{% highlight bash %}
$ python --version
# the version printed here must be 3.5+
{% endhighlight %}

<span class="label label-info">Note</span> PyFlink depends on apache-beam 2.19.0 for Python UDF execution. You can run the following command to install apache-beam:

{% highlight bash %}
$ python -m pip install apache-beam==2.19.0
{% endhighlight %}
