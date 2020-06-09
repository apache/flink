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

## 环境要求
<span class="label label-info">注意</span> PyFlink 需要特定的Python 版本（3.5, 3.6 或 3.7）。请运行如下的命令确保版本满足要求。

{% highlight bash %}
$ python --version
# the version printed here must be 3.5, 3.6 or 3.7
{% endhighlight %}

## PyFlink 安装

PyFlink 已经被部署到 PyPi，可以按如下方式安装：

{% highlight bash %}
$ python -m pip install apache-flink
{% endhighlight %}

你也可以从源码手动构建 PyFlink，具体可以参见[开发指南]({{ site.baseurl }}/zh/flinkDev/building.html#build-pyflink).

<span class="label label-info">注意</span> 从 Flink 1.11 版本开始, PyFlink 作业可以支持在 Windows 系统上本地运行，因此你也可以在 Windows 上开发和调试 PyFlink 作业了。
