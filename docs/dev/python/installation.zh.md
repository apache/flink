---
title: "环境安装"
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

## 环境要求
<span class="label label-info">注意</span> PyFlink需要特定的Python版本（3.5, 3.6, 3.7 或 3.8）。请运行以下命令，以确保Python版本满足要求。

{% highlight bash %}
$ python --version
# the version printed here must be 3.5, 3.6, 3.7 or 3.8
{% endhighlight %}

## PyFlink 安装

PyFlink已经被发布到PyPi，可以通过如下方式安装PyFlink：

{% highlight bash %}
$ python -m pip install apache-flink
{% endhighlight %}

您也可以从源码手动构建PyFlink，具体可以参见[开发指南]({% link flinkDev/building.zh.md %}#build-pyflink).

<span class="label label-info">注意</span> 从Flink 1.11版本开始, PyFlink作业支持在Windows系统上运行，因此您也可以在Windows上开发和调试PyFlink作业了。
