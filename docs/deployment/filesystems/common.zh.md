---
title: "通用配置"
nav-parent_id: filesystems
nav-pos: 0
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

Apache Flink 提供了一些对所有文件系统均适用的基本配置。

* This will be replaced by the TOC
{:toc}

## 默认文件系统

如果文件路径未明确指定文件系统的 scheme（和 authority），将会使用默认的 scheme（和 authority）：

{% highlight yaml %}
fs.default-scheme: <default-fs>
{% endhighlight %}

例如默认的文件系统配置为 `fs.default-scheme: hdfs://localhost:9000/`，则文件路径 `/user/hugo/in.txt` 将被处理为 `hdfs://localhost:9000/user/hugo/in.txt`。

## 连接限制

如果文件系统不能处理大量并发读/写操作或连接，可以为文件系统同时打开的总连接数设置上限。

例如在一个大型 Flink 任务建立 checkpoint 时，具有少量 RPC handler 的小型 HDFS 集群可能会由于建立了过多的连接而过载。

要限制文件系统的连接数，可将下列配置添加至 Flink 配置中。设置限制的文件系统由其 scheme 指定：

{% highlight yaml %}
fs.<scheme>.limit.total: (数量，0/-1 表示无限制)
fs.<scheme>.limit.input: (数量，0/-1 表示无限制)
fs.<scheme>.limit.output: (数量，0/-1 表示无限制)
fs.<scheme>.limit.timeout: (毫秒，0 表示无穷)
fs.<scheme>.limit.stream-timeout: (毫秒，0 表示无穷)
{% endhighlight %}

输入和输出连接（流）的数量可以分别进行限制（`fs.<scheme>.limit.input` 和 `fs.<scheme>.limit.output`），也可以限制并发流的总数（`fs.<scheme>.limit.total`）。如果文件系统尝试打开更多的流，操作将被阻塞直至某些流关闭。如果打开流的时间超过 `fs.<scheme>.limit.timeout`，则流打开失败。

为避免不活动的流占满整个连接池（阻止新连接的建立），可以在配置中添加无活动超时时间，如果连接至少在 `fs.<scheme>.limit.stream-timeout` 时间内没有读/写操作，则连接会被强制关闭。

连接数是按每个 TaskManager/文件系统来进行限制的。因为文件系统的创建是按照 scheme 和 authority 进行的，所以不同的 authority 具有独立的连接池，例如 `hdfs://myhdfs:50010/` 和 `hdfs://anotherhdfs:4399/` 会有单独的连接池。

{% top %}