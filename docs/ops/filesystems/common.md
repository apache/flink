---
title: "Common Configurations"
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

Apache Flink provides several standard configuration settings that work across all file system implementations. 

* This will be replaced by the TOC
{:toc}

## Default File System

A default scheme (and authority) is used if paths to files do not explicitly specify a file system scheme (and authority).

{% highlight yaml %}
fs.default-scheme: <default-fs>
{% endhighlight %}

For example, if the default file system configured as `fs.default-scheme: hdfs://localhost:9000/`, then a file path of
`/user/hugo/in.txt` is interpreted as `hdfs://localhost:9000/user/hugo/in.txt`.

## Connection limiting

You can limit the total number of connections that a file system can concurrently open which is useful when the file system cannot handle a large number
of concurrent reads/writes or open connections at the same time.

For example, small HDFS clusters with few RPC handlers can sometimes be overwhelmed by a large Flink job trying to build up many connections during a checkpoint.

To limit a specific file system's connections, add the following entries to the Flink configuration. The file system to be limited is identified by
its scheme.

{% highlight yaml %}
fs.<scheme>.limit.total: (number, 0/-1 mean no limit)
fs.<scheme>.limit.input: (number, 0/-1 mean no limit)
fs.<scheme>.limit.output: (number, 0/-1 mean no limit)
fs.<scheme>.limit.timeout: (milliseconds, 0 means infinite)
fs.<scheme>.limit.stream-timeout: (milliseconds, 0 means infinite)
{% endhighlight %}

You can limit the number of input/output connections (streams) separately (`fs.<scheme>.limit.input` and `fs.<scheme>.limit.output`), as well as impose a limit on
the total number of concurrent streams (`fs.<scheme>.limit.total`). If the file system tries to open more streams, the operation blocks until some streams close.
If the opening of the stream takes longer than `fs.<scheme>.limit.timeout`, the stream opening fails.

To prevent inactive streams from taking up the full pool (preventing new connections to be opened), you can add an inactivity timeout which forcibly closes them if they do not read/write any bytes for at least that amount of time: `fs.<scheme>.limit.stream-timeout`. 

Limit enforcment on a per TaskManager/file system basis.
Because file systems creation occurs per scheme and authority, different
authorities have independent connection pools. For example `hdfs://myhdfs:50010/` and `hdfs://anotherhdfs:4399/` will have separate pools.

{% top %}