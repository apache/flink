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

Apache Flink provides a number of common configuration settings that work across all file system implementations. 

* This will be replaced by the TOC
{:toc}

## Default File System

If paths to files do not explicitly specify a file system scheme (and authority), a default scheme (and authority) will be used.

{% highlight yaml %}
fs.default-scheme: <default-fs>
{% endhighlight %}

For example, if the default file system configured as `fs.default-scheme: hdfs://localhost:9000/`, then a file path of
`/user/hugo/in.txt` is interpreted as `hdfs://localhost:9000/user/hugo/in.txt`.

## Connection limiting

You can limit the total number of connections that a file system can concurrently open. This is useful when the file system cannot handle a large number
of concurrent reads / writes or open connections at the same time.

For example, very small HDFS clusters with few RPC handlers can sometimes be overwhelmed by a large Flink job trying to build up many connections during a checkpoint.

To limit a specific file system's connections, add the following entries to the Flink configuration. The file system to be limited is identified by
its scheme.

{% highlight yaml %}
fs.<scheme>.limit.total: (number, 0/-1 mean no limit)
fs.<scheme>.limit.input: (number, 0/-1 mean no limit)
fs.<scheme>.limit.output: (number, 0/-1 mean no limit)
fs.<scheme>.limit.timeout: (milliseconds, 0 means infinite)
fs.<scheme>.limit.stream-timeout: (milliseconds, 0 means infinite)
{% endhighlight %}

You can limit the number if input/output connections (streams) separately (`fs.<scheme>.limit.input` and `fs.<scheme>.limit.output`), as well as impose a limit on
the total number of concurrent streams (`fs.<scheme>.limit.total`). If the file system tries to open more streams, the operation will block until some streams are closed.
If the opening of the stream takes longer than `fs.<scheme>.limit.timeout`, the stream opening will fail.

To prevent inactive streams from taking up the complete pool (preventing new connections to be opened), you can add an inactivity timeout for streams:
`fs.<scheme>.limit.stream-timeout`. If a stream does not read/write any bytes for at least that amount of time, it is forcibly closed.

These limits are enforced per TaskManager, so each TaskManager in a Flink application or cluster will open up to that number of connections.
In addition, the limits are also only enforced per FileSystem instance. Because File Systems are created per scheme and authority, different
authorities will have their own connection pool. For example `hdfs://myhdfs:50010/` and `hdfs://anotherhdfs:4399/` will have separate pools.