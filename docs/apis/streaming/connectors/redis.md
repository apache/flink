---
title: "Redis Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 4
sub-nav-title: Redis
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

This connector provides access to data streams from [Redis PubSub](http://redis.io/topics/pubsub). To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-redis{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Redis
Follow the instructions from the [Redis download page](http://redis.io/download).

#### Redis Sink
A class providing an interface for sending data to Redis. It internally sends data to redis
channel using Redis PUBLISH command

The followings have to be provided for the `RedisSink(…)` constructor in order:

1. The hostname
2. The port number
3. The channel name
4. Serialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new RedisSink<String>("localhost", 6379, "hello", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new RedisSink[String]("localhost", 6379, "hello", new SimpleStringSchema))
{% endhighlight %}
</div>
</div>

More about Redis can be found [here](http://redis.io/).