---
title: "Redis Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 6
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

This connector provides a Sink that can write to
[Redis](http://redis.io/) and also can publish data to [Redis PubSub](http://redis.io/topics/pubsub). To use this connector, add the
following dependency to your project:
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-redis{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
Version Compatibility: This module is compatible with Redis 2.8.5.

Note that the streaming connectors are currently not part of the binary distribution. You need to link them for cluster execution [explicitly]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Redis
Follow the instructions from the [Redis download page](http://redis.io/download).

#### Redis Sink
A class providing an interface for sending data to Redis. 
The sink can use three different methods for communicating with different type of Redis environments:
1. Single Redis Server
2. Redis Cluster
3. Redis Sentinel

This code shows how to create a sink that communicate to a single redis server:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>>{

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }
}
FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

DataStream<String> stream = ...;
stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class RedisExampleMapper extends RedisMapper[(String, String)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")
  }

  override def getKeyFromData(data: (String, String)): String = data._1

  override def getValueFromData(data: (String, String)): String = data._2
}
val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()
stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))
{% endhighlight %}
</div>
</div>

This example code does the same, but for Redis Cluster:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
    .setNodes(new HashSet<InetSocketAddress>(Arrays.asList(new InetSocketAddress(5601)))).build();

DataStream<String> stream = ...;
stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val conf = new FlinkJedisPoolConfig.Builder().setNodes(...).build()
stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))
{% endhighlight %}
</div>
</div>

This example shows when the Redis environment is with Sentinels:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

FlinkJedisSentinelConfig conf = new FlinkJedisSentinelConfig.Builder()
    .setMasterName("master").setSentinels(...).build();

DataStream<String> stream = ...;
stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val conf = new FlinkJedisSentinelConfig.Builder().setMasterName("master").setSentinels(...).build()
stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))
{% endhighlight %}
</div>
</div>

This section gives a description of all the available data types and what Redis command used for that.

<table class="table table-bordered" style="width: 75%">
    <thead>
        <tr>
          <th class="text-center" style="width: 20%">Data Type</th>
          <th class="text-center" style="width: 25%">Redis Command [Sink]</th>
          <th class="text-center" style="width: 25%">Redis Command [Source]</th>
        </tr>
      </thead>
      <tbody>
        <tr>
            <td>HASH</td><td><a href="http://redis.io/commands/hset">HSET</a></td><td>--NA--</td>
        </tr>
        <tr>
            <td>LIST</td><td>
                <a href="http://redis.io/commands/rpush">RPUSH</a>, 
                <a href="http://redis.io/commands/lpush">LPUSH</a>
            </td><td>--NA--</td>
        </tr>
        <tr>
            <td>SET</td><td><a href="http://redis.io/commands/rpush">SADD</a></td><td>--NA--</td>
        </tr>
        <tr>
            <td>PUBSUB</td><td><a href="http://redis.io/commands/publish">PUBLISH</a></td><td>--NA--</td>
        </tr>
        <tr>
            <td>STRING</td><td><a href="http://redis.io/commands/set">SET</a></td><td>--NA--</td>
        </tr>
        <tr>
            <td>HYPER_LOG_LOG</td><td><a href="http://redis.io/commands/pfadd">PFADD</a></td><td>--NA--</td>
        </tr>
        <tr>
            <td>SORTED_SET</td><td><a href="http://redis.io/commands/zadd">ZADD</a></td><td>--NA--</td>
        </tr>                
      </tbody>
</table>
More about Redis can be found [here](http://redis.io/).
