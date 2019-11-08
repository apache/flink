---
title: "Twitter 连接器"
nav-title: Twitter
nav-parent_id: connectors
nav-pos: 8
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

[Twitter Streaming API](https://dev.twitter.com/docs/streaming-apis) 提供了访问 Twitter 的 tweets 流的能力。
Flink Streaming 通过一个内置的 `TwitterSource` 类来创建到 tweets 流的连接。
使用 Twitter 连接器，需要在工程中添加下面的依赖：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-twitter{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

注意：当前的二进制发行版还没有这些连接器。集群执行请参考[这里]({{site.baseurl}}/zh/dev/projectsetup/dependencies.html).

#### 认证
使用 Twitter 流，用户需要先注册自己的程序，获取认证相关的必要信息。过程如下：

#### 获取认证信息
首先，需要一个 Twitter 账号。可以通过 [twitter.com/signup](https://twitter.com/signup) 免费注册，
或者在 Twitter 的 [Application Management](https://apps.twitter.com/) 登录，然后点击 "Create New App"
 按钮来注册应用,填写应用程序相关表格并且接受条款。选择应用程序之后，可以在 "API Keys" 标签页看到 API key 和 
 API secret（对应于`TwitterSource`中的`twitter-source.consumerKey` 和 `twitter-source.consumerSecret` ）。
请保管好这些信息并且不要将其发布到public的仓库。


#### 使用
和其他的连接器不同的是，`TwitterSource` 没有任何其他依赖。下面的示例代码就可以优雅的运行：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties props = new Properties();
props.setProperty(TwitterSource.CONSUMER_KEY, "");
props.setProperty(TwitterSource.CONSUMER_SECRET, "");
props.setProperty(TwitterSource.TOKEN, "");
props.setProperty(TwitterSource.TOKEN_SECRET, "");
DataStream<String> streamSource = env.addSource(new TwitterSource(props));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val props = new Properties()
props.setProperty(TwitterSource.CONSUMER_KEY, "")
props.setProperty(TwitterSource.CONSUMER_SECRET, "")
props.setProperty(TwitterSource.TOKEN, "")
props.setProperty(TwitterSource.TOKEN_SECRET, "")
val streamSource = env.addSource(new TwitterSource(props))
{% endhighlight %}
</div>
</div>

`TwitterSource` 会发出包含了JSON object的字符串，这样的字符串表示一个Tweet.

`flink-examples-streaming` 中的 `TwitterExample` 类是使用 `TwitterSource` 的完整示范。

`TwitterSource` 默认使用 `StatusesSampleEndpoint`。`StatusesSampleEndpoint` 会返回一个 Tweets 的随机抽样。用户可以通过实现 `TwitterSource.EndpointInitializer` 接口来自定义 endpoint。

{% top %}
