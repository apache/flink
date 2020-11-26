---
title: "Twitter Connector"
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

The [Twitter Streaming API](https://dev.twitter.com/docs/streaming-apis) provides access to the stream of tweets made available by Twitter.
Flink Streaming comes with a built-in `TwitterSource` class for establishing a connection to this stream.
To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-twitter{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution.
See linking with them for cluster execution [here]({% link dev/project-configuration.md %}).

#### Authentication
In order to connect to the Twitter stream the user has to register their program and acquire the necessary information for the authentication. The process is described below.

#### Acquiring the authentication information
First of all, a Twitter account is needed. Sign up for free at [twitter.com/signup](https://twitter.com/signup)
or sign in at Twitter's [Application Management](https://apps.twitter.com/) and register the application by
clicking on the "Create New App" button. Fill out a form about your program and accept the Terms and Conditions.
After selecting the application, the API key and API secret (called `twitter-source.consumerKey` and `twitter-source.consumerSecret` in `TwitterSource` respectively) are located on the "API Keys" tab.
The necessary OAuth Access Token data (`twitter-source.token` and `twitter-source.tokenSecret` in `TwitterSource`) can be generated and acquired on the "Keys and Access Tokens" tab.
Remember to keep these pieces of information secret and do not push them to public repositories.



#### Usage
In contrast to other connectors, the `TwitterSource` depends on no additional services. For example the following code should run gracefully:

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

The `TwitterSource` emits strings containing a JSON object, representing a Tweet.

The `TwitterExample` class in the `flink-examples-streaming` package shows a full example how to use the `TwitterSource`.

By default, the `TwitterSource` uses the `StatusesSampleEndpoint`. This endpoint returns a random sample of Tweets.
There is a `TwitterSource.EndpointInitializer` interface allowing users to provide a custom endpoint.

{% top %}
