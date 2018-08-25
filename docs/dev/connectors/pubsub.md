---
title: "Google PubSub"
nav-title: PubSub
nav-parent_id: connectors
nav-pos: 7
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

This connector provides a Source and Sink that can read from and write to
[Google PubSub](https://cloud.google.com/pubsub). To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-pubsub{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{site.baseurl}}/dev/linking.html)
for information about how to package the program with the libraries for
cluster execution.

#### PubSub Source

The connector provides a Source for reading data from Google PubSub to Apache Flink. PubSub has an Atleast-Once guarantee and as such.

The class `PubSubSource(…)` has a builder to create PubSubsources. `PubSubSource.newBuilder()`

There are several optional methods to alter how the PubSubSource is created, the bare minimum is to provide a google project and pubsub subscription and a way to deserialize the PubSubMessages.
Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

DeserializationSchema<SomeObject> deserializationSchema = (...);
SourceFunction<SomeObject> pubsubSource = PubSubSource.<SomeObject>newBuilder()
                                                      .withDeserializationSchema(deserializationSchema)
                                                      .withProjectSubscriptionName("google-project-name", "pubsub-subscription")
                                                      .build();

streamExecEnv.addSource(pubsubSource);
{% endhighlight %}
</div>
</div>

#### PubSub Sink

The connector provides a Sink for writing data to PubSub.

The class `PubSubSource(…)` has a builder to create PubSubsources. `PubSubSource.newBuilder()`

This builder works in a similar way to the PubSubSource.
Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

SerializationSchema<SomeObject> serializationSchema = (...);
SinkFunction<SomeObject> pubsubSink = PubSubSink.<SomeObject>newBuilder()
                                                  .withSerializationSchema(serializationSchema)
                                                  .withTopicName("pubsub-topic-name")
                                                  .withProjectName("google-project-name")
                                                  .build()

streamExecEnv.addSink(pubsubSink);
{% endhighlight %}
</div>
</div>

#### Google Credentials

Google uses [Credentials](https://cloud.google.com/docs/authentication/production) to authenticate and authorize applications so that they can use Google cloud resources such as PubSub. Both builders allow several ways to provide these credentials.

By default the connectors will look for an environment variable: [GOOGLE_APPLICATION_CREDENTIALS](https://cloud.google.com/docs/authentication/production#obtaining_and_providing_service_account_credentials_manually) which should point to a file containing the credentials.

It is also possible to provide a Credentials object directly. For instance if you read the Credentials yourself from an external system. In this case you can use `PubSubSource.newBuilder().withCredentials(...)`

#### Integration testing

When using integration tests you might not want to connect to PubSub directly but use a docker container to read and write to. This is possible by using `PubSubSource.newBuilder().withHostAndPort("localhost:1234")`.
{% top %}
