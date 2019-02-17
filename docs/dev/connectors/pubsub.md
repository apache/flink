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
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{ site.baseurl }}/dev/linking.html)
for information about how to package the program with the libraries for
cluster execution.

## Consuming or Producing PubSubMessages

### PubSub SourceFunction

The connector provides a Source for reading data from Google PubSub to Apache Flink.
Google PubSub has an `Atleast-Once` guarantee and as such this connector delivers the same guarantees.

The class `PubSubSource(…)` has a builder to create PubSubsources. `PubSubSource.newBuilder(...)`

There are several optional methods to alter how the PubSubSource is created,
the bare minimum is to provide a Google project, Pubsub subscription and a way to deserialize the PubSubMessages.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

DeserializationSchema<SomeObject> deserializationSchema = (...);
SourceFunction<SomeObject> pubsubSource = PubSubSource.newBuilder(deserializationSchema, "google-project", "subscription")
                                                .build();

streamExecEnv.addSource(source);
{% endhighlight %}
</div>
</div>

### PubSub Sink

The connector provides a Sink for writing data to PubSub.

The class `PubSubSource(…)` has a builder to create PubSubsources. `PubSubSource.newBuilder()`

This builder works in a similar way to the PubSubSource.
Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

SerializationSchema<SomeObject> serializationSchema = (...);
SinkFunction<SomeObject> pubsubSink = PubSubSink.newBuilder(deserializationSchema, "google-project", "topic")
                                                .build()

streamExecEnv.addSink(pubsubSink);
{% endhighlight %}
</div>
</div>

### Google Credentials

Google uses [Credentials](https://cloud.google.com/docs/authentication/production) to authenticate and authorize applications so that they can use Google cloud resources such as PubSub. Both builders allow several ways to provide these credentials.

By default the connectors will look for an environment variable: [GOOGLE_APPLICATION_CREDENTIALS](https://cloud.google.com/docs/authentication/production#obtaining_and_providing_service_account_credentials_manually) which should point to a file containing the credentials.

It is also possible to provide a Credentials object directly. For instance if you read the Credentials yourself from an external system you can use `PubSubSource.newBuilder(...).withCredentials(...)` .

### Integration testing

When running integration tests you might not want to connect to PubSub directly but use a docker container to read and write to.

This is possible by using `PubSubSource.newBuilder().withHostAndPort("localhost:1234")`, note in this case the connector will use the `NoCredentialsProvider` from the `google-cloud-pubsub` sdk to make sure it connects properly with the docker container.

## Backpressure

Backpressure can happen when the source function produces messages faster than the flink pipeline can handle.

The connector uses the Google Cloud PubSub SDK under the hood and this allows us to deal with backpressure. Through the PubSubSource builder you are able set a PubSubSubscriberFactory, this factory produces a [Subscriber](http://googleapis.github.io/google-cloud-java/google-cloud-clients/apidocs/index.html?com/google/cloud/pubsub/v1/package-summary.html). This `Subscriber` gives you control of how it handles backpressure through [Message Flow Control](https://cloud.google.com/pubsub/docs/pull#message-flow-control). For instance, in the following example we allow at most 10000 messages to be buffered (meaning messages read but not yet acknowledged), once we have more than 10000 messages we stop pulling in more messages.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

PubSubSubscriberFactory factory = (Credentials credentials, ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver) -> {
    FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
                                                                 .setMaxOutstandingElementCount(10000L)
                                                                 .setMaxOutstandingRequestBytes(100000L)
                                                                 .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                                                                 .build();
    return Subscriber.newBuilder(projectSubscriptionName, messageReceiver)
                     .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                     .setFlowControlSettings(flowControlSettings)
                     .setMaxAckExtensionPeriod(Duration.ofMinutes(5))
                     .build();
};

DeserializationSchema<SomeObject> deserializationSchema = (...);
SourceFunction<SomeObject> pubsubSource = PubSubSource.newBuilder(deserializationSchema, "google-project", "subscription")
                                                .withPubSubSubscriberFactory(factory)
                                                .build();

streamExecEnv.addSource(source);
{% endhighlight %}
</div>
</div>

One important aspect to keep in mind is the 10000 messages limit is based on the amount of messages that has not been acknowledged yet. The connector will only acknowledge messages on successful checkpoints. This means if you checkpoint once every minute and you set the message limit to 10000. Your max throughput will be 10000 messages per minute.

To give insight into this behavior 2 metrics have been added:
  * `PubSubMessagesReceivedNotProcessed` this is the amount of messages that has been received but have not been processed yet. If this number is high that is a good indicator you are having backpressure problems.
  * `PubSubMessagesProcessedNotAcked` this is the amount of messages that has been send to the next operator in the pipeline but has not yet been acknowledged. (Again note: only after a successful checkpoint are messages acknowledged)

{% top %}
