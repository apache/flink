/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;

import static org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider.credentialsProviderFromEnvironmentVariables;


/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them as soon as they have been received.
 */
public class PubSubSource<OUT> extends RichParallelSourceFunction<OUT> implements MessageReceiver {
	private final DeserializationSchema<OUT> deserializationSchema;
	private final SubscriberFactory subscriberFactory;

	private transient SourceContext<OUT> sourceContext = null;

	PubSubSource(SubscriberFactory subscriberFactory, DeserializationSchema<OUT> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
		this.subscriberFactory = subscriberFactory;
	}

	/**
	 * Convenience factory method to return a PubSubSource with default application credentials based on environment variables. ({@link org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider})
	 * @param projectSubscriptionName The google project and subscription to read from
	 * @param deserializationSchema Schema to deserialize the {@link PubsubMessage}
	 * @param <OUT> The type of messages that will be read
	 * @return Returns a RichParallelSourceFunction which reads from a PubSub subscription
	 * @throws Exception exception is thrown when no default application credentials can be found
	 */
	public static <OUT> PubSubSource<OUT> withDefaultApplicationCredentials(ProjectSubscriptionName projectSubscriptionName, DeserializationSchema<OUT> deserializationSchema) throws Exception {
		return withCustomApplicationCredentials(projectSubscriptionName, deserializationSchema, credentialsProviderFromEnvironmentVariables());
	}

	/**
	 * Factory method to return a PubSubSource.
	 * @param projectSubscriptionName The google project and subscription to read from
	 * @param deserializationSchema Schema to deserialize the {@link PubsubMessage}
	 * @param serializableCredentialsProvider CredentialsProvider used to give the correct permissions to read from PubSub
	 * @param <OUT> The type of messages that will be read
	 * @return Returns a RichParallelSourceFunction which reads from a PubSub subscription
	 */
	public static <OUT> PubSubSource<OUT> withCustomApplicationCredentials(ProjectSubscriptionName projectSubscriptionName, DeserializationSchema<OUT> deserializationSchema, SerializableCredentialsProvider serializableCredentialsProvider) {
		return new PubSubSource<>(new SubscriberFactory(serializableCredentialsProvider, projectSubscriptionName), deserializationSchema);
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		subscriberFactory.initialize(this);
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) {
		this.sourceContext = sourceContext;
		subscriberFactory.startBlocking();
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		if (sourceContext == null) {
			consumer.nack();
			return;
		}

		sourceContext.collect(uncheckedExceptionDeserialize(message.getData().toByteArray()));
		consumer.ack();
	}

	@Override
	public void cancel() {
		subscriberFactory.stop();
	}

	private OUT uncheckedExceptionDeserialize(byte[] bytes) {
		try {
			return deserializationSchema.deserialize(bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
