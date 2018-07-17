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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider.credentialsProviderFromEnvironmentVariables;


/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them as soon as they have been received.
 */
public class PubSubSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, AckReplyConsumer> implements MessageReceiver, ResultTypeQueryable<OUT> {
	private final DeserializationSchema<OUT> deserializationSchema;
	private final SubscriberWrapper subscriberWrapper;
	private boolean autoAcknowledge = true;

	private transient SourceContext<OUT> sourceContext = null;

	PubSubSource(SubscriberWrapper subscriberWrapper, DeserializationSchema<OUT> deserializationSchema) {
		super(String.class);
		this.deserializationSchema = deserializationSchema;
		this.subscriberWrapper = subscriberWrapper;
	}

	/**
	 * Convenience factory method to return a PubSubSource with default application credentials based on environment variables. ({@link org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider})
	 * @param projectName The name of the google project where the subscription is in
	 * @param subscriptionName The name of the subscription to read from
	 * @param deserializationSchema Schema to deserialize the {@link PubsubMessage}
	 * @param <OUT> The type of messages that will be read
	 * @return Returns a RichParallelSourceFunction which reads from a PubSub subscription
	 * @throws Exception exception is thrown when no default application credentials can be found
	 */
	public static <OUT> PubSubSource<OUT> withDefaultApplicationCredentials(String projectName, String subscriptionName, DeserializationSchema<OUT> deserializationSchema) throws Exception {
		return withCustomApplicationCredentials(projectName, subscriptionName, deserializationSchema, credentialsProviderFromEnvironmentVariables());
	}

	/**
	 * Factory method to return a PubSubSource.
	 * @param projectName The name of the google project where the subscription is in
	 * @param subscriptionName The name of the subscription to read from
	 * @param deserializationSchema Schema to deserialize the {@link PubsubMessage}
	 * @param serializableCredentialsProvider CredentialsProvider used to give the correct permissions to read from PubSub
	 * @param <OUT> The type of messages that will be read
	 * @return Returns a RichParallelSourceFunction which reads from a PubSub subscription
	 */
	public static <OUT> PubSubSource<OUT> withCustomApplicationCredentials(String projectName, String subscriptionName, DeserializationSchema<OUT> deserializationSchema, SerializableCredentialsProvider serializableCredentialsProvider) {
		return new PubSubSource<>(new SubscriberWrapper(serializableCredentialsProvider, ProjectSubscriptionName.of(projectName, subscriptionName)), deserializationSchema);
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		subscriberWrapper.initialize(this);
		if (hasCheckpointingEnabled(getRuntimeContext())) {
			autoAcknowledge = false;
		}
	}

	private boolean hasCheckpointingEnabled(RuntimeContext runtimeContext) {
		return runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled();
	}

	@Override
	protected void acknowledgeSessionIDs(List<AckReplyConsumer> ackReplyConsumers) {
		ackReplyConsumers.forEach(AckReplyConsumer::ack);
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) {
		this.sourceContext = sourceContext;
		subscriberWrapper.startBlocking();
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		if (sourceContext == null) {
			consumer.nack();
			return;
		}

		if (autoAcknowledge) {
			sourceContext.collect(deserializeMessage(message));
			consumer.ack();
			return;
		}

		processMessage(message, consumer);
	}

	private void processMessage(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
		synchronized (sourceContext.getCheckpointLock()) {
			boolean alreadyProcessed = !addId(message.getMessageId());
			if (alreadyProcessed) {
				return;
			}

			sessionIds.add(ackReplyConsumer);
			sourceContext.collect(deserializeMessage(message));
		}
	}

	@Override
	public void cancel() {
		subscriberWrapper.stop();
	}

	private OUT deserializeMessage(PubsubMessage message) {
		try {
			return deserializationSchema.deserialize(message.getData().toByteArray());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
