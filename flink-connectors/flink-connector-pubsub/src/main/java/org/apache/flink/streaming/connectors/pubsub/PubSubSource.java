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
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;

/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them on the next checkpoint.
 * This ensures every message will get acknowledged at least once.
 */
public class PubSubSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, AckReplyConsumer>
		implements ResultTypeQueryable<OUT>, ParallelSourceFunction<OUT>, StoppableFunction {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);
	protected DeserializationSchema<OUT> deserializationSchema;
	protected SubscriberWrapper subscriberWrapper;

	PubSubSource(DeserializationSchema<OUT> deserializationSchema, SubscriberWrapper subscriberWrapper) {
		super(String.class);
		this.deserializationSchema = deserializationSchema;
		this.subscriberWrapper = subscriberWrapper;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		subscriberWrapper.initialize();
		if (hasNoCheckpointingEnabled(getRuntimeContext())) {
			throw new IllegalArgumentException("The PubSubSource REQUIRES Checkpointing to be enabled and " +
				"the checkpointing frequency must be MUCH lower than the PubSub timeout for it to retry a message.");
		}

		getRuntimeContext().getMetricGroup().gauge("PubSubMessagesProcessedNotAcked", this::getOutstandingMessagesToAck);
		getRuntimeContext().getMetricGroup().gauge("PubSubMessagesReceivedNotProcessed", subscriberWrapper::amountOfMessagesInBuffer);
	}

	private boolean hasNoCheckpointingEnabled(RuntimeContext runtimeContext) {
		return !(runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled());
	}

	@Override
	protected void acknowledgeSessionIDs(List<AckReplyConsumer> ackReplyConsumers) {
		ackReplyConsumers.forEach(AckReplyConsumer::ack);
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) throws Exception {
		subscriberWrapper.start();

		while (subscriberWrapper.isRunning()) {
			try {
				Tuple2<PubsubMessage, AckReplyConsumer> newMessage = subscriberWrapper.take();
				if (newMessage != null) {
					processMessage(sourceContext, newMessage);
				}
			} catch (InterruptedException e) {
				LOG.debug("Interrupted - stop or cancel called?");
			}
		}

		nackOutstandingMessages();

		LOG.debug("Waiting for PubSubSubscriber to terminate.");
		subscriberWrapper.awaitTerminated();
	}

	void processMessage(SourceContext<OUT> sourceContext, Tuple2<PubsubMessage, AckReplyConsumer> input) {
		PubsubMessage message = input.f0;
		AckReplyConsumer ackReplyConsumer = input.f1;

		synchronized (sourceContext.getCheckpointLock()) {
			boolean alreadyProcessed = !addId(message.getMessageId());
			if (alreadyProcessed) {
				return;
			}

			sessionIds.add(ackReplyConsumer);
			OUT deserializedMessage = deserializeMessage(message);
			sourceContext.collect(deserializedMessage);
			if (deserializationSchema.isEndOfStream(deserializedMessage)) {
				stop();
			}
		}
	}

	private Integer getOutstandingMessagesToAck() {
		return this.sessionIdsPerSnapshot
				.stream()
				.mapToInt(tuple -> tuple.f1.size())
				.sum() + this.sessionIds.size();
	}

	private void nackOutstandingMessages() {
		LOG.debug("Going to nack {} processed but not checkpointed pubsub messages.", getOutstandingMessagesToAck());
		this.sessionIdsPerSnapshot.stream()
			.flatMap(tuple -> tuple.f1.stream())
			.forEach(AckReplyConsumer::nack);
		this.sessionIds.forEach(AckReplyConsumer::nack);
		LOG.debug("Finished nacking pubsub messages.");
	}

	@Override
	public void cancel() {
		subscriberWrapper.stop();
	}

	@Override
	public void stop() {
		subscriberWrapper.stopAsync();
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

	public static <OUT> PubSubSourceBuilder<OUT> newBuilder(DeserializationSchema<OUT> deserializationSchema, String projectName, String subscriptionName) {
		Preconditions.checkNotNull(deserializationSchema);
		Preconditions.checkNotNull(projectName);
		Preconditions.checkNotNull(subscriptionName);
		return new PubSubSourceBuilder<>(deserializationSchema, projectName, subscriptionName);
	}

	/**
	 * Builder to create PubSubSource.
	 *
	 * @param <OUT>     The type of objects which will be read
	 */
	@SuppressWarnings("unchecked")
	public static class PubSubSourceBuilder<OUT> {
		private DeserializationSchema<OUT> deserializationSchema;
		private String projectName;
		private String subscriptionName;

		private PubSubSubscriberFactory pubSubSubscriberFactory;
		private Long maxMessagesReceivedNotProcessed;
		private Long maxBytesReceivedNotProcessed;
		private Credentials credentials;
		private String hostAndPort;

		protected PubSubSourceBuilder(DeserializationSchema<OUT> deserializationSchema, String projectName, String subscriptionName) {
			this.deserializationSchema = deserializationSchema;
			this.projectName = projectName;
			this.subscriptionName = subscriptionName;
		}

		/**
		 * Set the credentials.
		 * If this is not used then the credentials are picked up from the environment variables.
		 *
		 * @param credentials the Credentials needed to connect.
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withCredentials(Credentials credentials) {
			this.credentials = credentials;
			return this;
		}

		/**
		 * @param deserializationSchema Instance of a DeserializationSchema that converts the OUT into a byte[]
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
			this.deserializationSchema = deserializationSchema;
			return this;
		}

		/**
		 * @param projectName      The name of the project in GoogleCloudPlatform
		 * @param subscriptionName The name of the subscription in PubSub
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withProjectSubscriptionName(String projectName, String subscriptionName) {
			this.projectName = projectName;
			this.subscriptionName = subscriptionName;
			return this;
		}

		/**
		 * Set the custom hostname/port combination of PubSub.
		 * The ONLY reason to use this is during tests with the emulator provided by Google.
		 *
		 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withHostAndPort(String hostAndPort) {
			this.hostAndPort = hostAndPort;
			return this;
		}

		/**
		 * Set a PubSubSubscriberFactory
		 * This allows for custom Subscriber options to be set.
		 * Cannot be used in combination with withHostAndPortForEmulator().
		 *
		 * @param pubSubSubscriberFactory A factory to create a {@link Subscriber}
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withPubSubSubscriberFactory(PubSubSubscriberFactory pubSubSubscriberFactory) {
			this.pubSubSubscriberFactory = pubSubSubscriberFactory;
			return this;
		}

		/**
		 * Tune how many messages the connector will buffer when the Flink pipeline cannot keep up (backpressure).
		 *
		 * @param maxMessagesReceivedNotProcessed This indicates how many messages will be read and buffered until when the flink pipeline can't handle the messages fast enough.
		 * @param maxBytesReceivedNotProcessed This indicates how many bytes will be read and buffered. A good pick would be: maxBytesReceivedNotProcessed = maxMessagesReceivedNotProcessed * averageBytesSizePerMessage
		 */
		public PubSubSourceBuilder<OUT> withBackpressureParameters(long maxMessagesReceivedNotProcessed, long maxBytesReceivedNotProcessed) {
			this.maxMessagesReceivedNotProcessed = maxMessagesReceivedNotProcessed;
			this.maxBytesReceivedNotProcessed = maxBytesReceivedNotProcessed;
			return this;
		}

		/**
		 * Actually build the desired instance of the PubSubSourceBuilder.
		 *
		 * @return a brand new SourceFunction
		 * @throws IOException              in case of a problem getting the credentials
		 * @throws IllegalArgumentException in case required fields were not specified.
		 */
		public PubSubSource<OUT> build() throws IOException {
			if (credentials == null) {
				credentials = defaultCredentialsProviderBuilder().build().getCredentials();
			}

			if (pubSubSubscriberFactory != null && hostAndPort != null) {
				throw new IllegalArgumentException(("withPubSubSubscriberFactory() and withHostAndPortForEmulator() both called, only one may be called."));
			}

			if (pubSubSubscriberFactory != null && maxMessagesReceivedNotProcessed != null) {
				throw new IllegalArgumentException(("withPubSubSubscriberFactory() and withBackpressureParameters() both called, only one may be called."));
			}

			if (pubSubSubscriberFactory == null) {
				pubSubSubscriberFactory = new DefaultPubSubSubscriberFactory(hostAndPort, maxMessagesReceivedNotProcessed, maxBytesReceivedNotProcessed);
			}

			SubscriberWrapper subscriberWrapper = new SubscriberWrapper(credentials, ProjectSubscriptionName.of(projectName, subscriptionName), pubSubSubscriberFactory);
			return new PubSubSource<>(deserializationSchema, subscriberWrapper);
		}
	}
}
