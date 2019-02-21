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
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;

/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them on the next checkpoint.
 * This ensures every message will get acknowledged at least once.
 */
public class PubSubSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, String>
		implements ResultTypeQueryable<OUT>, ParallelSourceFunction<OUT>, StoppableFunction {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);
	protected final DeserializationSchema<OUT> deserializationSchema;
	protected final PubSubSubscriberFactory pubSubSubscriberFactory;
	protected final Credentials credentials;
	protected final String projectSubscriptionName;
	protected final int maxMessagesPerPull;

	protected transient boolean deduplicateMessages;
	protected transient SubscriberStub subscriber;
	protected transient boolean isRunning;
	protected transient PullRequest pullRequest;

	PubSubSource(DeserializationSchema<OUT> deserializationSchema, PubSubSubscriberFactory pubSubSubscriberFactory, Credentials credentials, String projectSubscriptionName, int maxMessagesPerPull) {
		super(String.class);
		this.deserializationSchema = deserializationSchema;
		this.pubSubSubscriberFactory = pubSubSubscriberFactory;
		this.credentials = credentials;
		this.projectSubscriptionName = projectSubscriptionName;
		this.maxMessagesPerPull = maxMessagesPerPull;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		if (hasNoCheckpointingEnabled(getRuntimeContext())) {
			throw new IllegalArgumentException("The PubSubSource REQUIRES Checkpointing to be enabled and " +
				"the checkpointing frequency must be MUCH lower than the PubSub timeout for it to retry a message.");
		}

		getRuntimeContext().getMetricGroup().gauge("PubSubMessagesProcessedNotAcked", this::getOutstandingMessagesToAck);

		this.subscriber = pubSubSubscriberFactory.getSubscriber(credentials);
		this.deduplicateMessages = getRuntimeContext().getNumberOfParallelSubtasks() == 1;
		this.isRunning = true;
		this.pullRequest = PullRequest.newBuilder()
										.setMaxMessages(100)
										.setReturnImmediately(false)
										.setSubscription(projectSubscriptionName)
										.build();
	}

	private boolean hasNoCheckpointingEnabled(RuntimeContext runtimeContext) {
		return !(runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled());
	}

	@Override
	protected void acknowledgeSessionIDs(List<String> acknowledgementIds) {
		if (acknowledgementIds.isEmpty()) {
			return;
		}

		AcknowledgeRequest acknowledgeRequest =
			AcknowledgeRequest.newBuilder()
								.setSubscription(projectSubscriptionName)
								.addAllAckIds(acknowledgementIds)
								.build();
		subscriber.acknowledgeCallable().call(acknowledgeRequest);
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) throws Exception {
		while (isRunning) {
				List<ReceivedMessage> messages = subscriber.pullCallable().call(pullRequest).getReceivedMessagesList();
				processMessage(sourceContext, messages);
		}
	}

	void processMessage(SourceContext<OUT> sourceContext, List<ReceivedMessage> messages) throws IOException {
		synchronized (sourceContext.getCheckpointLock()) {
			for (ReceivedMessage message : messages) {
				sessionIds.add(message.getAckId());

				PubsubMessage pubsubMessage = message.getMessage();
				if (deduplicateMessages && !addId(pubsubMessage.getMessageId())) {
					// message is duplicate so just ignore it
					return;
				}

				OUT deserializedMessage = deserializeMessage(pubsubMessage);
				if (deserializationSchema.isEndOfStream(deserializedMessage)) {
					stop();
					return;
				}

				sourceContext.collect(deserializedMessage);
			}

		}
	}

	private Integer getOutstandingMessagesToAck() {
		return this.sessionIdsPerSnapshot
				.stream()
				.mapToInt(tuple -> tuple.f1.size())
				.sum() + this.sessionIds.size();
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void stop() {
		isRunning = false;
	}

	private OUT deserializeMessage(PubsubMessage message) throws IOException {
		return deserializationSchema.deserialize(message.getData().toByteArray());
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
	 * @param <OUT> The type of objects which will be read
	 */
	public static class PubSubSourceBuilder<OUT> {
		private final DeserializationSchema<OUT> deserializationSchema;
		private final String projectName;
		private final String subscriptionName;

		private PubSubSubscriberFactory pubSubSubscriberFactory;
		private Credentials credentials;
		private int maxMessagesPerPull = 100;

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
		 * Set a PubSubSubscriberFactory
		 * This allows for custom Subscriber options to be set.
		 * {@link DefaultPubSubSubscriberFactory} is the default.
		 *
		 * @param pubSubSubscriberFactory A factory to create a {@link Subscriber}
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withPubSubSubscriberFactory(PubSubSubscriberFactory pubSubSubscriberFactory) {
			this.pubSubSubscriberFactory = pubSubSubscriberFactory;
			return this;
		}

		/**
		 * The source function uses Grpc calls to get new messages.
		 * This parameter limited the maximum amount of messages to retrieve per pull. Default value is 100.
		 *
		 * @param maxMessagesPerPull
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withMaxMessagesPerPull(int maxMessagesPerPull) {
			this.maxMessagesPerPull = maxMessagesPerPull;
			return this;
		}

		/**
		 * Actually build the desired instance of the PubSubSourceBuilder.
		 *
		 * @return a brand new SourceFunction
		 * @throws IOException	          in case of a problem getting the credentials
		 * @throws IllegalArgumentException in case required fields were not specified.
		 */
		public PubSubSource<OUT> build() throws IOException {
			if (credentials == null) {
				credentials = defaultCredentialsProviderBuilder().build().getCredentials();
			}

			if (pubSubSubscriberFactory == null) {
				pubSubSubscriberFactory = new DefaultPubSubSubscriberFactory();
			}

			return new PubSubSource<>(deserializationSchema, pubSubSubscriberFactory, credentials, ProjectSubscriptionName.format(projectName, subscriptionName), maxMessagesPerPull);
		}
	}
}
