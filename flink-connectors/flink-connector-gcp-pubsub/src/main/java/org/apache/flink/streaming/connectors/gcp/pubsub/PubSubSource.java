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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeIdsForCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeOnCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.Acknowledger;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;
import static java.util.Collections.emptyList;

/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them on the next checkpoint.
 * This ensures every message will get acknowledged at least once.
 */
public class PubSubSource<OUT> extends RichSourceFunction<OUT>
	implements Acknowledger<String>, ResultTypeQueryable<OUT>, ParallelSourceFunction<OUT>, CheckpointListener, ListCheckpointed<AcknowledgeIdsForCheckpoint<String>> {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);
	protected final PubSubDeserializationSchema<OUT> deserializationSchema;
	protected final PubSubSubscriberFactory pubSubSubscriberFactory;
	protected final Credentials credentials;
	protected final String projectSubscriptionName;
	protected final int maxMessagesPerPull;
	protected final AcknowledgeOnCheckpointFactory acknowledgeOnCheckpointFactory;

	protected transient AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint;
	protected transient SubscriberStub subscriber;
	protected transient PullRequest pullRequest;
	protected transient EventLoopGroup eventLoopGroup;

	protected transient volatile boolean isRunning;
	protected transient volatile ApiFuture<PullResponse> messagesFuture;

	PubSubSource(PubSubDeserializationSchema<OUT> deserializationSchema, PubSubSubscriberFactory pubSubSubscriberFactory, Credentials credentials, String projectSubscriptionName, int maxMessagesPerPull, AcknowledgeOnCheckpointFactory acknowledgeOnCheckpointFactory) {
		this.deserializationSchema = deserializationSchema;
		this.pubSubSubscriberFactory = pubSubSubscriberFactory;
		this.credentials = credentials;
		this.projectSubscriptionName = projectSubscriptionName;
		this.maxMessagesPerPull = maxMessagesPerPull;
		this.acknowledgeOnCheckpointFactory = acknowledgeOnCheckpointFactory;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		if (hasNoCheckpointingEnabled(getRuntimeContext())) {
			throw new IllegalArgumentException("The PubSubSource REQUIRES Checkpointing to be enabled and " +
				"the checkpointing frequency must be MUCH lower than the PubSub timeout for it to retry a message.");
		}

		getRuntimeContext().getMetricGroup().gauge("PubSubMessagesProcessedNotAcked", this::getOutstandingMessagesToAck);

		createAndSetAcknowledgeOnCheckpoint();
		this.eventLoopGroup = new NioEventLoopGroup();
		this.subscriber = pubSubSubscriberFactory.getSubscriber(eventLoopGroup, credentials);
		this.isRunning = true;
		this.pullRequest = PullRequest.newBuilder()
			.setMaxMessages(maxMessagesPerPull)
			.setReturnImmediately(false)
			.setSubscription(projectSubscriptionName)
			.build();
	}

	private boolean hasNoCheckpointingEnabled(RuntimeContext runtimeContext) {
		return !(runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled());
	}

	@Override
	public void acknowledge(List<String> acknowledgementIds) {
		if (acknowledgementIds.isEmpty() || !isRunning) {
			return;
		}

		//grpc servers won't accept acknowledge requests that are too large so we split the ackIds
		Tuple2<List<String>, List<String>> splittedAckIds = splitAckIds(acknowledgementIds);
		while (!splittedAckIds.f0.isEmpty()) {
			AcknowledgeRequest acknowledgeRequest =
				AcknowledgeRequest.newBuilder()
					.setSubscription(projectSubscriptionName)
					.addAllAckIds(splittedAckIds.f0)
					.build();
			subscriber.acknowledgeCallable().call(acknowledgeRequest);

			splittedAckIds = splitAckIds(splittedAckIds.f1);
		}
	}

	/* maxPayload is the maximum number of bytes to devote to actual ids in
	 * acknowledgement or modifyAckDeadline requests. A serialized
	 * AcknowledgeRequest grpc call has a small constant overhead, plus the size of the
	 * subscription name, plus 3 bytes per ID (a tag byte and two size bytes). A
	 * ModifyAckDeadlineRequest has an additional few bytes for the deadline. We
	 * don't know the subscription name here, so we just assume the size exclusive
	 * of ids is 100 bytes.

	 * With gRPC there is no way for the client to know the server's max message size (it is
	 * configurable on the server). We know from experience that it is 512K.
	 * @return First list contains no more than 512k bytes, second list contains remaining ids
	 */
	private Tuple2<List<String>, List<String>> splitAckIds(List<String> ackIds) {
		final int maxPayload = 500 * 1024; //little below 512k bytes to be on the safe side
		final int fixedOverheadPerCall = 100;
		final int overheadPerId = 3;

		int totalBytes = fixedOverheadPerCall;

		for (int i = 0; i < ackIds.size(); i++) {
			totalBytes += ackIds.get(i).length() + overheadPerId;
			if (totalBytes > maxPayload) {
				return Tuple2.of(ackIds.subList(0, i), ackIds.subList(i, ackIds.size()));
			}
		}

		return Tuple2.of(ackIds, emptyList());
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) throws Exception {
		messagesFuture = subscriber.pullCallable().futureCall(pullRequest);
		while (isRunning) {
			try {
				List<ReceivedMessage> messages = messagesFuture.get().getReceivedMessagesList();

				// start the next pull while processing the current response.
				messagesFuture = subscriber.pullCallable().futureCall(pullRequest);

				processMessage(sourceContext, messages);
			} catch (InterruptedException | CancellationException e) {
				isRunning = false;
			}
		}
		shutdownSubscriber();
	}

	void processMessage(SourceContext<OUT> sourceContext, List<ReceivedMessage> messages) throws Exception {
		synchronized (sourceContext.getCheckpointLock()) {
			for (ReceivedMessage message : messages) {
				acknowledgeOnCheckpoint.addAcknowledgeId(message.getAckId());

				PubsubMessage pubsubMessage = message.getMessage();

				OUT deserializedMessage = deserializationSchema.deserialize(pubsubMessage);
				if (deserializationSchema.isEndOfStream(deserializedMessage)) {
					cancel();
					return;
				}

				sourceContext.collect(deserializedMessage);
			}

		}
	}

	private Integer getOutstandingMessagesToAck() {
		return acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements();
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deserializationSchema.getProducedType();
	}

	/*
	 * If we don't wait for the subscriber to terminate all background threads
	 * ClassNotFoundExceptions will be thrown when Flink starts unloading classes.
	 */
	private void shutdownSubscriber() {
		subscriber.shutdownNow();
		eventLoopGroup.shutdownGracefully();
		//Wait for the subscriber to terminate, to prevent leaking threads
		while (!subscriber.isTerminated() || !eventLoopGroup.isTerminated()) {
			try {
				subscriber.awaitTermination(60, TimeUnit.SECONDS);
				eventLoopGroup.awaitTermination(60, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.warn("Still waiting for subscriber to terminate.", e);
			}
		}
	}

	public static <OUT> PubSubSourceBuilder<OUT> newBuilder(PubSubDeserializationSchema<OUT> deserializationSchema, String projectName, String subscriptionName) {
		Preconditions.checkNotNull(deserializationSchema);
		Preconditions.checkNotNull(projectName);
		Preconditions.checkNotNull(subscriptionName);
		return new PubSubSourceBuilder<>(deserializationSchema, projectName, subscriptionName);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		acknowledgeOnCheckpoint.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public List<AcknowledgeIdsForCheckpoint<String>> snapshotState(long checkpointId, long timestamp) throws Exception {
		return acknowledgeOnCheckpoint.snapshotState(checkpointId, timestamp);
	}

	@Override
	public void restoreState(List<AcknowledgeIdsForCheckpoint<String>> state) throws Exception {
		createAndSetAcknowledgeOnCheckpoint();
		acknowledgeOnCheckpoint.restoreState(state);
	}

	private void createAndSetAcknowledgeOnCheckpoint() {
		if (acknowledgeOnCheckpoint == null) {
			acknowledgeOnCheckpoint = acknowledgeOnCheckpointFactory.create(this);
		}
	}

	/**
	 * Builder to create PubSubSource.
	 *
	 * @param <OUT> The type of objects which will be read
	 */
	public static class PubSubSourceBuilder<OUT> {
		private final PubSubDeserializationSchema<OUT> deserializationSchema;
		private final String projectName;
		private final String subscriptionName;

		private PubSubSubscriberFactory pubSubSubscriberFactory;
		private Credentials credentials;
		private int maxMessagesPerPull = 100;

		protected PubSubSourceBuilder(PubSubDeserializationSchema<OUT> deserializationSchema, String projectName, String subscriptionName) {
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
		 * @throws IOException              in case of a problem getting the credentials
		 * @throws IllegalArgumentException in case required fields were not specified.
		 */
		public PubSubSource<OUT> build() throws IOException {
			if (credentials == null) {
				credentials = defaultCredentialsProviderBuilder().build().getCredentials();
			}

			if (pubSubSubscriberFactory == null) {
				pubSubSubscriberFactory = new DefaultPubSubSubscriberFactory();
			}

			return new PubSubSource<>(deserializationSchema, pubSubSubscriberFactory, credentials, ProjectSubscriptionName.format(projectName, subscriptionName), maxMessagesPerPull, new AcknowledgeOnCheckpointFactory());
		}
	}

	static class AcknowledgeOnCheckpointFactory implements Serializable {
		AcknowledgeOnCheckpoint<String> create(Acknowledger<String> acknowledger) {
			return new AcknowledgeOnCheckpoint<>(acknowledger);
		}
	}
}
