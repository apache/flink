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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannel;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;
import static org.apache.flink.runtime.concurrent.Executors.directExecutor;

/**
 * A sink function that outputs to PubSub.
 *
 * @param <IN> type of PubSubSink messages to write
 */
public class PubSubSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

	private final AtomicReference<Exception> exceptionAtomicReference;
	private final ApiFutureCallback<String> failureHandler;
	private final ConcurrentLinkedQueue<ApiFuture<String>> outstandingFutures;
	private final Credentials credentials;
	private final SerializationSchema<IN> serializationSchema;
	private final String projectName;
	private final String topicName;
	private final String hostAndPortForEmulator;

	private transient Publisher publisher;
	private volatile boolean isRunning;

	private PubSubSink(
		Credentials credentials,
		SerializationSchema<IN> serializationSchema,
		String projectName,
		String topicName,
		String hostAndPortForEmulator) {
		this.exceptionAtomicReference = new AtomicReference<>();
		this.failureHandler = new FailureHandler();
		this.outstandingFutures = new ConcurrentLinkedQueue<>();
		this.credentials = credentials;
		this.serializationSchema = serializationSchema;
		this.projectName = projectName;
		this.topicName = topicName;
		this.hostAndPortForEmulator = hostAndPortForEmulator;
	}

	private transient ManagedChannel managedChannel = null;
	private transient TransportChannel channel = null;

	@Override
	public void open(Configuration configuration) throws Exception {
		Publisher.Builder builder = Publisher
			.newBuilder(ProjectTopicName.of(projectName, topicName))
			.setCredentialsProvider(FixedCredentialsProvider.create(credentials));

		if (hostAndPortForEmulator != null) {
			managedChannel = ManagedChannelBuilder
				.forTarget(hostAndPortForEmulator)
				.usePlaintext(true) // This is 'Ok' because this is ONLY used for testing.
				.build();
			channel = GrpcTransportChannel.newBuilder().setManagedChannel(managedChannel).build();
			builder.setChannelProvider(FixedTransportChannelProvider.create(channel))
					.setCredentialsProvider(NoCredentialsProvider.create());
		}

		publisher = builder.build();
		isRunning = true;
	}

	@Override
	public void close() throws Exception {
		super.close();
		shutdownPublisher();
		shutdownTransportChannel();
		shutdownManagedChannel();
		isRunning = false;
	}

	private void shutdownPublisher() {
		try {
			if (publisher != null) {
				publisher.shutdown();
			}
		} catch (Exception e) {
			LOG.info("Shutting down Publisher failed.", e);
		}
	}

	private void shutdownTransportChannel() {
		if (channel == null) {
			return;
		}
		try {
			channel.close();
		} catch (Exception e) {
			LOG.info("Shutting down TransportChannel failed.", e);
		}
	}

	private void shutdownManagedChannel() {
		if (managedChannel == null) {
			return;
		}
		try {
			managedChannel.shutdownNow();
			managedChannel.awaitTermination(1000L, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			LOG.info("Shutting down ManagedChannel failed.", e);
		}
	}

	@Override
	public void invoke(IN message, SinkFunction.Context context) throws Exception {
		PubsubMessage pubsubMessage = PubsubMessage
			.newBuilder()
			.setData(ByteString.copyFrom(serializationSchema.serialize(message)))
			.build();

		ApiFuture<String> future = publisher.publish(pubsubMessage);
		outstandingFutures.add(future);
		ApiFutures.addCallback(future, failureHandler, directExecutor());
	}

	/**
	 * Create a builder for a new PubSubSink.
	 *
	 * @param <IN> The generic of the type that is to be written into the sink.
	 * @return a new PubSubSinkBuilder instance
	 */
	public static <IN> SerializationSchemaBuilder<IN> newBuilder(Class<IN> clazz) {
		return new PubSubSinkBuilder<>();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		//before checkpoints make sure all the batched / buffered pubsub messages have actually been sent
		publisher.publishAllOutstanding();

		waitForFuturesToComplete();
		if (exceptionAtomicReference.get() != null) {
			throw exceptionAtomicReference.get();
		}
	}

	private void waitForFuturesToComplete() {
		while (isRunning && !outstandingFutures.isEmpty()) {
			outstandingFutures.removeIf(ApiFuture::isDone);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	/**
	 * PubSubSinkBuilder to create a PubSubSink.
	 *
	 * @param <IN> Type of PubSubSink to create.
	 */
	public static class PubSubSinkBuilder<IN> implements SerializationSchemaBuilder<IN>, ProjectNameBuilder<IN>, TopicNameBuilder<IN> {
		private SerializationSchema<IN> serializationSchema;
		private String projectName;
		private String topicName;

		private Credentials credentials;
		private String hostAndPort;

		private PubSubSinkBuilder() { }

		/**
		 * Set the credentials.
		 * If this is not used then the credentials are picked up from the environment variables.
		 *
		 * @param credentials the Credentials needed to connect.
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withCredentials(Credentials credentials) {
			this.credentials = credentials;
			return this;
		}

		@Override
		public ProjectNameBuilder<IN> withSerializationSchema(SerializationSchema<IN> serializationSchema) {
			Preconditions.checkNotNull(serializationSchema);
			this.serializationSchema = serializationSchema;
			return this;
		}

		@Override
		public TopicNameBuilder<IN> withProjectName(String projectName) {
			Preconditions.checkNotNull(projectName);
			this.projectName = projectName;
			return this;
		}

		@Override
		public PubSubSinkBuilder<IN> withTopicName(String topicName) {
			Preconditions.checkNotNull(topicName);
			this.topicName = topicName;
			return this;
		}

		/**
		 * Set the custom hostname/port combination of PubSub.
		 * The ONLY reason to use this is during tests with the emulator provided by Google.
		 *
		 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withHostAndPortForEmulator(String hostAndPort) {
			this.hostAndPort = hostAndPort;
			return this;
		}

		/**
		 * Actually builder the desired instance of the PubSubSink.
		 *
		 * @return a brand new PubSubSink
		 * @throws IOException              in case of a problem getting the credentials
		 * @throws IllegalArgumentException in case required fields were not specified.
		 */
		public PubSubSink<IN> build() throws IOException {
			if (credentials == null) {
				credentials = defaultCredentialsProviderBuilder().build().getCredentials();
			}
			return new PubSubSink<>(credentials, serializationSchema, projectName, topicName, hostAndPort);
		}
	}

	/**
	 * Part of {@link PubSubSinkBuilder} to set required fields.
	 */
	public interface SerializationSchemaBuilder<IN> {
		/**
		 * Set the SerializationSchema used to Serialize objects to be added as payloads of PubSubMessages.
		 */
		ProjectNameBuilder<IN> withSerializationSchema(SerializationSchema<IN> deserializationSchema);
	}

	/**
	 * Part of {@link PubSubSinkBuilder} to set required fields.
	 */
	public interface ProjectNameBuilder<IN> {
		/**
		 * Set the project name of the subscription to pull messages from.
		 */
		TopicNameBuilder<IN> withProjectName(String projectName);
	}

	/**
	 * Part of {@link PubSubSinkBuilder} to set required fields.
	 */
	public interface TopicNameBuilder<IN> {
		/**
		 * Set the subscription name of the subscription to pull messages from.
		 */
		PubSubSinkBuilder<IN> withTopicName(String topicName);
	}

	private class FailureHandler implements ApiFutureCallback<String>, Serializable {
		@Override
		public void onFailure(Throwable t) {
			exceptionAtomicReference.set(new RuntimeException("Failed trying to publish message", t));
		}

		@Override
		public void onSuccess(String result) {
			//do nothing on success
			LOG.debug("Successfully published message with id: {}", result);
		}
	}
}
