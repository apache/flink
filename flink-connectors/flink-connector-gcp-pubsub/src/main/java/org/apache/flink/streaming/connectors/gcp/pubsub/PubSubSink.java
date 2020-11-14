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

import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentials;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentialsProvider;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannel;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
	private final AtomicInteger numPendingFutures;
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
		this.numPendingFutures = new AtomicInteger(0);
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
		serializationSchema.open(RuntimeContextInitializationContextAdapters.serializationAdapter(
				getRuntimeContext(),
				metricGroup -> metricGroup.addGroup("user")
		));

		Publisher.Builder builder = Publisher
			.newBuilder(TopicName.of(projectName, topicName))
			.setCredentialsProvider(FixedCredentialsProvider.create(credentials));

		// Having the host and port for the emulator means we are in a testing scenario.
		if (hostAndPortForEmulator != null) {
			managedChannel = ManagedChannelBuilder
				.forTarget(hostAndPortForEmulator)
				.usePlaintext() // This is 'Ok' because this is ONLY used for testing.
				.build();
			channel = GrpcTransportChannel.newBuilder().setManagedChannel(managedChannel).build();
			builder.setChannelProvider(FixedTransportChannelProvider.create(channel))
					.setCredentialsProvider(EmulatorCredentialsProvider.create())
					// In test scenarios we are limiting the Retry settings.
					// The values here are based on the default settings with lower attempts and timeouts.
					.setRetrySettings(
						RetrySettings.newBuilder()
							.setMaxAttempts(10)
							.setTotalTimeout(Duration.ofSeconds(10))
							.setInitialRetryDelay(Duration.ofMillis(100))
							.setRetryDelayMultiplier(1.3)
							.setMaxRetryDelay(Duration.ofSeconds(5))
							.setInitialRpcTimeout(Duration.ofSeconds(5))
							.setRpcTimeoutMultiplier(1)
							.setMaxRpcTimeout(Duration.ofSeconds(10))
							.build());
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
	public void invoke(IN message, SinkFunction.Context context) {
		PubsubMessage pubsubMessage = PubsubMessage
			.newBuilder()
			.setData(ByteString.copyFrom(serializationSchema.serialize(message)))
			.build();

		ApiFuture<String> future = publisher.publish(pubsubMessage);
		numPendingFutures.incrementAndGet();
		ApiFutures.addCallback(future, failureHandler, directExecutor());
	}

	/**
	 * Create a builder for a new PubSubSink.
	 *
	 * @return a new PubSubSinkBuilder instance
	 */
	public static SerializationSchemaBuilder newBuilder() {
		return new SerializationSchemaBuilder();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		//before checkpoints make sure all the batched / buffered pubsub messages have actually been sent
		publisher.publishAllOutstanding();

		// At this point, no new messages will be published because this thread has successfully acquired
		// the checkpoint lock. So we just wait for all the pending futures to complete.
		waitForFuturesToComplete();
		if (exceptionAtomicReference.get() != null) {
			throw exceptionAtomicReference.get();
		}
	}

	private void waitForFuturesToComplete() {
		// We have to synchronize on numPendingFutures here to ensure the notification won't be missed.
		synchronized (numPendingFutures) {
			while (isRunning && numPendingFutures.get() > 0) {
				try {
					numPendingFutures.wait();
				} catch (InterruptedException e) {
					// Simply cache the interrupted exception. Supposedly the thread will exit the loop
					// gracefully when it checks the isRunning flag.
					LOG.info("Interrupted when waiting for futures to complete");
				}
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
	}

	/**
	 * PubSubSinkBuilder to create a PubSubSink.
	 *
	 * @param <IN> Type of PubSubSink to create.
	 */
	public static class PubSubSinkBuilder<IN> implements ProjectNameBuilder<IN>, TopicNameBuilder<IN> {
		private SerializationSchema<IN> serializationSchema;
		private String projectName;
		private String topicName;

		private Credentials credentials;
		private String hostAndPort;

		private PubSubSinkBuilder(SerializationSchema<IN> serializationSchema) {
			this.serializationSchema = serializationSchema;
		}

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
				if (hostAndPort == null) {
					// No hostAndPort is the normal scenario so we use the default credentials.
					credentials = defaultCredentialsProviderBuilder().build().getCredentials();
				} else {
					// With hostAndPort the PubSub emulator is used so we do not have credentials.
					credentials = EmulatorCredentials.getInstance();
				}
			}
			return new PubSubSink<>(credentials, serializationSchema, projectName, topicName, hostAndPort);
		}
	}

	/**
	 * Part of {@link PubSubSinkBuilder} to set required fields.
	 */
	public static class SerializationSchemaBuilder {
		/**
		 * Set the SerializationSchema used to Serialize objects to be added as payloads of PubSubMessages.
		 */
		public <IN> ProjectNameBuilder<IN> withSerializationSchema(SerializationSchema<IN> deserializationSchema) {
			return new PubSubSinkBuilder<>(deserializationSchema);
		}
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
			ackAndMaybeNotifyNoPendingFutures();
			exceptionAtomicReference.set(new RuntimeException("Failed trying to publish message", t));
		}

		@Override
		public void onSuccess(String result) {
			ackAndMaybeNotifyNoPendingFutures();
			LOG.debug("Successfully published message with id: {}", result);
		}

		private void ackAndMaybeNotifyNoPendingFutures() {
			// When there are no pending futures anymore, notify the thread that is waiting for
			// all the pending futures to be completed.
			if (numPendingFutures.decrementAndGet() == 0) {
				synchronized (numPendingFutures) {
					numPendingFutures.notify();
				}
			}
		}
	}
}
