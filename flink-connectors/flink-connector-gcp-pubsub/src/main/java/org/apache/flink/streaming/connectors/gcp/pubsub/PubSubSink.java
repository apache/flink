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
import java.util.concurrent.TimeUnit;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;
import static org.apache.flink.runtime.concurrent.Executors.directExecutor;

/**
 * A sink function that outputs to PubSub.
 *
 * @param <IN> type of PubSubSink messages to write
 */
public class PubSubSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

	private final ApiFutureCallback<String> failureHandler;
	private final Credentials credentials;
	private final SerializationSchema<IN> serializationSchema;
	private final String projectName;
	private final String topicName;
	private final String hostAndPortForEmulator;

	private transient Publisher publisher;

	private PubSubSink(
		Credentials credentials,
		SerializationSchema<IN> serializationSchema,
		String projectName,
		String topicName,
		String hostAndPortForEmulator) {
		this.failureHandler = new FailureHandler();
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
	}

	@Override
	public void close() throws Exception {
		super.close();
		shutdownPublisher();
		shutdownTransportChannel();
		shutdownManagedChannel();
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
		ApiFutures.addCallback(publisher.publish(pubsubMessage), failureHandler, directExecutor());
	}

	/**
	 * Create a builder for a new PubSubSink.
	 *
	 * @param <IN> The generic of the type that is to be written into the sink.
	 * @return a new PubSubSinkBuilder instance
	 */
	public static <IN> PubSubSinkBuilder<IN> newBuilder(SerializationSchema<IN> serializationSchema, String projectName, String topicName) {
		return new PubSubSinkBuilder<>(serializationSchema, projectName, topicName);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		//before checkpoints make sure all the batched / buffered pubsub messages have actually been sent
		publisher.publishAllOutstanding();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	/**
	 * PubSubSinkBuilder to create a PubSubSink.
	 *
	 * @param <IN> Type of PubSubSink to create.
	 */
	@SuppressWarnings("unchecked")
	public static class PubSubSinkBuilder<IN> {
		private SerializationSchema<IN> serializationSchema;
		private String projectName;
		private String topicName;

		private Credentials credentials;
		private String hostAndPort;

		private PubSubSinkBuilder(SerializationSchema<IN> serializationSchema, String projectName, String topicName) {
			this.serializationSchema = serializationSchema;
			this.projectName = projectName;
			this.topicName = topicName;
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

		/**
		 * @param serializationSchema Instance of a SerializationSchema that converts the IN into a byte[]
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withSerializationSchema(SerializationSchema<IN> serializationSchema) {
			this.serializationSchema = serializationSchema;
			return this;
		}

		/**
		 * @param projectName The name of the project in PubSub
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withProjectName(String projectName) {
			this.projectName = projectName;
			return this;
		}

		/**
		 * @param topicName The name of the topic in PubSub
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withTopicName(String topicName) {
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

	private class FailureHandler implements ApiFutureCallback<String> {
		@Override
		public void onFailure(Throwable t) {
			throw new RuntimeException("Failed trying to publish message", t);
		}

		@Override
		public void onSuccess(String result) {
			//do nothing on success
			LOG.debug("Successfully published message with id: {}", result);
		}
	}
}
