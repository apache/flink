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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.api.gax.core.CredentialsProvider;
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

/**
 * A sink function that outputs to PubSub.
 *
 * @param <IN> type of PubSubSink messages to write
 */
public class PubSubSink<IN> extends RichSinkFunction<IN> {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

	private SerializableCredentialsProvider serializableCredentialsProvider;
	private SerializationSchema<IN> serializationSchema;
	private String projectName;
	private String topicName;
	private String hostAndPort = null;

	private transient Publisher publisher;

	private PubSubSink() {
	}

	void setSerializableCredentialsProvider(SerializableCredentialsProvider serializableCredentialsProvider) {
		this.serializableCredentialsProvider = serializableCredentialsProvider;
	}

	void setSerializationSchema(SerializationSchema<IN> serializationSchema) {
		this.serializationSchema = serializationSchema;
	}

	void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	/**
	 * Set the custom hostname/port combination of PubSub.
	 * The ONLY reason to use this is during tests with the emulator provided by Google.
	 *
	 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
	 */
	void withHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}

	void initialize() throws IOException {
		if (serializableCredentialsProvider == null) {
			serializableCredentialsProvider = SerializableCredentialsProvider.credentialsProviderFromEnvironmentVariables();
		}
		if (serializationSchema == null) {
			throw new IllegalArgumentException("The serializationSchema has not been specified.");
		}
		if (projectName == null) {
			throw new IllegalArgumentException("The projectName has not been specified.");
		}
		if (topicName == null) {
			throw new IllegalArgumentException("The topicName has not been specified.");
		}
	}


	private transient ManagedChannel managedChannel = null;
	private transient TransportChannel channel = null;

	@Override
	public void open(Configuration configuration) throws Exception {
		Publisher.Builder builder = Publisher
			.newBuilder(ProjectTopicName.of(projectName, topicName))
			.setCredentialsProvider(serializableCredentialsProvider);

		if (hostAndPort != null) {
			managedChannel = ManagedChannelBuilder
				.forTarget(hostAndPort)
				.usePlaintext(true) // This is 'Ok' because this is ONLY used for testing.
				.build();
			channel = GrpcTransportChannel.newBuilder().setManagedChannel(managedChannel).build();
			builder.setChannelProvider(FixedTransportChannelProvider.create(channel));
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
			publisher.shutdown();
		} catch (Exception e) {
			LOG.info("Shutting down Publisher failed: " + e.getMessage());
		}
	}

	private void shutdownTransportChannel() {
		if (channel == null) {
			return;
		}
		try {
			channel.close();
		} catch (Exception e) {
			LOG.info("Shutting down TransportChannel failed: " + e.getMessage());
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
			LOG.info("Shutting down ManagedChannel failed: " + e.getMessage());
		}
	}

	@Override
	public void invoke(IN message, SinkFunction.Context context) {
		PubsubMessage pubsubMessage = PubsubMessage
			.newBuilder()
			.setData(ByteString.copyFrom(serializationSchema.serialize(message)))
			.build();
		publisher.publish(pubsubMessage);
		publisher.publishAllOutstanding();
	}

	/**
	 * Create a builder for a new PubSubSink.
	 *
	 * @param <IN> The generic of the type that is to be written into the sink.
	 * @return a new PubSubSinkBuilder instance
	 */
	public static <IN> PubSubSinkBuilder<IN, ? extends PubSubSink<IN>, ? extends PubSubSinkBuilder<IN, ?, ?>> newBuilder() {
		return new PubSubSinkBuilder<>(new PubSubSink<>());
	}

	/**
	 * PubSubSinkBuilder to create a PubSubSink.
	 *
	 * @param <IN> Type of PubSubSink to create.
	 */
	@SuppressWarnings("unchecked")
	public static class PubSubSinkBuilder<IN, PSS extends PubSubSink<IN>, BUILDER extends PubSubSinkBuilder<IN, PSS, BUILDER>> {
		protected PSS sinkUnderConstruction;

		private PubSubSinkBuilder(PSS sinkUnderConstruction) {
			this.sinkUnderConstruction = sinkUnderConstruction;
		}

		/**
		 * Set the credentials.
		 * If this is not used then the credentials are picked up from the environment variables.
		 *
		 * @param credentials the Credentials needed to connect.
		 * @return The current PubSubSinkBuilder instance
		 */
		public BUILDER withCredentials(Credentials credentials) {
			sinkUnderConstruction.setSerializableCredentialsProvider(new SerializableCredentialsProvider(credentials));
			return (BUILDER) this;
		}

		/**
		 * Set the CredentialsProvider.
		 * If this is not used then the credentials are picked up from the environment variables.
		 *
		 * @param credentialsProvider the custom SerializableCredentialsProvider instance.
		 * @return The current PubSubSinkBuilder instance
		 */
		public BUILDER withCredentialsProvider(CredentialsProvider credentialsProvider) throws IOException {
			return withCredentials(credentialsProvider.getCredentials());
		}

		/**
		 * Set the credentials to be absent.
		 * This means that no credentials are to be used at all.
		 *
		 * @return The current PubSubSinkBuilder instance
		 */
		public BUILDER withoutCredentials() {
			sinkUnderConstruction.setSerializableCredentialsProvider(SerializableCredentialsProvider.withoutCredentials());
			return (BUILDER) this;
		}

		/**
		 * @param serializationSchema Instance of a SerializationSchema that converts the IN into a byte[]
		 * @return The current PubSubSinkBuilder instance
		 */
		public BUILDER withSerializationSchema(SerializationSchema<IN> serializationSchema) {
			sinkUnderConstruction.setSerializationSchema(serializationSchema);
			return (BUILDER) this;
		}

		/**
		 * @param projectName The name of the project in PubSub
		 * @return The current PubSubSinkBuilder instance
		 */
		public BUILDER withProjectName(String projectName) {
			sinkUnderConstruction.setProjectName(projectName);
			return (BUILDER) this;
		}

		/**
		 * @param topicName The name of the topic in PubSub
		 * @return The current PubSubSinkBuilder instance
		 */
		public BUILDER withTopicName(String topicName) {
			sinkUnderConstruction.setTopicName(topicName);
			return (BUILDER) this;
		}

		/**
		 * Set the custom hostname/port combination of PubSub.
		 * The ONLY reason to use this is during tests with the emulator provided by Google.
		 *
		 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
		 * @return The current PubSubSinkBuilder instance
		 */
		public BUILDER withHostAndPort(String hostAndPort) {
			sinkUnderConstruction.withHostAndPort(hostAndPort);
			return (BUILDER) this;
		}

		/**
		 * Actually builder the desired instance of the PubSubSink.
		 *
		 * @return a brand new PubSubSink
		 * @throws IOException              incase of a problem getting the credentials
		 * @throws IllegalArgumentException incase required fields were not specified.
		 */
		public PubSubSink<IN> build() throws IOException {
			sinkUnderConstruction.initialize();
			return sinkUnderConstruction;
		}
	}

}
