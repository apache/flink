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

import com.google.api.gax.core.NoCredentialsProvider;
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

import java.io.IOException;

/**
 * A sink function that outputs to PubSub.
 * @param <IN> type of PubSubSink messages to write
 */
public class PubSubSink<IN> extends RichSinkFunction<IN> {

	private final SerializableCredentialsProvider serializableCredentialsProvider;
	private final SerializationSchema<IN>         serializationSchema;
	private final String                          projectName;
	private final String                          topicName;
	private       String                          hostAndPort = null;

	private transient Publisher publisher;

	public PubSubSink(SerializableCredentialsProvider serializableCredentialsProvider, SerializationSchema<IN> serializationSchema, String projectName, String topicName) {
		this.serializableCredentialsProvider = serializableCredentialsProvider;
		this.serializationSchema = serializationSchema;
		this.projectName = projectName;
		this.topicName = topicName;
	}

	/**
	 * Set the custom hostname/port combination of PubSub.
	 * The ONLY reason to use this is during tests with the emulator provided by Google.
	 *
	 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
	 * @return The current instance
	 */
	public PubSubSink<IN> withHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
		return this;
	}

	private transient ManagedChannel   managedChannel = null;
	private transient TransportChannel channel        = null;

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
		publisher.shutdown();
		if (channel != null) {
			channel.close();
			managedChannel.shutdownNow();
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
	 * @param <IN> The generic of the type that is to be written into the sink.
	 * @return a new PubSubSinkBuilder instance
	 */
	public static <IN> PubSubSinkBuilder<IN> newBuilder() {
		return new PubSubSinkBuilder<>();
	}

	/**
	 * PubSubSinkBuilder to create a PubSubSink.
	 * @param <IN> Type of PubSubSink to create.
	 */
	public static class PubSubSinkBuilder<IN> {
		private SerializableCredentialsProvider serializableCredentialsProvider = null;
		private SerializationSchema<IN>         serializationSchema             = null;
		private String                          projectName                     = null;
		private String                          topicName                       = null;
		private String                          hostAndPort                     = null;

		private PubSubSinkBuilder() {
		}

		/**
		 * Set the credentials.
		 * If this is not used then the credentials are picked up from the environment variables.
		 * @param credentials the Credentials needed to connect.
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withCredentials(Credentials credentials) {
			this.serializableCredentialsProvider = new SerializableCredentialsProvider(credentials);
			return this;
		}

		/**
		 * Set the CredentialsProvider.
		 * If this is not used then the credentials are picked up from the environment variables.
		 * @param credentialsProvider the custom SerializableCredentialsProvider instance.
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withCredentialsProvider(CredentialsProvider credentialsProvider) throws IOException {
			return withCredentials(credentialsProvider.getCredentials());
		}

		/**
		 * Set the credentials to be absent.
		 * This means that no credentials are to be used at all.
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withoutCredentials() {
			this.serializableCredentialsProvider = SerializableCredentialsProvider.withoutCredentials();
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
		public PubSubSinkBuilder<IN> withProjectName (String projectName) {
			this.projectName = projectName;
			return this;
		}

		/**
		 * @param topicName The name of the topic in PubSub
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withTopicName (String topicName) {
			this.topicName = topicName;
			return this;
		}


		/**
		 * Set the custom hostname/port combination of PubSub.
		 * The ONLY reason to use this is during tests with the emulator provided by Google.
		 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
		 * @return The current PubSubSinkBuilder instance
		 */
		public PubSubSinkBuilder<IN> withHostAndPort(String hostAndPort) {
			this.hostAndPort = hostAndPort;
			return this;
		}

		/**
		 * Actually builder the desired instance of the PubSubSink.
		 * @return a brand new PubSubSink
		 * @throws IOException incase of a problem getting the credentials
		 * @throws IllegalArgumentException incase required fields were not specified.
		 */
		public PubSubSink<IN> build() throws IOException {
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

			PubSubSink<IN> pubSubSink = new PubSubSink<>(
				serializableCredentialsProvider,
				serializationSchema,
				projectName, topicName);

			if (hostAndPort != null) {
				pubSubSink.withHostAndPort(hostAndPort);
			}

			return pubSubSink;
		}
	}

}
