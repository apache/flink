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
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.List;


/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them as soon as they have been received.
 */
public class PubSubSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, AckReplyConsumer> implements MessageReceiver, ResultTypeQueryable<OUT>, ParallelSourceFunction<OUT> {
	private DeserializationSchema<OUT> deserializationSchema;
	private SubscriberWrapper          subscriberWrapper;

	protected transient SourceContext<OUT> sourceContext = null;

	protected PubSubSource() {
		super(String.class);
	}

	protected void setDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	protected void setSubscriberWrapper(SubscriberWrapper subscriberWrapper) {
		this.subscriberWrapper = subscriberWrapper;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		subscriberWrapper.initialize(this);
		if (hasNoCheckpointingEnabled(getRuntimeContext())) {
			throw new IllegalArgumentException("Checkpointing needs to be enabled to support: PubSub ATLEAST_ONCE");
		}
	}

	private boolean hasNoCheckpointingEnabled(RuntimeContext runtimeContext) {
		return !(runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled());
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

	@SuppressWarnings("unchecked")
	public static <OUT> PubSubSourceBuilder<OUT, ? extends PubSubSource, ? extends PubSubSourceBuilder> newBuilder() {
		return new PubSubSourceBuilder<>(new PubSubSource<OUT>());
	}

	/**
	 * Builder to create PubSubSource.
	 * @param <OUT> The type of objects which will be read
	 * @param <PSS> The type of PubSubSource
	 * @param <BUILDER> The type of Builder to create the PubSubSource
	 */
	public static class PubSubSourceBuilder<OUT, PSS extends PubSubSource<OUT>, BUILDER extends PubSubSourceBuilder<OUT, PSS, BUILDER>> {
		protected PSS 							sourceUnderConstruction;

		private SubscriberWrapper               subscriberWrapper = null;
		private SerializableCredentialsProvider serializableCredentialsProvider;
		private DeserializationSchema<OUT>      deserializationSchema;
		private String                          projectName;
		private String                          subscriptionName;
		private String                          hostAndPort;

		protected PubSubSourceBuilder(PSS sourceUnderConstruction) {
			this.sourceUnderConstruction = sourceUnderConstruction;
		}

		/**
		 * Set the credentials.
		 * If this is not used then the credentials are picked up from the environment variables.
		 * @param credentials the Credentials needed to connect.
		 * @return The current PubSubSourceBuilder instance
		 */
		public BUILDER withCredentials(Credentials credentials) {
			this.serializableCredentialsProvider = new SerializableCredentialsProvider(credentials);
			return (BUILDER) this;
		}

		/**
		 * Set the CredentialsProvider.
		 * If this is not used then the credentials are picked up from the environment variables.
		 * @param credentialsProvider the custom SerializableCredentialsProvider instance.
		 * @return The current PubSubSourceBuilder instance
		 */
		public BUILDER withCredentialsProvider(CredentialsProvider credentialsProvider) throws IOException {
			return withCredentials(credentialsProvider.getCredentials());
		}

		/**
		 * Set the credentials to be absent.
		 * This means that no credentials are to be used at all.
		 * @return The current PubSubSourceBuilder instance
		 */
		public BUILDER withoutCredentials() {
			this.serializableCredentialsProvider = SerializableCredentialsProvider.withoutCredentials();
			return (BUILDER) this;
		}

		/**
		 * @param deserializationSchema Instance of a DeserializationSchema that converts the OUT into a byte[]
		 * @return The current PubSubSourceBuilder instance
		 */
		public BUILDER withDeserializationSchema(DeserializationSchema <OUT> deserializationSchema) {
			this.deserializationSchema = deserializationSchema;
			return (BUILDER) this;
		}

		/**
		 * @param projectName The name of the project in GoogleCloudPlatform
		 * @param subscriptionName The name of the subscription in PubSub
		 * @return The current PubSubSourceBuilder instance
		 */
		public BUILDER withProjectSubscriptionName(String projectName, String subscriptionName) {
			this.projectName = projectName;
			this.subscriptionName = subscriptionName;
			return (BUILDER) this;
		}

		/**
		 * Set the custom hostname/port combination of PubSub.
		 * The ONLY reason to use this is during tests with the emulator provided by Google.
		 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
		 * @return The current PubSubSourceBuilder instance
		 */
		public BUILDER withHostAndPort(String hostAndPort) {
			this.hostAndPort = hostAndPort;
			return (BUILDER) this;
		}

		/**
		 * Set a complete SubscriberWrapper.
		 * The ONLY reason to use this is during tests.
		 * @param subscriberWrapper The fully instantiated SubscriberWrapper
		 * @return The current PubSubSourceBuilder instance
		 */
		public BUILDER withSubscriberWrapper(SubscriberWrapper subscriberWrapper) {
			this.subscriberWrapper = subscriberWrapper;
			return (BUILDER) this;
		}

		/**
		 * Actually build the desired instance of the PubSubSourceBuilder.
		 * @return a brand new SourceFunction
		 * @throws IOException incase of a problem getting the credentials
		 * @throws IllegalArgumentException incase required fields were not specified.
		 */
		public PSS build() throws IOException {
			if (serializableCredentialsProvider == null) {
				serializableCredentialsProvider = SerializableCredentialsProvider.credentialsProviderFromEnvironmentVariables();
			}
			if (deserializationSchema == null) {
				throw new IllegalArgumentException("The deserializationSchema has not been specified.");
			}

			if (subscriberWrapper == null) {
				if (projectName == null || subscriptionName == null) {
					throw new IllegalArgumentException("The ProjectName And SubscriptionName have not been specified.");
				}

				subscriberWrapper =
					new SubscriberWrapper(serializableCredentialsProvider, ProjectSubscriptionName.of(projectName, subscriptionName));

				if (hostAndPort != null) {
					subscriberWrapper.withHostAndPort(hostAndPort);
				}
			}

			sourceUnderConstruction.setSubscriberWrapper(subscriberWrapper);
			sourceUnderConstruction.setDeserializationSchema(deserializationSchema);

			return sourceUnderConstruction;
		}
	}
}
