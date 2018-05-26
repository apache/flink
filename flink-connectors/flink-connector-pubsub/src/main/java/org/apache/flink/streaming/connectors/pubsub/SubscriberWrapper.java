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

import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.api.core.ApiService;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannel;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Serializable;

class SubscriberWrapper implements Serializable {
	private final SerializableCredentialsProvider serializableCredentialsProvider;
	private final String                          projectId;
	private final String                          subscriptionId;
	private       String                          hostAndPort = null;

	private transient Subscriber       subscriber;
	private transient ManagedChannel   managedChannel = null;
	private transient TransportChannel channel        = null;

	SubscriberWrapper(SerializableCredentialsProvider serializableCredentialsProvider, ProjectSubscriptionName projectSubscriptionName) {
		this.serializableCredentialsProvider = serializableCredentialsProvider;
		this.projectId = projectSubscriptionName.getProject();
		this.subscriptionId = projectSubscriptionName.getSubscription();
	}

	void initialize(MessageReceiver messageReceiver) {
		Subscriber.Builder builder = Subscriber
				.newBuilder(ProjectSubscriptionName.of(projectId, subscriptionId), messageReceiver)
				.setCredentialsProvider(serializableCredentialsProvider);

		if (hostAndPort != null) {
			managedChannel = ManagedChannelBuilder
					.forTarget(hostAndPort)
					.usePlaintext(true) // This is 'Ok' because this is ONLY used for testing.
					.build();
			channel = GrpcTransportChannel.newBuilder().setManagedChannel(managedChannel).build();
			builder.setChannelProvider(FixedTransportChannelProvider.create(channel));
		}

		this.subscriber = builder.build();
	}

	/**
	 * Set the custom hostname/port combination of PubSub.
	 * The ONLY reason to use this is during tests with the emulator provided by Google.
	 *
	 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
	 * @return The current instance
	 */
	public SubscriberWrapper withHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
		return this;
	}

	void startBlocking() {
		ApiService apiService = subscriber.startAsync();
		apiService.awaitRunning();

		if (apiService.state() != ApiService.State.RUNNING) {
			throw new IllegalStateException("Could not start PubSubSubscriber, ApiService.State: " + apiService.state());
		}
		apiService.awaitTerminated();
	}

	void stop() {
		subscriber.stopAsync().awaitTerminated();
		if (channel != null) {
			try {
				channel.close();
				managedChannel.shutdownNow();
			} catch (Exception e) {
				// Ignore
			}
		}
	}

	Subscriber getSubscriber() {
		return subscriber;
	}
}
